package beat

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/elastic/libbeat/logp"
	"encoding/json"
)

const (
	blockSize = 4096
)

/*
Journaler is responsible for writing data out to the filesystem in a way
that meets the durability requirements of the application.

This generally requires synchronous IO (via the O_SYNC flag) to guarantee that
the kernel buffer is flushed to disk as part of each write.  Sync IO also
means we should be careful to fill entire blocks of data before syncing.
This is done
*/
type Journaler struct {
	journalDir       string
	bufferSizeBlocks int
	maxFileSizeBytes int
	curFileSizeBytes int
	maxDelay         time.Duration
	writer           *os.File
	buffer           *bufio.Writer
	timer            *time.Timer
	emitter          *emitter
	Out              chan []*AmqpEvent
}

type emitter struct {
	queuedEvents []*AmqpEvent
	output       chan<- []*AmqpEvent
}

func (e *emitter) add(event *AmqpEvent) {
	e.queuedEvents = append(e.queuedEvents, event)
}

func (e *emitter) sendAll() {
	if len(e.queuedEvents) == 0 {
		return
	}
	logp.Debug("", "Emitting %d queued messages", len(e.queuedEvents))
	e.output <- e.queuedEvents
	e.queuedEvents = make([]*AmqpEvent, 0, len(e.queuedEvents))
}

func (e *emitter) close() {
	close(e.output)
}

func NewJournaler(cfg *JournalerConfig) (*Journaler, error) {

	out := make(chan []*AmqpEvent)
	emitter := &emitter{
		queuedEvents: make([]*AmqpEvent, 0, 128),
		output:       out,
	}
	maxDelay := time.Duration(1000) * time.Millisecond

	j := &Journaler{
		journalDir:       *cfg.JournalDir,
		maxFileSizeBytes: *cfg.MaxFileSizeBytes,
		curFileSizeBytes: 0,
		bufferSizeBlocks: *cfg.BufferSizeBlocks,
		maxDelay:         time.Duration(*cfg.MaxDelayMs) * time.Millisecond,
		timer:            time.NewTimer(maxDelay),
		emitter:          emitter,
		Out:              out,
	}

	err := j.openNewFile()

	if err != nil {
		return nil, err
	}

	return j, nil
}

func (j *Journaler) openNewFile() error {
	writer, err := os.OpenFile(j.genFileName(),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_SYNC,
		0660)

	j.writer = writer

	if err != nil {
		return err
	}

	j.buffer = bufio.NewWriter(j.writer)

	return nil
}

func (j *Journaler) Close() {
	defer j.emitter.close()
	defer j.emitter.sendAll()
	defer j.writer.Close()
	defer j.buffer.Flush()
}

// Run ranges over input, buffering the journal until the buffer is full,
// or the maxDelay time has exceeded.  Either condition will cause the
// the journal to be flushed to disk and the journaled deliveries to
// be published to the j.Out channel
func (j *Journaler) Run(input <-chan *AmqpEvent, stop chan interface{}) {
	var err error
	defer j.Close()

loop:
	for {
		// For an event, we may or may not want to flush the buffer, depending
		// on whether the buffer is out of space. Whereas on receiving a timer
		// event, we always need to flush the buffer.
		select {
		case d, more := <-input:
			if !more {
				break loop
			}
			err = j.processEvent(d)
		case <-j.timer.C:
			err = j.flush()
		}

		if err != nil {
			panic(err)
		}
	}
}

func (j *Journaler) processEvent(d *AmqpEvent) error {
	if j.curFileSizeBytes > j.maxFileSizeBytes {
		// Rollover journal file
		j.Close()
		j.openNewFile()
	}

	// We don't have enough room in the buffer, so flush, sync and send
	if len(d.body) > j.buffer.Available() {
		err := j.flush()

		if err != nil {
			return err
		}
	}

	bytes, err := json.Marshal(d.body)
	if err != nil {
		return fmt.Errorf("failed to encode to payload: %v: %v", d.body, err)
	}

	// Now that we've made room (if necessary), add the next event
	j.emitter.add(d)
	// TODO: add TagType as well?
	// TODO: compress the data going to disk, will require modifying the
	//       buffer.Available() check above
	j.buffer.Write(bytes)

	return nil
}

func (j *Journaler) flush() error {
	var flushErr error
	for j.buffer.Buffered() > 0 &&
		(flushErr == nil || flushErr == io.ErrShortWrite) {

		if flushErr != nil {
			logp.Warn(flushErr.Error())
		}
		flushErr = j.buffer.Flush()
	}

	if flushErr != nil {
		j.emitter.close()
		return flushErr
	}

	j.emitter.sendAll()
	j.resetTimer()

	return nil
}

func (j *Journaler) resetTimer() {
	j.timer.Reset(j.maxDelay)
}

func (j *Journaler) genFileName() string {
	fname := fmt.Sprintf("amqpbeat.%d.journal.log", time.Now().UnixNano())
	return filepath.Join(j.journalDir, fname)
}
