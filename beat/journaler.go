package beat

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"bytes"
	"encoding/json"
	"errors"
	"github.com/elastic/libbeat/logp"
)

const (
	blockSize = 4096
	metricBufByt = "journal.bufferedBytes"
    metricBufAvl = "journal.bufferAvailable"
	metricBufQue = "journal.queued"
	metricBytWrt = "journal.totalBytesWritten"
	metricEvtPrc = "journal.totalEventsProcessed"
)

/*
Journaler is responsible for writing data out to the filesystem in a way
that meets the durability requirements of the application.

This generally requires synchronous IO (via the O_SYNC flag) to guarantee that
the kernel buffer is flushed to disk as part of each write.  Sync IO also
means we should be careful to fill entire blocks of data before syncing.
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
	metrics          chan *metric
	totalBytesWrit   int64
	totalEvtsProcd   int64

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

func NewJournaler(cfg *JournalerConfig, metrics chan *metric) (*Journaler, error) {

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
		metrics:		  metrics,
	}

	err := j.openNewFile()

	if err != nil {
		return nil, err
	}

	return j, nil
}

func (j *Journaler) openNewFile() error {

	err := os.MkdirAll(j.journalDir, 0750)
	if err != nil {
		return fmt.Errorf("Failed to create journal dir: %v", err)
	}

	writer, err := os.OpenFile(j.genFileName(),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_SYNC,
		0660)

	j.writer = writer

	if err != nil {
		return fmt.Errorf("Failed to open file for journaling: %v", err)
	}

	j.buffer = bufio.NewWriterSize(j.writer, j.bufferSizeBlocks*blockSize)

	return nil
}

func (j *Journaler) closeFile() error {
	flushErr := j.buffer.Flush()
	closeErr := j.writer.Close()

	var errBuf bytes.Buffer

	if flushErr != nil {
		errBuf.WriteString(fmt.Sprintf(":failed to flush journal buffer: %v", flushErr))
	}
	if closeErr != nil {
		if errBuf.Len() > 0 {
			errBuf.WriteString(" and ")
		}
		errBuf.WriteString(fmt.Sprintf("failed to close journal writer: %v", closeErr))
	}

	if errBuf.Len() > 0 {
		return errors.New(errBuf.String())
	}

	return nil
}

func (j *Journaler) Close() {
	defer j.emitter.close()
	defer j.emitter.sendAll()
	defer func() {
		err := j.closeFile()
		if err != nil {
			logp.Err(err.Error())
		}
	}()
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
			if d == nil {
				// This seems to coincide with server disconnects, but its not clearly documented
				// in the amqp lib
				logp.Warn("Journaler recieved nil delivery, ignoring")
				continue
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
		j.closeFile()
		err := j.openNewFile()
		if err != nil {
			return fmt.Errorf("Failed to open file for journaling: %v", err)
		}
		j.curFileSizeBytes = 0
	}

	bytes, err := json.Marshal(d.body)
	if err != nil {
		return fmt.Errorf("failed to encode to payload: %v: %v", d.body, err)
	}

	// end each record with a newline to make them easier to parse by humans and computers
	bytes = append(bytes, []byte("\n")...)

	// We don't have enough room in the buffer, so flush the journaler
	if len(bytes) > j.buffer.Available() {
		logp.Debug("journal", "Journal buffer full, triggering flush")
		err := j.flush()

		if err != nil {
			return err
		}
	}

	// Now that we've made room (if necessary), add the next event
	j.emitter.add(d)
	// TODO: compress the data going to disk, will require modifying the
	//       buffer.Available() check above
	j.buffer.Write(bytes)
	j.refreshMetrics()

	return nil
}

func (j *Journaler) refreshMetrics() {
	j.metrics <- &metric{metricBufByt, int64(j.buffer.Buffered())}
	j.metrics <- &metric{metricBufAvl, int64(j.buffer.Available())}
	j.metrics <- &metric{metricBufQue, int64(len(j.emitter.queuedEvents))}
	j.metrics <- &metric{metricBytWrt, int64(j.totalBytesWrit)}
	j.metrics <- &metric{metricEvtPrc, int64(j.totalEvtsProcd)}
}

func (j *Journaler) flush() error {
	// keep track of how many bytes we've flushed to the current file
	// so we know when to rotate it
	j.curFileSizeBytes += j.buffer.Buffered()
	logp.Debug("Journal", "Flushing journal buffer of size %d bytes", j.buffer.Buffered())

	var flushErr error
	j.totalBytesWrit += int64(j.buffer.Buffered())
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

	j.totalEvtsProcd += int64(len(j.emitter.queuedEvents))
	j.emitter.sendAll()
	j.resetTimer()
	j.refreshMetrics()

	return nil
}

func (j *Journaler) resetTimer() {
	j.timer.Reset(j.maxDelay)
}

func (j *Journaler) genFileName() string {
	fname := fmt.Sprintf("amqpbeat.%d.journal.log", time.Now().UnixNano())
	return filepath.Join(j.journalDir, fname)
}
