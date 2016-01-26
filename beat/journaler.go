package beat

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/elastic/libbeat/logp"
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
	Out              chan *TaggedDelivery
}

type emitter struct {
	queuedEvents []*TaggedDelivery
	output       chan<- *TaggedDelivery
}

func (e *emitter) add(event *TaggedDelivery) {
	e.queuedEvents = append(e.queuedEvents, event)
}

func (e *emitter) sendAll() {
	fmt.Printf("Emitting events: %d\n", len(e.queuedEvents))
	logp.Debug("", "Emitting %d queued messages", len(e.queuedEvents))
	for _, event := range e.queuedEvents {
		//fmt.Printf("event = %v:%v\n", event, i)
		e.output <- event
	}

	e.queuedEvents = make([]*TaggedDelivery, 0, len(e.queuedEvents))

}

func (e *emitter) close() {
	close(e.output)
}

func NewJournal(maxFileSizeBytes int, journalDir string) (*Journaler, error) {

	out := make(chan *TaggedDelivery)
	emitter := &emitter{
		queuedEvents: make([]*TaggedDelivery, 0, 128),
		output:       out,
	}
	maxDelay := time.Duration(5000) * time.Millisecond

	j := &Journaler{
		journalDir:       journalDir,
		maxFileSizeBytes: maxFileSizeBytes,
		curFileSizeBytes: 0,
		bufferSizeBlocks: 2,
		maxDelay:         maxDelay,
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
func (j *Journaler) Run(input <-chan *TaggedDelivery, stop chan interface{}) {

	var err error

loop:
	for {

		// For an event, we may or may not want to flush the buffer, depending
		// on whether the buffer is out of space. Whereas on receiving a timer
		// event, we always need to flush the buffer.
		select {
		case d := <-input:
			//fmt.Printf("processing event: %v\n", d)
			err = j.processEvent(d)
		case <-j.timer.C:
			err = j.flush()
		case <-stop:
			break loop
		}

		if err != nil {
			panic(err)
		}
	}

	/*
		go func() {
			for {
				select {
				case td, more := <-input:
					if more {
						j.Out <- td
					} else {
						close(j.Out)
						return
					}
				case <-stop:
					fmt.Println("STOPPING JOURNALER")
					close(j.Out)
					return
				}
			}
		}()
	*/
}

func (j *Journaler) processEvent(d *TaggedDelivery) error {
	if j.curFileSizeBytes > j.maxFileSizeBytes {
		// Rollover journal file
		j.Close()
		j.openNewFile()
	}

	// We don't have enough room in the buffer, so flush, sync and send
	if len(d.delivery.Body) > j.buffer.Available() {
		err := j.flush()

		if err != nil {
			return err
		}
	}

	// Now that we've made room (if necessary), add the next event
	j.emitter.add(d)
	// TODO: add TagType as well?
	// TODO: compress the data going to disk, will require modifying the
	//       buffer.Available() check above
	j.buffer.Write(d.delivery.Body)

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
