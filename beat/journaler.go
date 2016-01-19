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
	writer           *os.File
	buffer           *bufio.Writer
}

type emitter struct {
	queuedEvents [][]byte
	output       chan<- []byte
}

func (e *emitter) add(event []byte) {
	e.queuedEvents = append(e.queuedEvents, event)
}

func (e *emitter) sendAll() {
	for _, event := range e.queuedEvents {
		e.output <- event
	}
}

func NewJournal(maxFileSizeBytes int, journalDir string) (*Journaler, error) {
	j := &Journaler{
		journalDir:       journalDir,
		maxFileSizeBytes: maxFileSizeBytes,
		curFileSizeBytes: 0,
		bufferSizeBlocks: 2,
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
	j.buffer.Flush()
	j.writer.Close()
}

// Run ranges over the inbound events maintaining two buffers: one for the
// representations to be written to disk, and the other for the unmodified
// events to be forwarded on to the out channel, once they have been
// written to disk.
//
// Once the I/O buffer is full, it is flushed, which writes it to the
// underlying, synchronous, file handle. Being syncrhonous, we know
// that when the flush returns, our data has been persisted
//
// Note that Run expects to be the only producer to 'out', and will close it
// on error.
//
func (j *Journaler) Run(input <-chan []byte, out chan<- []byte) error {
	emitter := &emitter{
		queuedEvents: make([][]byte, 128, 128),
		output:       out,
	}

	for d := range input {
		if j.curFileSizeBytes > j.maxFileSizeBytes {
			j.Close()
			j.openNewFile()
		}

		// TODO: compress the data going to disk

		// We don't have enough room in the buffer, so flush, sync and send
		if len(d) > j.buffer.Available() {
			var flushErr error
			for flushErr == nil || flushErr == io.ErrShortWrite {
				logp.Warn(flushErr.Error())
				flushErr = j.buffer.Flush()
			}

			if flushErr != nil {
				close(out)
				return flushErr
			}

			logp.Debug("", "Emitting %d queued messages", len(emitter.queuedEvents))
			emitter.sendAll()
		}

		// Now that we've made room (if necessary), add the next event
		emitter.add(d)
		j.buffer.Write(d)
	}

	j.Close()
	emitter.sendAll()

	return nil
}

func (j *Journaler) genFileName() string {
	fname := fmt.Sprintf("amqpbeat.%s.journal.log", string(time.Now().Unix()))
	return filepath.Join(j.journalDir, fname)
}
