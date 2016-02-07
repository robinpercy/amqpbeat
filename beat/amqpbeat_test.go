package beat

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
	"github.com/robinpercy/amqpbeat/utils"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestCanStartAndStopBeat(t *testing.T) {
	rb, b := helpBuildBeat("./test_data/minimal.yml")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		rb.Run(b)
		wg.Done()
	}()

	time.AfterFunc(1*time.Second, func() {
		rb.Stop()
	})
	wg.Wait()
}

func TestConfigIsLoaded(t *testing.T) {
	rb, _ := helpBuildBeat("./test_data/minimal.yml")
	assert.NotNil(t, rb.Config)
	assert.Equal(t, 1, len(*rb.RbConfig.AmqpInput.Channels))
	assert.Equal(t, "test", *(*rb.RbConfig.AmqpInput.Channels)[0].Name)
}

func TestCanReceiveMessage(t *testing.T) {

	expected := "This is a test"
	test := fmt.Sprintf("{\"payload\": \"%s\"}", expected)

	received := false
	rb, b := helpBuildBeat("./test_data/full.yml")
	pub := newPublisher(*rb.RbConfig.AmqpInput.ServerURI, &(*rb.RbConfig.AmqpInput.Channels)[0], nil)
	defer pub.close()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	b.Events = &MockClient{beat: rb,
		eventsPublished: func(events[]common.MapStr, beat *AmqpBeat) {
			received = true
			assert.Equal(t, expected, events[0]["payload"])
			wg.Done()
		},
	}

	go rb.Run(b)
	pub.send(test)

	wg.Wait()
	rb.Stop()
	assert.True(t, received, "Did not receive message")
}

type Runner struct {
	t        *testing.T
	ab       *AmqpBeat
	b        *beat.Beat
	received chan common.MapStr
	pubs     []*Publisher
	pubConn  *amqp.Connection
	spec     Spec
}

func newRunner(t *testing.T, config string) *Runner {
	ab, b := helpBuildBeat(config)
	r := &Runner{
		ab:       ab,
		b:        b,
		received: make(chan common.MapStr),
	}
	r.initPublishers()

	b.Events = &MockClient{beat: ab,
		eventsPublished: func(events []common.MapStr, beat *AmqpBeat) {
			for _, event := range events {
				r.received <- event
			}
		},
		eventPublished: func(event common.MapStr, beat *AmqpBeat) {
			r.received <- event
		},
	}

	return r
}

func (r *Runner) initPublishers() {
	var ch *amqp.Channel
	input := r.ab.RbConfig.AmqpInput
	r.pubs = make([]*Publisher, len(*input.Channels))
	for i, cfg := range *input.Channels {
		pub := newPublisher(*input.ServerURI, &cfg, ch)
		if pub.conn != nil {
			r.pubConn = pub.conn
		}
		r.pubs[i] = pub
	}
}

type Spec interface {
	numQueues() int
	handleEvent(e common.MapStr)
	isComplete() bool
	verify(runner *Runner)
}

func (r *Runner) run() {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for c := range r.received {
			r.spec.handleEvent(c)
			if r.spec.isComplete() {
				break
			}
		}

		r.spec.verify(r)
		r.ab.Stop()
		wg.Done()
	}()

	go r.ab.Run(r.b)

	wg.Wait()

}

func (r *Runner) cleanup() error {
	return r.pubConn.Close()
}

type MultiQueueTest struct {
	msgPerQueue   int
	totalExpected int
	counts        map[string]int
	totalReceived int
}

func (test *MultiQueueTest) init() {
	test.msgPerQueue = 1000
	test.totalExpected = test.msgPerQueue * test.numQueues()
	test.counts = make(map[string]int)
	test.totalReceived = 0
}

func (test *MultiQueueTest) numQueues() int {
	return 4
}

func (test *MultiQueueTest) handleEvent(e common.MapStr) {
	typeName := e["type"].(string)
	test.counts[typeName]++
	test.totalReceived++
}

func (test *MultiQueueTest) isComplete() bool {
	return test.totalReceived >= test.totalExpected
}

func (test *MultiQueueTest) verify(r *Runner) {
	for i := 0; i < len(r.pubs); i++ {
		assert.Equal(r.t, test.msgPerQueue, test.counts[r.pubs[i].typeTag], "Did not receive all messages for publisher")
	}
}

func TestCanReceiveOnMultipleQueues(t *testing.T) {
	r := newRunner(t, "./test_data/multi.yml")
	defer r.cleanup()
	test := new(MultiQueueTest)
	test.init()
	r.spec = test
	testMsg := "{\"payload\": \"This is a tetst\"}"
	for i := 0; i < test.totalExpected; i++ {
		r.pubs[i%len(r.pubs)].send(testMsg)
	}
	r.run()
}

func helpBuildBeat(cfgFile string) (*AmqpBeat, *beat.Beat) {
	rb := new(AmqpBeat)
	b := beat.NewBeat("", "", rb)
	b.Events = &MockClient{beat: rb,
		eventsPublished: func(event []common.MapStr, beat *AmqpBeat) {
		},
	}
	rb.ConfigWithFile(b, cfgFile)
	rb.Setup(b)
	return rb, b
}

type Publisher struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	exch       string
	routingKey string
	typeTag    string
}

/*
If ch is nil, a new Connection and Channel will be created, and this publisher
will 'own' the connection. A call to close() will close both channel and connection.

If ch is provided, then this publisher will reuse the channel and
calls to close() will do nothing.
*/
func newPublisher(serverURI string, cfg *ChannelConfig, ch *amqp.Channel) *Publisher {

	var conn *amqp.Connection
	var err error

	if ch == nil {
		conn, err = amqp.Dial(serverURI)
		utils.FailOnError(err, "Failed to connect to RabbitMQ")

		ch, err = conn.Channel()
		utils.FailOnError(err, "Failed to open a channel")
	}

	_, err = ch.QueueDeclare(*cfg.Name, *cfg.Durable, *cfg.AutoDelete, *cfg.Exclusive, false, *cfg.Args)
	ch.QueuePurge(*cfg.Name, true)

	utils.FailOnError(err, fmt.Sprintf("Failed to declare queue %s", cfg.Name))
	return &Publisher{exch: "", routingKey: *cfg.Name, conn: conn, ch: ch, typeTag: *cfg.TypeTag}
}

func (p *Publisher) close() {
	// Only close if this instance was responsible for creating the connection
	if p.conn != nil {
		defer p.conn.Close()
		defer p.ch.Close()
	}
}

func (p *Publisher) send(msg string) {
	err := (*p.ch).Publish("", p.routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg),
	})
	utils.FailOnError(err, "Failed to publish message")
}

type MockClient struct {
	beat            *AmqpBeat
	eventPublished  func(event common.MapStr, beat *AmqpBeat)
	eventsPublished func(event []common.MapStr, beat *AmqpBeat)
	lastReceived    time.Time
	msgsReceived    int
}

func (c MockClient) PublishEvent(event common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventPublished(event, c.beat)
	c.lastReceived = time.Now()
	return true
}

func (c MockClient) PublishEvents(events []common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventsPublished(events, c.beat)
	c.lastReceived = time.Now()
	return true
}
