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
		fmt.Println("Calling stop because test timed out")
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
		eventPublished: func(event common.MapStr, beat *AmqpBeat) {
			received = true
			assert.Equal(t, expected, event["payload"])
			fmt.Println("Exiting test")
			wg.Done()
		},
	}

	go rb.Run(b)
	pub.send(test)

	wg.Wait()
	rb.Stop()
	assert.True(t, received, "Did not receive message")
}

func TestCanReceiveOnMultipleQueues(t *testing.T) {

	expected := "This is a test"
	test := fmt.Sprintf("{\"payload\": \"%s\"}", expected)

	rb, b := helpBuildBeat("./test_data/multi.yml")
	input := rb.RbConfig.AmqpInput

	var ch *amqp.Channel
	pubs := make([]*Publisher, len(*input.Channels))
	for i, cfg := range *input.Channels {
		pub := newPublisher(*input.ServerURI, &cfg, ch)
		if pub.conn != nil {
			ch = pub.ch
			defer pub.close()
		}
		pubs[i] = pub
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	msgPerQueue := 1000
	received := make(chan string)
	totalMsgs := msgPerQueue * len(pubs)

	go func() {
		counts := make(map[string]int)
		i := 0
		for c := range received {
			i++
			counts[c]++
			if i == totalMsgs {
				break
			}
		}

		for i := 0; i < len(pubs); i++ {
			assert.Equal(t, msgPerQueue, counts[pubs[i].typeTag], "Did not receive all messages for publisher")
		}
		fmt.Println("Received all expected messages")

		rb.Stop()
		wg.Done()
	}()

	b.Events = &MockClient{beat: rb,
		eventPublished: func(event common.MapStr, beat *AmqpBeat) {
			tstr := event["type"].(string)
			assert.Equal(t, expected, event["payload"])
			received <- tstr
		},
	}

	go rb.Run(b)

	for i := 0; i < totalMsgs; i++ {
		pubs[i%len(pubs)].send(test)
	}

	wg.Wait()

}

func helpBuildBeat(cfgFile string) (*AmqpBeat, *beat.Beat) {
	rb := new(AmqpBeat)
	b := beat.NewBeat("", "", rb)
	b.Events = &MockClient{beat: rb,
		eventPublished: func(event common.MapStr, beat *AmqpBeat) {
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
