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
		fmt.Print("DONE RUNNING")
		wg.Done()
	}()

	time.AfterFunc(time.Second, func() {
		rb.Stop()
	})
	fmt.Print("Waiting")
	wg.Wait()
	fmt.Print("Done test")
}

func TestConfigIsLoaded(t *testing.T) {
	rb, _ := helpBuildBeat("./test_data/minimal.yml")
	assert.NotNil(t, rb.Config)
	assert.Equal(t, 1, len(*rb.RbConfig.AmqpInput.Channels))
	assert.Equal(t, "test", *(*rb.RbConfig.AmqpInput.Channels)[0].Name)
}

func TestCanReceiveMessage(t *testing.T) {
	pub := newPublisher("", "test")
	defer pub.close()

	expected := "This is a test"
	test := fmt.Sprintf("{\"payload\": \"%s\"}", expected)
	pub.send(test)

	received := false
	rb, b := helpBuildBeat("./test_data/full.yml")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	exitTest := func() {
		rb.Stop()
		wg.Done()
	}

	fmt.Printf("Events1 = %v\n", b.Events)
	b.Events = &MockClient{beat: rb,
		eventPublished: func(event common.MapStr, beat *RabbitBeat) {
			fmt.Printf("eventPublished: %v\n", event)
			received = true
			assert.Equal(t, expected, event["payload"])
			exitTest()
		},
	}
	fmt.Printf("Events2 = %v\n", b.Events)

	go rb.Run(b)

	time.AfterFunc(3*time.Second, func() {
		exitTest()
	})
	wg.Wait()
	assert.True(t, received, "Did not receive message")
}

func helpBuildBeat(cfgFile string) (*RabbitBeat, *beat.Beat) {
	rb := new(RabbitBeat)
	b := beat.NewBeat("", "", rb)
	b.Events = &MockClient{beat: rb,
		eventPublished: func(event common.MapStr, beat *RabbitBeat) {
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
}

func newPublisher(exch string, routingKey string) *Publisher {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	utils.FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")

	_, err = ch.QueueDeclare(routingKey, false, false, false, false, nil)

	utils.FailOnError(err, fmt.Sprintf("Failed to declare queue %s", routingKey))
	return &Publisher{exch: exch, routingKey: routingKey, conn: conn, ch: ch}
}

func (p *Publisher) close() {
	defer p.conn.Close()
	defer p.ch.Close()
}

func (p *Publisher) send(msg string) {
	err := (*p.ch).Publish("", "test", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg),
	})
	utils.FailOnError(err, "Failed to publish message")
}

type MockClient struct {
	beat            *RabbitBeat
	eventPublished  func(event common.MapStr, beat *RabbitBeat)
	eventsPublished func(event []common.MapStr, beat *RabbitBeat)
	lastReceived    time.Time
}

func (c MockClient) PublishEvent(event common.MapStr, opts ...publisher.ClientOption) bool {
	fmt.Println("PublishEvent")
	c.eventPublished(event, c.beat)
	c.lastReceived = time.Now()
	return true
}

func (c MockClient) PublishEvents(events []common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventsPublished(events, c.beat)
	c.lastReceived = time.Now()
	return true
}
