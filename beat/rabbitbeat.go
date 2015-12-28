package beat

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/cfgfile"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/logp"
	"github.com/streadway/amqp"
)

type RabbitBeat struct {
	RbConfig Settings
	stop     chan interface{}
}

func (rb *RabbitBeat) Config(b *beat.Beat) error {
	return rb.ConfigWithFile(b, "")
}

func (rb *RabbitBeat) ConfigWithFile(b *beat.Beat, filePath string) error {
	err := cfgfile.Read(&rb.RbConfig, filePath)
	if err != nil {
		logp.Err("Error reading configuration file:'%s' %v", filePath, err)
		return err
	}
	rb.RbConfig.CheckRequired()
	rb.RbConfig.SetDefaults()
	return nil
}

func (rb *RabbitBeat) Setup(b *beat.Beat) error {
	rb.stop = make(chan interface{})
	return nil
}

func (rb *RabbitBeat) Run(b *beat.Beat) error {

	fmt.Printf("Events3 = %v\n", b.Events)
	serverURI := rb.RbConfig.AmqpInput.ServerURI
	conn, err := amqp.Dial(*serverURI)
	defer conn.Close()
	if err != nil {
		logp.Err("Failed to connect to RabbitMQ at '%s': %v", *serverURI, err)
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		logp.Err("Failed to open RabbitMQ channel: %v", err)
		return err
	}
	defer ch.Close()

	events := make(chan common.MapStr)

	/*
			  The go routines below setup a pipeline that collects the messages received
			  on each queue into a single channel.
			  - Each consumer takes from it's delivery channel and writes it to events
			  - Each consumer also selects on the rb.stop channel. When rb.stop
			    is closed, each consumer goroutine will exit and decrement wg's count.
			  - A separate goroutine is used to wait on wg and clean up the events channels
			    as well as the amqp connection
		    In other words, the entire pipeline can be stopped by closing rb.stop
	*/
	var wg sync.WaitGroup
	wg.Add(len(*rb.RbConfig.AmqpInput.Channels))

	consume := func(c *ChannelConfig) {
		defer wg.Done()
		defer fmt.Errorf("Done consuming")
		fmt.Println("Starting to consume")
		_, err = ch.QueueDeclare(*c.Name, *c.Durable, *c.AutoDelete, false, false, *c.Args)
		if err != nil {
			fmt.Printf("Error: %v", err)
			logp.Err("Failed to declare queue '%s': %v", *c.Name, err)
			return
		}

		q, err := ch.Consume(*c.Name, "", false, false, false, false, *c.Args)
		if err != nil {
			logp.Err("Failed to consume queue %s: %v", *c.Name, err)
			return
		}

		for {
			select {
			case d := <-q:
				fmt.Printf("Got message: %v\n", string(d.Body))
				// process delivery
				m := common.MapStr{}
				err = json.Unmarshal(d.Body, &m)
				if err != nil {
					fmt.Printf("Error unmarshalling: %s", err)
				}
				events <- m
				d.Ack(false)
			case <-rb.stop:
				return
			}
		}
	}

	for _, c := range *rb.RbConfig.AmqpInput.Channels {
		go consume(&c)
	}

	/*
		go func() {
			defer conn.Close()
			defer ch.Close()
			wg.Wait()
			close(events)
		}()
	*/

	go func() {
		for e := range events {
			fmt.Printf("Sending message %v\n", e)
			e["@timestamp"] = time.Now()
			b.Events.PublishEvent(e)
		}
	}()

	wg.Wait()
	fmt.Println("DONE")

	return nil
}

func (rb *RabbitBeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (rb *RabbitBeat) Stop() {
	fmt.Println("Stopping")
	if rb.stop != nil {
		fmt.Println("Closing")
		close(rb.stop)
	}
}
