package beat

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/cfgfile"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/logp"
	"github.com/elastic/libbeat/publisher"
	"github.com/streadway/amqp"
)

type AmqpBeat struct {
	RbConfig  Settings
	journaler *Journaler
	stop      chan interface{}
}

func (rb *AmqpBeat) Config(b *beat.Beat) error {
	return rb.ConfigWithFile(b, "")
}

func (rb *AmqpBeat) ConfigWithFile(b *beat.Beat, filePath string) error {
	err := cfgfile.Read(&rb.RbConfig, filePath)
	if err != nil {
		logp.Err("Error reading configuration file:'%s' %v", filePath, err)
		return err
	}

	rb.RbConfig.CheckRequired()
	rb.RbConfig.SetDefaults()
	return nil
}

func (rb *AmqpBeat) Setup(b *beat.Beat) error {
	rb.stop = make(chan interface{})

	var err error
	rb.journaler, err = NewJournaler(rb.RbConfig.AmqpInput.Journal)

	if err != nil {
		return err
	}

	return nil
}

func (rb *AmqpBeat) Run(b *beat.Beat) error {
	logp.Info("Running...")
	serverURI := rb.RbConfig.AmqpInput.ServerURI

	conn, err := amqp.Dial(*serverURI)
	if err != nil {
		logp.Err("Failed to connect to RabbitMQ at '%s': %v", *serverURI, err)
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logp.Err("Failed to open RabbitMQ channel: %v", err)
		return err
	}
	defer ch.Close()

	rb.runPipeline(b, ch)

	return nil
}

// The go routines setup a pipeline that collects the messages received
// on each queue into a single channel.
//
// Each consumer takes from it's amqp channel and writes it to events and also also selects on the rb.stop channel.
// When rb.stop is closed, each consumer's goroutine will exit and decrement wg's count. The aggregated events
// are passed to the journaler, which will buffer and persist messages before emitting them via it's Out channel.
//
// Journaled messages are passed (in batches) to the publishStream method, which emits them via the libbeats client
//
// To stop the entire pipeline, close the rb.stop channel.
func (rb *AmqpBeat) runPipeline(b *beat.Beat, ch *amqp.Channel) {

	var wg sync.WaitGroup
	wg.Add(len(*rb.RbConfig.AmqpInput.Channels))

	events := make(chan *TaggedDelivery)
	for _, c := range *rb.RbConfig.AmqpInput.Channels {
		cfg := c
		go rb.consumeIntoStream(events, ch, &cfg, &wg)
	}
	go rb.journaler.Run((<-chan *TaggedDelivery)(events), rb.stop)
	go publishStream(rb.journaler.Out, b.Events, &wg)

	wg.Wait()
}

func (rb *AmqpBeat) consumeIntoStream(stream chan<- *TaggedDelivery, ch *amqp.Channel, c *ChannelConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	_, err := ch.QueueDeclare(*c.Name, *c.Durable, *c.AutoDelete, false, false, *c.Args)
	if err != nil {
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
			stream <- &TaggedDelivery{delivery: &d, typeTag: c.TypeTag}
		case <-rb.stop:
			logp.Info("Consumer '%s' is stopping...")
			return
		}
	}
}

func (rb *AmqpBeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (rb *AmqpBeat) Stop() {
	if rb.stop != nil {
		logp.Info("Stopping beat")
		close(rb.stop)
	}
}

type TaggedDelivery struct {
	delivery *amqp.Delivery
	typeTag  *string
}

func publishStream(stream <-chan []*TaggedDelivery, client publisher.Client, wg *sync.WaitGroup) {

	for tdList := range stream {
		events := make([]common.MapStr, 0, len(tdList))
		sent := make([]*TaggedDelivery, 0, len(tdList))

		for _, td := range tdList {
			// process delivery
			m := common.MapStr{}
			err := json.Unmarshal(td.delivery.Body, &m)
			if err != nil {
				logp.Err("Error unmarshalling: %s", err)
				continue
			} else {
				sent = append(sent, td)
			}
			m["@timestamp"] = common.Time(time.Now())
			m["type"] = *td.typeTag
			logp.Debug("", "Publishing event: %v", m)
			events = append(events, m)
		}

		logp.Debug("", "Publishing %d events", len(events))
		success := client.PublishEvents(events, publisher.Sync)

		for _, td := range sent {
			if success {
				logp.Debug("", "Acked event")
				td.delivery.Ack(false)
			} else {
				logp.Err("Failed to publish event: %v", td.delivery.Body)
				td.delivery.Nack(false, true)
			}
		}
	}
}

