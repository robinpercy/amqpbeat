package beat

import (
	"encoding/json"
	"sync"
	"time"

	"fmt"
	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/cfgfile"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/logp"
	"github.com/elastic/libbeat/publisher"
	"github.com/streadway/amqp"
	"strings"
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

	err = rb.RbConfig.CheckRequired()
	if err != nil {
		return err
	}

	err = rb.RbConfig.SetDefaults()
	if err != nil {
		return err
	}

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
// Journaled messages are passed (in batches) to the publishStream method, which emits them via the libbeat client.
//
// To stop the entire pipeline, close the rb.stop channel. This will result in all consumers exiting and incrementing
// the consumer WaitGroup. Once we've finished waiting on that, we close the events channel, which will signal
// the journaler to finish processing events and close its Out channel.
func (rb *AmqpBeat) runPipeline(b *beat.Beat, ch *amqp.Channel) {

	var consumerGroup sync.WaitGroup
	consumerGroup.Add(len(*rb.RbConfig.AmqpInput.Channels))

	events := make(chan *AmqpEvent)
	for _, c := range *rb.RbConfig.AmqpInput.Channels {
		cfg := c
		go rb.consumeIntoStream(events, ch, &cfg, &consumerGroup)
	}
	go rb.journaler.Run((<-chan *AmqpEvent)(events), rb.stop)

	var publisherGroup sync.WaitGroup
	publisherGroup.Add(1)
	go publishStream(rb.journaler.Out, b.Events, &publisherGroup)

	// Wait for all consumers to receive the stop 'signal' (ie close(stop))
	consumerGroup.Wait()

	// Close the events channel, since no more messages will be sent through it.
	// this will cause the journaler to close its Out channel
	close(events)

	// Wait for the publisher to finish processing the journaler.Out channel
	publisherGroup.Wait()

}

func (rb *AmqpBeat) consumeIntoStream(stream chan<- *AmqpEvent, ch *amqp.Channel, c *ChannelConfig, wg *sync.WaitGroup) {
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
			evt, err := newAmqpEvent(&d, c.TypeTag, c.TsField, c.TsFormat)
			if err != nil {
				logp.Warn("failed to build event for delivery, will nack and requeue. delivery: %v \nreason: %v", d, err)
				d.Nack(false, true)
			}
			stream <- evt
		case <-rb.stop:
			logp.Info("Consumer '%s' is stopping...")
			return
		}
	}
}

func newAmqpEvent(delivery *amqp.Delivery, typeTag, tsField, tsFormat *string) (*AmqpEvent, error) {
	m := common.MapStr{}
	err := json.Unmarshal(delivery.Body, &m)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling delivery %v: %v", delivery.Body, err)
	}

	now := time.Now()
	ts := common.Time(now)
	if tsField != nil && tsFormat != nil {
		var err error
		ts, err = extractTS(m, *tsField, *tsFormat)
		if err != nil {
			logp.Warn("Failed to extract @timestamp for event, defaulting to current time ('%s'): %v", now, err)
		}
	}

	m["type"] = *typeTag
	m["@timestamp"] = ts

	ev := &AmqpEvent{
		deliveryTag:  delivery.DeliveryTag,
		acknowledger: delivery.Acknowledger,
		body:         m,
	}

	return ev, nil

}

func extractTS(m common.MapStr, tsField, tsFormat string) (common.Time, error) {
	// NOTE: this only works if keys do not contain periods.
	// TODO: support periods in path components
	dflt := common.Time(time.Now())
	path := strings.Split(tsField, ".")
	submap := m
	var ok bool
	for _, k := range path[:len(path)-1] {
		v, found := submap[k]
		if !found {
			return dflt, fmt.Errorf("did not find component '%s' of path '%s' in %v", k, tsField, m)
		}

		// careful not to shadow submap here (ie don't use ':=' )
		submap, ok = v.(common.MapStr)
		if !ok {
			return dflt, fmt.Errorf("component '%s' of path '%s' is not a submap in %v", k, tsField, m)
		}
	}

	tsValue, found := submap[path[len(path)-1]]
	if !found {
		return dflt, fmt.Errorf("no value found at path '%s' in %v", tsField, m)
	}

	tsStr, ok := tsValue.(string)
	if !ok {
		return dflt, fmt.Errorf("value '%v' at path '%s' is not a string, cannot parse as timestamp", tsValue, tsField)
	}

	ts, err := time.Parse(tsFormat, tsStr)
	if err != nil {
		return dflt, fmt.Errorf("failed to parse timestamp '%s' with layout '%s': %v", tsValue, tsFormat, err)
	}

	return common.Time(ts), nil
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

type AmqpEvent struct {
	deliveryTag  uint64
	acknowledger amqp.Acknowledger
	body         common.MapStr
}

func publishStream(stream <-chan []*AmqpEvent, client publisher.Client, wg *sync.WaitGroup) {

	var failedDTags map[uint64]bool
	for evList := range stream {
		failedDTags = make(map[uint64]bool)

		payloads := make([]common.MapStr, len(evList))
		for i, ev := range evList {
			payloads[i] = ev.body
		}

		logp.Debug("", "Publishing %d events", len(evList))
		success := client.PublishEvents(payloads, publisher.Sync)

		for _, ev := range evList {
			if success && !failedDTags[ev.deliveryTag] {
				logp.Debug("", "Acked event")
				ev.acknowledger.Ack(ev.deliveryTag, false)
			} else {
				logp.Err("Failed to publish event: %v", ev.body)
				ev.acknowledger.Nack(ev.deliveryTag, false, true)
			}
		}
	}

	wg.Done()
}
