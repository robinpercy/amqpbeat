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

	var wg sync.WaitGroup
	events := make(chan common.MapStr)

	wg.Add(len(*rb.RbConfig.AmqpInput.Channels))

	for _, c := range *rb.RbConfig.AmqpInput.Channels {
		cfg := c
		go rb.consumeIntoStream(events, ch, &cfg, &wg)
	}

	go publishStream(events, b.Events)
	wg.Wait()

	return nil
}

func (rb *RabbitBeat) consumeIntoStream(stream chan<- common.MapStr, ch *amqp.Channel, c *ChannelConfig, wg *sync.WaitGroup) {
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
			// process delivery
			m := common.MapStr{}
			err = json.Unmarshal(d.Body, &m)
			if err != nil {
				logp.Err("Error unmarshalling: %s", err)
			}
			m["@timestamp"] = time.Now()
			m["type"] = *c.TypeTag
			stream <- m
			d.Ack(false)
		case <-rb.stop:
			return
		}
	}
}

func publishStream(stream chan common.MapStr, client publisher.Client) {
	for e := range stream {
		client.PublishEvent(e)
	}
}

func (rb *RabbitBeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (rb *RabbitBeat) Stop() {
	if rb.stop != nil {
		close(rb.stop)
	}
}
