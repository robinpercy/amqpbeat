package main

import (
	"os"

	libbeat "github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/logp"
	"github.com/robinpercy/rabbitbeat/beat"
)

var name = "amqpbeat"
var version = "1.0.0-alpha"

func main() {
	rb := &beat.RabbitBeat{}

	b := libbeat.NewBeat(name, version, rb)

	b.CommandLineSetup()

	b.LoadConfig()

	err := rb.Config(b)

	if err != nil {
		logp.Critical("Cannot start due to configuration error: %v", err)
		os.Exit(1)
	}

	b.Run()

}
