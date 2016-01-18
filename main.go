package main

import (
	"os"

	libbeat "github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/logp"
	"github.com/robinpercy/amqpbeat/beat"
)

var name = "amqpbeat"
var version = "1.0.0-alpha"

func main() {
	ab := &beat.AmqpBeat{}

	b := libbeat.NewBeat(name, version, ab)

	b.CommandLineSetup()

	b.LoadConfig()

	err := ab.Config(b)

	if err != nil {
		logp.Critical("Cannot start due to configuration error: %v", err)
		os.Exit(1)
	}

	b.Run()

}
