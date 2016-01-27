package beat

import (
	"fmt"

	"github.com/streadway/amqp"
)
import "strings"

const DFLT_J_DIR = "/tmp/"
const DFLT_J_BLKS = 4
const DFLT_J_FILE_SZ = 20 * 1024 * 1024
const DFLT_J_DELAY = 500

// ChannelConfig ...
type ChannelConfig struct {
	Name       *string
	Required   *bool
	Durable    *bool
	AutoDelete *bool
	Exclusive  *bool
	// TODO support args
	Args              *amqp.Table
	MaxBatchSize      *int
	MaxIntervalMS     *int
	MinIntervalMS     *int
	MaxMessagesPerSec *int
	TypeTag           *string
}

// AmqpConfig ...
type AmqpConfig struct {
	ServerURI *string
	Channels  *[]ChannelConfig
	Journal   *JournalerConfig
}

type JournalerConfig struct {
	JournalDir       *string
	BufferSizeBlocks *int
	MaxFileSizeBytes *int
	MaxDelayMs       *int
}

// Settings ...
type Settings struct {
	AmqpInput *AmqpConfig
}

/*
CheckRequired ...
*/
func (s *Settings) CheckRequired() ConfigError {
	errors := make(errorMap)
	if s.AmqpInput == nil {
		errors.missing("amqpinput")
		return ErrorFor(errors)
	}

	input := s.AmqpInput
	if input.Channels == nil || len(*input.Channels) == 0 {
		errors.missing("channels")
	} else {
		for _, c := range *input.Channels {
			c.CheckRequired(errors)
		}
	}

	return ErrorFor(errors)
}

func (j *JournalerConfig) SetDefaults() {

	if j.JournalDir == nil {
		j.JournalDir = new(string)
		*j.JournalDir = DFLT_J_DIR
	}

	if j.BufferSizeBlocks == nil {
		j.BufferSizeBlocks = new(int)
		*j.BufferSizeBlocks = DFLT_J_BLKS
	}

	if j.MaxFileSizeBytes == nil {
		j.MaxFileSizeBytes = new(int)
		*j.MaxFileSizeBytes = DFLT_J_FILE_SZ
	}

	if j.MaxDelayMs == nil {
		j.MaxDelayMs = new(int)
		*j.MaxDelayMs = DFLT_J_DELAY
	}
}

func (s *Settings) SetDefaults() {
	for i := range *s.AmqpInput.Channels {
		(*s.AmqpInput.Channels)[i].SetDefaults()
	}

	if s.AmqpInput.Journal == nil {
		s.AmqpInput.Journal = new(JournalerConfig)
	}

	s.AmqpInput.Journal.SetDefaults()
}

func (c *ChannelConfig) SetDefaults() {
	if c.Durable == nil {
		c.Durable = new(bool)
		*c.Durable = false
	}

	if c.AutoDelete == nil {
		c.AutoDelete = new(bool)
		*c.AutoDelete = false
	}

	if c.Exclusive == nil {
		c.Exclusive = new(bool)
		*c.Exclusive = false
	}

	if c.Args == nil {
		c.Args = new(amqp.Table)
		*c.Args = nil
	}

	if c.TypeTag == nil {
		c.TypeTag = new(string)
		*c.TypeTag = "event"
	}
}

func (c *ChannelConfig) CheckRequired(errors errorMap) errorMap {
	if c == nil {
		return errors
	}

	if c.Name == nil || strings.Trim(*c.Name, " ") == "" {
		errors["channel.name"] = "All channels require a name attribute"
	}
	return errors

}

type errorMap map[string]string

func (e errorMap) missing(field string) {
	e[field] = fmt.Sprintf("%s is required in config", field)
}
