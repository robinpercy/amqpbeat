package beat

import (
	"fmt"

	"github.com/streadway/amqp"
)
import (
	"bytes"
	"strings"
	"errors"
)

const (
	defaultJournalDir      = "/tmp/"
	defaultJournalBlocks   = 4
	defaultJournalSizeKB   = 20 * 1024
	defaultJournalMaxDelay = 500
)

// Settings ...
type Settings struct {
	AmqpInput *AmqpConfig
}

/*
CheckRequired ...
*/
func (s *Settings) CheckRequired() error {
	errors := make(errorMap)
	if s.AmqpInput == nil {
		errors.missing("amqpinput")
		return errorFor(errors)
	}

	inputErrors := s.AmqpInput.CheckRequired()
	if inputErrors != nil {
		e := inputErrors.(*ConfigError)
		for k, v := range e.ErrorMap {
			errors[k] = v
		}
		return errorFor(errors)
	}
	return nil
}

func (s *Settings) SetDefaults() error {
	if s.AmqpInput == nil {
		return errors.New("amqpinput section is missing from settings file.")
	}

	for i := range *s.AmqpInput.Channels {
		(*s.AmqpInput.Channels)[i].SetDefaults()
	}

	if s.AmqpInput.Journal == nil {
		s.AmqpInput.Journal = new(JournalerConfig)
	}

	s.AmqpInput.Journal.SetDefaults()
	return nil;
}

// AmqpConfig ...
type AmqpConfig struct {
	ServerURI *string
	Channels  *[]ChannelConfig
	Journal   *JournalerConfig
}

func (a *AmqpConfig) CheckRequired() error {
	errors := make(errorMap)
	if a.Channels == nil || len(*a.Channels) == 0 {
		errors.missing("channels")
	} else {
		for _, c := range *a.Channels {
			c.CheckRequired(errors)
		}
	}
	return errorFor(errors)
}

type JournalerConfig struct {
	JournalDir       *string
	BufferSizeBlocks *int
	MaxFileSizeBytes *int
	MaxDelayMs       *int
}

func (j *JournalerConfig) SetDefaults() {

	if j.JournalDir == nil {
		j.JournalDir = new(string)
		*j.JournalDir = defaultJournalDir
	}

	if j.BufferSizeBlocks == nil {
		j.BufferSizeBlocks = new(int)
		*j.BufferSizeBlocks = defaultJournalBlocks
	}

	if j.MaxFileSizeBytes == nil {
		j.MaxFileSizeBytes = new(int)
		*j.MaxFileSizeBytes = defaultJournalSizeKB
	}

	if j.MaxDelayMs == nil {
		j.MaxDelayMs = new(int)
		*j.MaxDelayMs = defaultJournalMaxDelay
	}
}

// ChannelConfig ...
type ChannelConfig struct {
	Name              *string
	Required          *bool
	Durable           *bool
	AutoDelete        *bool
	Exclusive         *bool
	Args              *amqp.Table
	MaxBatchSize      *int
	MaxIntervalMS     *int
	MinIntervalMS     *int
	MaxMessagesPerSec *int
	TypeTag           *string
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

func (c *ChannelConfig) CheckRequired(errors errorMap) error {
	if c == nil {
		return nil
	}

	if c.Name == nil || strings.Trim(*c.Name, " ") == "" {
		errors["channel.name"] = "All channels require a name attribute"
		return errorFor(errors)
	}

	return nil
}

type errorMap map[string]string

func (e errorMap) missing(field string) {
	e[field] = fmt.Sprintf("%s is required in config", field)
}

//ConfigError ...
type ConfigError struct {
	ErrorMap map[string]string
}

// ErrorFor ...
func errorFor(m map[string]string) error {
	if len(m) == 0 {
		return nil
	}
	return &ConfigError{ErrorMap: m}
}

func (e *ConfigError) Error() string {
	if e.ErrorMap == nil {
		return ""
	}

	var buffer bytes.Buffer
	for key, msg := range e.ErrorMap {
		buffer.WriteString(fmt.Sprintf("%s: %s\n", key, msg))
	}

	return buffer.String()
}
