package beat

import (
	"fmt"

	"github.com/streadway/amqp"
)
import (
	"bytes"
	"strings"
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
func (s *Settings) CheckRequired() ConfigError {
	errors := make(errorMap)
	if s.AmqpInput == nil {
		errors.missing("amqpinput")
		return ErrorFor(errors)
	}

	inputErrors := s.AmqpInput.CheckRequired()

	for k, v := range inputErrors.ErrorMap {
		errors[k] = v
	}

	return ErrorFor(errors)
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

// AmqpConfig ...
type AmqpConfig struct {
	ServerURI *string
	Channels  *[]ChannelConfig
	Journal   *JournalerConfig
}

func (a *AmqpConfig) CheckRequired() ConfigError {
	errors := make(errorMap)
	if a.Channels == nil || len(*a.Channels) == 0 {
		errors.missing("channels")
	} else {
		for _, c := range *a.Channels {
			c.CheckRequired(errors)
		}
	}
	return ErrorFor(errors)
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

//ConfigError ...
type ConfigError struct {
	ErrorMap map[string]string
}

// ErrorFor ...
func ErrorFor(m map[string]string) ConfigError {
	return ConfigError{ErrorMap: m}
}

func (e ConfigError) Error() string {
	if e.ErrorMap == nil {
		return ""
	}

	var buffer bytes.Buffer
	for key, msg := range e.ErrorMap {
		buffer.WriteString(fmt.Sprintf("%s: %s\n", key, msg))
	}

	return buffer.String()
}
