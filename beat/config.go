package beat

import (
	"fmt"

	"github.com/streadway/amqp"
)
import (
	"bytes"
	"errors"
	"strings"
	"time"
)

const (
	defaultJournalDir       = "/tmp/"
	defaultJournalBlocks    = 4
	defaultJournalSizeBytes = 20 * 1024 * 1024
	defaultJournalMaxDelay  = 500
	defaultStatsAddr        = ":8111"
	defaultLUnderReplace    = ""
	defaultDotReplace       = "_"
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

	s.AmqpInput.SetDefaults()

	return nil
}

// AmqpConfig ...
type AmqpConfig struct {
	ServerURI     *string
	DryRun        *int
	Channels      *[]ChannelConfig
	Journal       *JournalerConfig
	StatsAddress  *string
	LUnderReplace *string
	DotReplace    *string
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
	if len(errors) > 0 {
		return errorFor(errors)
	}

	return nil
}

func (a *AmqpConfig) SetDefaults() error {

	if a.Journal == nil {
		a.Journal = new(JournalerConfig)
	}

	if a.StatsAddress == nil {
		a.StatsAddress = new(string)
		*a.StatsAddress = defaultStatsAddr
	}

	if a.LUnderReplace == nil {
		a.LUnderReplace = new(string)
		*a.LUnderReplace = defaultLUnderReplace
	}

	if a.DotReplace == nil {
		a.DotReplace = new(string)
		*a.DotReplace = defaultDotReplace
	}

	a.Journal.SetDefaults()

	if a.Channels == nil {
		a.Channels = new([]ChannelConfig)
	}

	for i := range *a.Channels {
		(*a.Channels)[i].SetDefaults()
	}

	return nil
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
		*j.MaxFileSizeBytes = defaultJournalSizeBytes
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
	TsField           *string
	TsFormat          *string
	QosPrefetchCount  *int
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

	if c.QosPrefetchCount == nil {
		c.QosPrefetchCount = new(int)
		*c.QosPrefetchCount = 1024
	}
}

func (c *ChannelConfig) CheckRequired(errors errorMap) error {
	if c == nil {
		return nil
	}

	foundErr := false
	if c.Name == nil || strings.Trim(*c.Name, " ") == "" {
		foundErr = true
		errors["channel.name"] = "All channels require a name attribute"
	}

	tsErr := validateTsConfig(c.TsField, c.TsFormat, errors)

	if foundErr || tsErr != nil {
		return errorFor(errors)
	}

	return nil
}

func validateTsConfig(tsField, tsFormat *string, errors errorMap) error {
	foundErr := false
	if tsField == nil && tsFormat != nil {
		foundErr = true
		errors["channel.tsfield"] = "tsfield must be set if tsformat is set"
	}

	if tsField != nil && tsFormat == nil {
		foundErr = true
		errors["channel.tsformat"] = "tsformat must be set if tsfield is set"
	}

	if tsFormat != nil {
		errKey := "channel.ts.format.test"
		errTpl := "tsformat '%s' is not a valid date format error while testing was: %v"

		str := time.Now().Format(*tsFormat)
		_, err := time.Parse(*tsFormat, str)

		if err != nil {
			foundErr = true
			errors[errKey] = fmt.Sprintf(errTpl, *tsFormat, err)
		}
	}

	if foundErr {
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
