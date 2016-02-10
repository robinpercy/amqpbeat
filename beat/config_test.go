package beat

import (
	"fmt"
	"strings"
	"testing"

	"github.com/elastic/beats/vendor/github.com/stretchr/testify/assert"
	"github.com/elastic/libbeat/cfgfile"
)

func TestMissingConfigFailsGracefully(t *testing.T) {
	var settings Settings
	settings.CheckRequired()
	settings.SetDefaults()
}

func TestJournalCfgIsOptional(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "./testfiles/minimal.yml")
	err := settings.SetDefaults()
	assert.Nil(t, err, fmt.Sprintf("Unexected error %v", err))
	jcfg := settings.AmqpInput.Journal
	assert.NotNil(t, jcfg, "JournalConfig be populated with defaults")
	assert.NotNil(t, jcfg.BufferSizeBlocks)
	assert.NotNil(t, jcfg.JournalDir)
	assert.NotNil(t, jcfg.MaxDelayMs)
	assert.NotNil(t, jcfg.MaxFileSizeBytes)
}

func TestMissingInputSectionError(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "")
	err := settings.CheckRequired()
	assertErrorFor(t, err.(*ConfigError), "amqpinput")
}

func TestComplainsWhenChannelsMissing(t *testing.T) {
	err := checkFile("./testfiles/missing_channels.yml")
	e := err.(*ConfigError)
	if len(e.ErrorMap) != 1 {
		t.Errorf("Expected exactly one error, got %d", len(e.ErrorMap))
	}
}

func TestComplainsWhenChannelNameMissing(t *testing.T) {
	err := checkFile("./testfiles/missing_channel_name.yml")
	e := err.(*ConfigError)
	assertErrorFor(t, e, "channel.name")
}

func TestDefaults(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "./testfiles/minimal.yml")
}

func TestNilChannelValidationShortCircuits(t *testing.T) {
	var c *ChannelConfig
	errors := make(errorMap)
	c.CheckRequired(errors)
	if len(errors) > 0 {
		t.Error("No errors expected")
	}
}

func TestConfigErrorMapConcatsMessages(t *testing.T) {
	errMap := make(errorMap)
	errMap["foo1"] = "bar1"
	errMap["foo2"] = "bar2"
	errStr := errorFor(errMap).Error()
	for k, v := range errMap {
		expected := fmt.Sprintf("%s: %s\n", k, v)
		if !strings.Contains(errStr, expected) {
			t.Errorf("%q does not contain: %q", errStr, expected)
		}
	}
}

func TestEmptyConfigErrorMapGivesEmptyString(t *testing.T) {
	tests := []ConfigError{ConfigError{ErrorMap: make(errorMap)}, ConfigError{ErrorMap: nil}}
	for _, err := range tests {
		if err.Error() != "" {
			t.Errorf("Expected empty string, got '%s'", err.Error())
		}
	}
}

func TestAllChannelValuesSet(t *testing.T) {
	settings := loadFile("./testfiles/full.yml")
	if settings == nil {
		t.Errorf("Settings should not be nil")
	}
	if settings.AmqpInput == nil {
		t.Errorf("AmqpInput should not be nil")
	}
	if settings.AmqpInput.Channels == nil {
		t.Errorf("Channels should not be nil")
	}
	if len(*settings.AmqpInput.Channels) != 1 {
		t.Errorf("Channels should not be empty")
	}

	c0 := (*settings.AmqpInput.Channels)[0]
	if *c0.Name != "test" {
		t.Errorf("Expected %s, got %s", "test", *c0.Name)
	}
	if *c0.Required != true {
		t.Errorf("Expected true, got %d", *c0.Required)
	}
	if *c0.MaxBatchSize != 100 {
		t.Errorf("Expected 100  got %d", *c0.MaxBatchSize)
	}
}

func assertErrorFor(t *testing.T, e *ConfigError, key string) {
	if _, ok := e.ErrorMap[key]; !ok {
		t.Errorf("%s should be reported as missing", key)
	}
}

func loadFile(fileName string) *Settings {
	var settings Settings
	cfgfile.Read(&settings, fileName)
	return &settings
}

func checkFile(fileName string) error {
	settings := loadFile(fileName)
	return settings.CheckRequired()
}
