package beat

import "bytes"
import "fmt"

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
