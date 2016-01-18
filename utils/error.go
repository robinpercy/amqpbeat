package utils

import (
	"fmt"
	"log"
	"runtime/debug"
)

// FailOnError panics if error is not nil
func FailOnError(err error, msg string) {
	if err != nil {
		debug.PrintStack()
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
