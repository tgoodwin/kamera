package logger

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
)

var (
	verbosity int
)

const (
	Info  = 0
	Debug = 1
	Trace = 2
)

// InitFlags registers the logging flags.
// Call this from your init() or early in your application startup

func getConsoleLogger(level int) logr.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

	zerologr.NameFieldName = "logger"
	zerologr.NameSeparator = "/"
	zerologr.SetMaxV(level)

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	// output.FormatMessage = func(i interface{}) string {
	// 	return fmt.Sprintf("***%s****", i)
	// }
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	// output.FormatFieldValue = func(i interface{}) string {
	// 	return strings.ToUpper(fmt.Sprintf("%s", i))
	// }
	zl := zerolog.New(output)
	zl = zl.With().Timestamp().Caller().Logger()
	var log logr.Logger = zerologr.New(&zl)
	return log
}

func GetLogger(level int) logr.Logger {
	return getConsoleLogger(level)
}
