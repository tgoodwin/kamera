package tracecheck

import (
	"flag"
	"fmt"
	"os"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"go.uber.org/zap/zapcore"
)

var (
	emitStats   = flag.Bool("emit-stats", false, "record and emit reconcile performance stats")
	searchDepth = flag.Int("depth", 10, "exploration search depth")
	timeout     = flag.Duration("timeout", 0, "abort exploration after this duration (0 disables)")
	logLevel    = flag.String("log-level", "info", "logging level (debug, info, warn, error)")
	// TODO add these in once we refactor how the inspector is launched
	// interactiveFlag = flag.Bool("interactive", true, "launch interactive trace inspector")
	// dumpPathFlag    = flag.String("dump-output", "", "optional path to write exploration results (converged + aborted) to disk")
)

func init() {
	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}

	logf.SetLogger(ctrlzap.New(
		ctrlzap.UseDevMode(level <= zapcore.DebugLevel),
		ctrlzap.Level(level),
	))

	SetLogger(logf.Log.WithName("tracecheck"))
}

func parseLogLevel(level string) (zapcore.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "dpanic":
		return zapcore.DPanicLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("supported values: debug, info, warn, error")
	}
}
