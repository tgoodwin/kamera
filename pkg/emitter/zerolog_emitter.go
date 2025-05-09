package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag" // Assuming tag package exists for label keys
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ZerologEmitter implements the Emitter interface using zerolog for logging.
type ZerologEmitter struct {
	logger zerolog.Logger
	writer diode.Writer // Keep a reference if needed for Close()
}

// Ensure ZerologEmitter implements the Emitter interface.
var _ Emitter = (*ZerologEmitter)(nil)

// NewZerologEmitter creates a new Emitter that logs to stdout using zerolog
// with a non-blocking diode writer.
// bufferSize: The number of log events to buffer. 1000 is a reasonable default.
// pollInterval: The interval at which the diode writer polls for new events. 10ms is a reasonable default.
func NewZerologEmitter(bufferSize int, pollInterval time.Duration) *ZerologEmitter {
	// Create a non-blocking writer using go-diodes
	// Adjust bufferSize and pollInterval based on your needs.
	// Larger buffer = less chance of blocking, more memory usage.
	// Smaller pollInterval = faster flushing, more CPU usage.
	wr := diode.NewWriter(os.Stdout, bufferSize, pollInterval, func(missed int) {
		// This function is called when the diode buffer is full and messages are dropped.
		// You might want to log this occurrence to stderr or another mechanism.
		fmt.Fprintf(os.Stderr, "Logger buffer overflow: %d MESSAGES DROPPED\n", missed)
	})

	// Create a zerolog logger using the diode writer.
	// Use UnixTime field format for better performance.
	logger := zerolog.New(wr).With().Timestamp().Logger()

	return &ZerologEmitter{
		logger: logger,
		writer: wr, // Store the writer if you need to call Close() later
	}
}

// Emit logs the operation and potentially the object snapshot.
// This implementation combines LogOperation and LogObjectVersion logic.
func (z *ZerologEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	// Create the core event
	e, err := event.NewOperation(obj, reconcileID, controllerID, rootID, opType)
	if err != nil {
		// Log error internally, as Emit interface doesn't return error
		z.logger.Error().Err(err).Msg("Failed to create base event for Emit")
		return
	}
	// Log the operation event
	z.LogOperation(ctx, e)
}

// LogOperation logs an operation event using the MarshalZerologObject implementation.
func (z *ZerologEmitter) LogOperation(ctx context.Context, e *event.Event) {
	// Use .Object() which calls the MarshalZerologObject method on the Event struct
	z.logger.Log().
		Str("LogType", tag.ControllerOperationKey).
		Object("event", e). // Pass the event pointer directly
		Msg("Controller Operation")
}

// LogObjectVersion logs an object snapshot record using zerolog.
func (z *ZerologEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record, controllerID string) {
	// Marshal the record to JSON (or log fields directly)
	recordJSON, err := json.Marshal(r)
	if err != nil {
		z.logger.Error().Err(err).Msg("Failed to marshal object version record")
		return
	}

	// Log using zerolog
	z.logger.Log().
		Str("LogType", tag.ObjectVersionKey). // Use your tag constant
		Str("controller_id", controllerID).   // Add controller ID for context if needed
		RawJSON("record", recordJSON).
		Msg("Object Version Snapshot") // Or a more descriptive message
}

// Close waits for the diode writer to flush remaining logs.
// Call this during graceful shutdown of your application.
func (z *ZerologEmitter) Close() error {
	return z.writer.Close()
}
