package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

type Emitter interface {
	LogOperation(ctx context.Context, e *event.Event)
	LogObjectVersion(ctx context.Context, r snapshot.Record)
}

type LogEmitter struct {
	logger logr.Logger
}

var _ Emitter = (*LogEmitter)(nil)

func NewLogEmitter(logger logr.Logger) *LogEmitter {
	return &LogEmitter{logger: logger}
}

func (l *LogEmitter) LogOperation(ctx context.Context, e *event.Event) {
	eventJSON, err := json.Marshal(e)
	if err != nil {
		panic("failed to serialize event")
	}
	l.logger.WithValues("LogType", tag.ControllerOperationKey).Info(string(eventJSON))
}

func (l *LogEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	recordJSON, err := json.Marshal(r)
	if err != nil {
		panic("failed to serialize record")
	}
	l.logger.WithValues("LogType", tag.ObjectVersionKey).Info(string(recordJSON))
}

type NoopEmitter struct{}

var _ Emitter = (*NoopEmitter)(nil)

func (n *NoopEmitter) LogOperation(ctx context.Context, e *event.Event) {}

func (n *NoopEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {}

type FileEmitter struct {
	filePath string
}

var _ Emitter = (*FileEmitter)(nil)

func NewFileEmitter(filePath string) *FileEmitter {
	// Clear the file if it already exists
	file, err := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("failed to clear file")
	}
	file.Close()

	return &FileEmitter{filePath: filePath}
}

func (f *FileEmitter) LogOperation(ctx context.Context, e *event.Event) {
	eventJSON, err := json.Marshal(e)
	if err != nil {
		panic("failed to serialize event")
	}
	f.appendToFile(string(eventJSON))
}

func (f *FileEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	recordJSON, err := json.Marshal(r)
	if err != nil {
		panic("failed to serialize record")
	}
	f.appendToFile(string(recordJSON))
}

func (f *FileEmitter) appendToFile(data string) {
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("failed to open file")
	}
	defer file.Close()

	if _, err := file.WriteString(data + "\n"); err != nil {
		panic("failed to write to file")
	}
}

type InMemoryEmitter struct {
	eventsByReconcileID  map[string][]*event.Event
	recordsByOperationID map[string][]snapshot.Record
}

var _ Emitter = (*InMemoryEmitter)(nil)

func NewInMemoryEmitter() *InMemoryEmitter {
	return &InMemoryEmitter{
		eventsByReconcileID:  make(map[string][]*event.Event),
		recordsByOperationID: make(map[string][]snapshot.Record),
	}
}

func (i *InMemoryEmitter) LogOperation(ctx context.Context, e *event.Event) {
	if _, ok := i.eventsByReconcileID[e.ReconcileID]; !ok {
		i.eventsByReconcileID[e.ReconcileID] = make([]*event.Event, 0)
	}
	i.eventsByReconcileID[e.ReconcileID] = append(i.eventsByReconcileID[e.ReconcileID], e)
}

func (i *InMemoryEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	if _, ok := i.recordsByOperationID[r.OperationID]; !ok {
		i.recordsByOperationID[r.OperationID] = make([]snapshot.Record, 0)
	}
	i.recordsByOperationID[r.OperationID] = append(i.recordsByOperationID[r.OperationID], r)
}

func (i *InMemoryEmitter) Dump(frameID string) []string {
	var logs []string

	if events, ok := i.eventsByReconcileID[frameID]; ok {
		for _, event := range events {
			eventJSON, err := json.Marshal(event)
			if err != nil {
				panic("failed to serialize event")
			}
			logs = append(logs, string(eventJSON))

			if records, ok := i.recordsByOperationID[event.ID]; ok {
				for _, record := range records {
					recordJSON, err := json.Marshal(record)
					if err != nil {
						panic("failed to serialize record")
					}
					logs = append(logs, string(recordJSON))
				}
			}
		}
	}

	if len(logs) == 0 {
		fmt.Println("Error: frameID not found")
		panic("frameID not found")
	}

	return logs
}

type DebugEmitter struct {
	fileEmitter *FileEmitter
	*InMemoryEmitter
}

var _ Emitter = (*DebugEmitter)(nil)

func NewDebugEmitter() *DebugEmitter {
	return &DebugEmitter{
		fileEmitter:     NewFileEmitter("debug.jsonl"),
		InMemoryEmitter: NewInMemoryEmitter(),
	}
}

func (d *DebugEmitter) LogOperation(ctx context.Context, e *event.Event) {
	d.fileEmitter.LogOperation(ctx, e)
	d.InMemoryEmitter.LogOperation(ctx, e)
}

func (d *DebugEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	d.fileEmitter.LogObjectVersion(ctx, r)
	d.InMemoryEmitter.LogObjectVersion(ctx, r)
}

type QueueItem struct {
	ItemType string // "operation" or "record"
	Event    *event.Event
	Record   snapshot.Record
	// We're not passing context through the queue since:
	// 1. Context might be canceled by the time the item is processed
	// 2. The underlying operations are just for logging, not for actual API calls
}

// AsyncEmitter is an emitter that buffers events in a channel to be processed asynchronously
type AsyncEmitter struct {
	underlyingEmitter Emitter
	queue             chan QueueItem
	wg                sync.WaitGroup
	shutdown          chan struct{}
}

// NewAsyncEmitter creates a new AsyncEmitter with the specified buffer size
func NewAsyncEmitter(underlying Emitter, bufferSize int) *AsyncEmitter {
	ae := &AsyncEmitter{
		underlyingEmitter: underlying,
		queue:             make(chan QueueItem, bufferSize),
		shutdown:          make(chan struct{}),
	}

	// Start the consumer goroutine
	ae.wg.Add(1)
	go ae.processQueue()

	return ae
}

// processQueue processes items from the queue until shutdown is signaled
func (ae *AsyncEmitter) processQueue() {
	defer ae.wg.Done()

	// Use background context for processing queued items
	// since the original context might be canceled by the time we process the item
	backgroundCtx := context.Background()

	for {
		select {
		case item := <-ae.queue:
			// Process the item
			if item.ItemType == "operation" {
				ae.underlyingEmitter.LogOperation(backgroundCtx, item.Event)
			} else if item.ItemType == "record" {
				ae.underlyingEmitter.LogObjectVersion(backgroundCtx, item.Record)
			}
		case <-ae.shutdown:
			// Drain the queue before exiting
			ae.drainQueue()
			return
		}
	}
}

// drainQueue processes all remaining items in the queue
func (ae *AsyncEmitter) drainQueue() {
	// Use background context for processing remaining queued items
	backgroundCtx := context.Background()

	// Keep processing until the queue is empty
	for {
		select {
		case item := <-ae.queue:
			if item.ItemType == "operation" {
				ae.underlyingEmitter.LogOperation(backgroundCtx, item.Event)
			} else if item.ItemType == "record" {
				ae.underlyingEmitter.LogObjectVersion(backgroundCtx, item.Record)
			}
		default:
			// Queue is empty
			return
		}
	}
}

// LogOperation queues an operation event
func (ae *AsyncEmitter) LogOperation(ctx context.Context, e *event.Event) {
	select {
	case ae.queue <- QueueItem{
		ItemType: "operation",
		Event:    e,
	}:
		// Item queued successfully
	case <-ctx.Done():
		// Context canceled, drop the event
		fmt.Println("WARNING: Context canceled, dropping operation event")
	default:
		// Queue is full, log a warning and drop the event
		fmt.Println("WARNING: AsyncEmitter queue is full, dropping operation event")
	}
}

// LogObjectVersion queues an object version record
func (ae *AsyncEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	select {
	case ae.queue <- QueueItem{
		ItemType: "record",
		Record:   r,
	}:
		// Item queued successfully
	case <-ctx.Done():
		// Context canceled, drop the record
		fmt.Println("WARNING: Context canceled, dropping object version record")
	default:
		// Queue is full, log a warning and drop the record
		fmt.Println("WARNING: AsyncEmitter queue is full, dropping object version record")
	}
}

// Shutdown gracefully shuts down the AsyncEmitter, ensuring all queued events are processed
func (ae *AsyncEmitter) Shutdown() {
	close(ae.shutdown)
	ae.wg.Wait()
}
