package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Emitter interface {
	LogOperation(ctx context.Context, e *event.Event)
	LogObjectVersion(ctx context.Context, r snapshot.Record)
	Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string)
}

type LogEmitter struct {
	logger logr.Logger
}

var _ Emitter = (*LogEmitter)(nil)

func NewLogEmitter(logger logr.Logger) *LogEmitter {
	return &LogEmitter{logger: logger}
}

func (l *LogEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	e, _ := event.NewOperation(obj, reconcileID, controllerID, rootID, opType)
	eventJSON, err := json.Marshal(e)
	if err != nil {
		panic("failed to serialize event")
	}
	l.logger.WithValues("LogType", tag.ControllerOperationKey).Info(string(eventJSON))
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

func (n *NoopEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	// No operation
}

func (n *NoopEmitter) LogOperation(ctx context.Context, e *event.Event) {}

func (n *NoopEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {}

type FileEmitter struct {
	filePath string
	file     *os.File
}

var _ Emitter = (*FileEmitter)(nil)

func NewFileEmitter(filePath string) *FileEmitter {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("failed to open file")
	}

	return &FileEmitter{
		filePath: filePath,
		file:     file,
	}
}

func (f *FileEmitter) Close() {
	if err := f.file.Close(); err != nil {
		panic("failed to close file")
	}
}

func (f *FileEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	e, _ := event.NewOperation(obj, reconcileID, controllerID, rootID, opType)
	f.LogOperation(ctx, e)
	r, err := snapshot.AsRecord(obj, reconcileID)
	if err != nil {
		panic("failed to serialize record")
	}
	r.OperationID = e.ID
	r.OperationType = string(opType)
	f.LogObjectVersion(ctx, *r)
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
	if _, err := f.file.WriteString(data + "\n"); err != nil {
		panic("failed to write to file: " + err.Error())
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

func (i *InMemoryEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	e, _ := event.NewOperation(obj, reconcileID, controllerID, rootID, opType)
	i.LogOperation(ctx, e)
	r, _ := snapshot.AsRecord(obj, reconcileID)
	r.OperationID = e.ID
	r.OperationType = string(opType)
	i.LogObjectVersion(ctx, *r)
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

func (d *DebugEmitter) Emit(ctx context.Context, obj client.Object, opType event.OperationType, controllerID, reconcileID, rootID string) {
	e, _ := event.NewOperation(obj, reconcileID, controllerID, rootID, opType)
	d.LogOperation(ctx, e)
	r, err := snapshot.AsRecord(obj, reconcileID)
	if err != nil {
		panic("failed to serialize record")
	}
	r.OperationID = e.ID
	r.OperationType = string(opType)
	d.LogObjectVersion(ctx, *r)
}

func (d *DebugEmitter) LogOperation(ctx context.Context, e *event.Event) {
	d.fileEmitter.LogOperation(ctx, e)
	d.InMemoryEmitter.LogOperation(ctx, e)
}

func (d *DebugEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	d.fileEmitter.LogObjectVersion(ctx, r)
	d.InMemoryEmitter.LogObjectVersion(ctx, r)
}
