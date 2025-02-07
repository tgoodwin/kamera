package event

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

type Emitter interface {
	LogOperation(e *Event)
	LogObjectVersion(r snapshot.Record)
}

type LogEmitter struct {
	logger logr.Logger
}

func NewLogEmitter(logger logr.Logger) *LogEmitter {
	return &LogEmitter{logger: logger}
}

func (l *LogEmitter) LogOperation(e *Event) {
	eventJSON, err := json.Marshal(e)
	if err != nil {
		panic("failed to serialize event")
	}
	l.logger.WithValues("LogType", tag.ControllerOperationKey).Info(string(eventJSON))
}

func (l *LogEmitter) LogObjectVersion(r snapshot.Record) {
	recordJSON, err := json.Marshal(r)
	if err != nil {
		panic("failed to serialize record")
	}
	l.logger.WithValues("LogType", tag.ObjectVersionKey).Info(string(recordJSON))
}

type NoopEmitter struct{}

func (n *NoopEmitter) LogOperation(e *Event) {}

func (n *NoopEmitter) LogObjectVersion(r snapshot.Record) {}

type FileEmitter struct {
	filePath string
}

func NewFileEmitter(filePath string) *FileEmitter {
	// Clear the file if it already exists
	file, err := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("failed to clear file")
	}
	file.Close()

	return &FileEmitter{filePath: filePath}
}

func (f *FileEmitter) LogOperation(e *Event) {
	eventJSON, err := json.Marshal(e)
	if err != nil {
		panic("failed to serialize event")
	}
	f.appendToFile(string(eventJSON))
}

func (f *FileEmitter) LogObjectVersion(r snapshot.Record) {
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
	blobsByRecordID map[string][]string
}

func NewInMemoryEmitter() *InMemoryEmitter {
	return &InMemoryEmitter{
		blobsByRecordID: make(map[string][]string),
	}
}

func (i *InMemoryEmitter) LogOperation(e *Event) {
	eventJSON, err := json.Marshal(e)
	if err != nil {
		panic("failed to serialize event")
	}
	if _, ok := i.blobsByRecordID[e.ReconcileID]; !ok {
		i.blobsByRecordID[e.ReconcileID] = make([]string, 0)
	}
	i.blobsByRecordID[e.ReconcileID] = append(i.blobsByRecordID[e.ReconcileID], string(eventJSON))
}

func (i *InMemoryEmitter) LogObjectVersion(r snapshot.Record) {
	recordJSON, err := json.Marshal(r)
	if err != nil {
		panic("failed to serialize record")
	}
	if _, ok := i.blobsByRecordID[r.ReconcileID]; !ok {
		i.blobsByRecordID[r.ReconcileID] = make([]string, 0)
	}
	i.blobsByRecordID[r.ReconcileID] = append(i.blobsByRecordID[r.ReconcileID], string(recordJSON))
}

func (i *InMemoryEmitter) Dump(frameID string) []string {
	logs, ok := i.blobsByRecordID[frameID]
	if !ok {
		fmt.Println("Error: frameID not found")
		panic("frameID not found")
	}
	return logs
}
