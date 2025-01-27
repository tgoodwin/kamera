package event

import (
	"encoding/json"
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
