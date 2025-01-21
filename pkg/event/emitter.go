package event

import (
	"encoding/json"

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
