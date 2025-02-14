package tracecheck

import (
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// var outFile = flag.String("outfile", "debug.log", "output file for debug logs")

// func init() {
// 	flag.Parse()
// }

type DebugEmitter struct {
	fileEmitter *event.FileEmitter
	*event.InMemoryEmitter
}

func NewDebugEmitter() *DebugEmitter {
	return &DebugEmitter{
		fileEmitter:     event.NewFileEmitter("debug.log"),
		InMemoryEmitter: event.NewInMemoryEmitter(),
	}
}

func (d *DebugEmitter) LogOperation(e *event.Event) {
	d.fileEmitter.LogOperation(e)
	d.InMemoryEmitter.LogOperation(e)
}

func (d *DebugEmitter) LogObjectVersion(r snapshot.Record) {
	d.fileEmitter.LogObjectVersion(r)
	d.InMemoryEmitter.LogObjectVersion(r)
}
