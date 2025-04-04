package tracecheck

import (
	"github.com/go-logr/logr"
)

var logger logr.Logger

func SetLogger(l logr.Logger) {
	logger = l
}
