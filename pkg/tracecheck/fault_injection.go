package tracecheck

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tgoodwin/kamera/pkg/event"
)

// ErrFaultInjectionCrash is returned by RecordEffect when the write count
// exceeds the crash threshold. Controllers propagate this error, aborting
// the reconcile before any subsequent in-memory side effects.
var ErrFaultInjectionCrash = fmt.Errorf("fault injection: simulated controller crash")

type crashInjector struct {
	mu              sync.Mutex
	writeCount      int
	crashThreshold  int
	crashed         bool
}

func newCrashInjector(threshold int) *crashInjector {
	return &crashInjector{crashThreshold: threshold}
}

// CheckWrite is called for each write effect. Returns an error when the
// crash threshold is reached, simulating an API connection failure.
// The Nth write IS recorded; the (N+1)th write triggers the crash.
func (ci *crashInjector) CheckWrite(opType event.OperationType) error {
	if opType == event.GET || opType == event.LIST {
		return nil // observations don't count
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.writeCount++
	if ci.writeCount > ci.crashThreshold {
		ci.crashed = true
		return ErrFaultInjectionCrash
	}
	return nil
}

func (ci *crashInjector) DidCrash() bool {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.crashed
}

// context key for crash injector
type crashInjectorKey struct{}

func withCrashInjector(ctx context.Context, ci *crashInjector) context.Context {
	return context.WithValue(ctx, crashInjectorKey{}, ci)
}

func getCrashInjector(ctx context.Context) *crashInjector {
	ci, _ := ctx.Value(crashInjectorKey{}).(*crashInjector)
	return ci
}

// isFaultInjectionCrash checks if an error is the sentinel crash error.
func isFaultInjectionCrash(err error) bool {
	if err == nil {
		return false
	}
	// Check the full error chain since the error may be wrapped.
	for e := err; e != nil; e = errors.Unwrap(e) {
		if e == ErrFaultInjectionCrash {
			return true
		}
	}
	return false
}
