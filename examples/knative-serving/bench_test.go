package main

import (
	"context"
	"io"
	"testing"

	"github.com/go-logr/zapr"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	klog "k8s.io/klog/v2"
	"knative.dev/pkg/logging"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func BenchmarkExploreKnative(b *testing.B) {
	logf.SetLogger(ctrlzap.New(
		ctrlzap.UseDevMode(false),
		ctrlzap.Level(zap.NewAtomicLevelAt(zapcore.ErrorLevel)),
		ctrlzap.WriteTo(io.Discard),
	))
	nopZap := zap.NewNop()
	zap.ReplaceGlobals(nopZap)
	logf.SetLogger(zapr.NewLogger(nopZap))
	klog.SetLogger(zapr.NewLogger(nopZap))
	tracecheck.SetLogger(logf.Log.WithName("tracecheck"))

	explorer, initialState, err := newKnativeExplorerAndState(1, false)
	if err != nil {
		b.Fatalf("setup explorer: %v", err)
	}

	ctx := logging.WithLogger(context.Background(), nopZap.Sugar())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := initialState.Clone()
		result := explorer.Explore(ctx, state)

		if len(result.ConvergedStates)+len(result.AbortedStates) == 0 {
			b.Fatalf("iteration %d: no states returned from exploration", i)
		}
	}
}
