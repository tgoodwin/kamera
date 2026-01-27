package main

import "sigs.k8s.io/karpenter/pkg/events"

// noopRecorder is used to avoid external event sinks during simulation.
type noopRecorder struct{}

func (noopRecorder) Publish(...events.Event) {}

func newNoopRecorder() events.Recorder { return noopRecorder{} }
