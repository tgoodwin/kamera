package main

import "k8s.io/apimachinery/pkg/runtime"

// noopEventRecorder satisfies record.EventRecorder without emitting events.
type noopEventRecorder struct{}

func (n *noopEventRecorder) Event(object runtime.Object, eventtype, reason, message string)                  {}
func (n *noopEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {}
func (n *noopEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {}
