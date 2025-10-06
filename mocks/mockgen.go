//go:generate mockgen -destination=mock_client.go -package=mocks sigs.k8s.io/controller-runtime/pkg/client Client
//go:generate mockgen -destination=mock_emitter.go -package=mocks github.com/tgoodwin/kamera/pkg/event Emitter
//go:generate mockgen -destination=../pkg/tracecheck/mock_trigger.go -package=tracecheck github.com/tgoodwin/kamera/pkg/tracecheck TriggerHandler
package mocks
