//go:generate mockgen -destination=mock_client.go -package=mocks sigs.k8s.io/controller-runtime/pkg/client Client
//go:generate mockgen -destination=mock_emitter.go -package=mocks github.com/tgoodwin/sleeve/pkg/event Emitter
package mocks
