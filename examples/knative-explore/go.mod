module github.com/tgoodwin/kamera/examples/knative-explore

go 1.24

require (
	github.com/google/go-containerregistry v0.20.3
	github.com/tgoodwin/kamera v0.0.0
	go.uber.org/zap v1.27.0
	k8s.io/api v0.30.1
	k8s.io/apimachinery v0.30.1
	k8s.io/client-go v0.30.1
	sigs.k8s.io/controller-runtime v0.18.4
)

require (
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
)

replace github.com/tgoodwin/kamera => ../..
