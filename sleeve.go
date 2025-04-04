package sleeve

import (
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tracegen"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TrackSnapshots() tracegen.Option {
	return func(o *tracegen.Config) {
		o.LogObjectSnapshots = true
	}
}

func Wrap(wrapped kclient.Client, name string) *tracegen.Client {
	minioEmitter, err := event.DefaultMinioEmitter()
	if err != nil {
		panic(err)
	}
	return tracegen.Wrap(wrapped, name).WithEnvConfig().WithEmitter(minioEmitter)
}
