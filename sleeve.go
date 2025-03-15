package sleeve

import (
	"time"

	"github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/event"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func VisibilityDelay(kind string, duration time.Duration) client.Option {
	return client.VisibilityDelay(kind, duration)
}

func TrackSnapshots() client.Option {
	return func(o *client.Config) {
		o.LogObjectSnapshots = true
	}
}

func Wrap(wrapped kclient.Client, name string) *client.Client {
	minioEmitter, err := event.DefaultMinioEmitter()
	if err != nil {
		panic(err)
	}
	return client.Wrap(wrapped, name).WithEnvConfig().WithEmitter(minioEmitter)
}
