package sleeve

import (
	"time"

	"github.com/tgoodwin/sleeve/pkg/client"
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
	return client.Wrap(wrapped, name).WithEnvConfig()
}
