package sleeve

import (
	"github.com/tgoodwin/sleeve/pkg/emitter"
	"github.com/tgoodwin/sleeve/pkg/tracegen"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func Wrap(wrapped kclient.Client, name string) *tracegen.Client {
	minioConfig, err := emitter.MinioConfigFromEnv()
	if err != nil {
		panic(err)
	}

	minioEmitter, err := emitter.NewMinioEmitter(minioConfig)
	if err != nil {
		panic(err)
	}
	return tracegen.Wrap(wrapped, name).WithEnvConfig().WithEmitter(minioEmitter)
}
