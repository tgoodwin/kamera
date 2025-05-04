package sleeve

import (
	"os"
	"strconv"

	"github.com/tgoodwin/sleeve/pkg/emitter"
	"github.com/tgoodwin/sleeve/pkg/tracegen"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var bufferSize = 1000

func Wrap(wrapped client.Client, name string) *tracegen.Client {
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

func WrapAsync(wrapped client.Client, name string) *tracegen.Client {
	minioConfig, err := emitter.MinioConfigFromEnv()
	if err != nil {
		panic(err)
	}

	// Set buffer size from environment variable if available
	bufferSizeEnv := os.Getenv("BUFFER_SIZE")
	if bufferSizeEnv != "" {
		if parsedBufferSize, err := strconv.Atoi(bufferSizeEnv); err == nil {
			bufferSize = parsedBufferSize
		}
	}

	minioEmitter, err := emitter.NewMinioEmitter(minioConfig)
	if err != nil {
		panic(err)
	}
	asyncEmitter := emitter.NewAsyncEmitter(minioEmitter, bufferSize)
	return tracegen.Wrap(wrapped, name).WithEnvConfig().WithEmitter(asyncEmitter)
}
