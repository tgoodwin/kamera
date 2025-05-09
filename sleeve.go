package sleeve

import (
	"os"
	"strconv"
	"time"

	"github.com/tgoodwin/sleeve/pkg/emitter"
	"github.com/tgoodwin/sleeve/pkg/tracegen"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var bufferSize = 1000
var numConsumers = 1

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

func WithMetrics(wrapped client.Client, name string) client.Client {
	withMetrics := tracegen.NewMetricsWrapper(wrapped, name)
	return withMetrics
}

func WrapAsync(wrapped client.Client, name string) client.Client {
	// minioConfig, err := emitter.MinioConfigFromEnv()
	// if err != nil {
	// 	panic(err)
	// }

	// Set buffer size from environment variable if available
	bufferSizeEnv := os.Getenv("BUFFER_SIZE")
	if bufferSizeEnv != "" {
		if parsedBufferSize, err := strconv.Atoi(bufferSizeEnv); err == nil {
			bufferSize = parsedBufferSize
		}
	}

	bufferSize := 10000
	pollInterval := 10 * time.Millisecond

	zerologEmitter := emitter.NewZerologEmitter(bufferSize, pollInterval)

	// minioEmitter, err := emitter.NewMinioEmitter(minioConfig)
	// if err != nil {
	// 	panic(err)
	// }
	numConsumersEnv := os.Getenv("NUM_CONSUMERS")
	if numConsumersEnv != "" {
		if parsedNumConsumers, err := strconv.Atoi(numConsumersEnv); err == nil {
			numConsumers = parsedNumConsumers
		}
	}
	asyncEmitter := emitter.NewAsyncEmitter(zerologEmitter, bufferSize, numConsumers)
	base := tracegen.Wrap(wrapped, name).WithEnvConfig().WithEmitter(asyncEmitter)
	withMetrics := tracegen.NewMetricsWrapper(base, name)
	return withMetrics
}
