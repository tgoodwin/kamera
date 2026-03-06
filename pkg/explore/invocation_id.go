package explore

import (
	"os"
	"strings"

	"github.com/tgoodwin/kamera/pkg/util"
)

const (
	invocationIDAttributeKey = "invocation_id"
	invocationIDEnvVar       = "KAMERA_INVOCATION_ID"
)

func resolveInvocationID() string {
	if existing := strings.TrimSpace(os.Getenv(invocationIDEnvVar)); existing != "" {
		return existing
	}
	return util.UUID()
}

