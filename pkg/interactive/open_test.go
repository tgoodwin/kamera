package interactive

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenArgsUsesDefaultViewer(t *testing.T) {
	switch runtime.GOOS {
	case "darwin":
		require.Equal(t, []string{"open", "/tmp/example.pdf"}, OpenArgs("/tmp/example.pdf"))
	case "linux":
		require.Equal(t, []string{"xdg-open", "/tmp/example.pdf"}, OpenArgs("/tmp/example.pdf"))
	case "windows":
		require.Equal(t, []string{"cmd", "/c", "start", "", "/tmp/example.pdf"}, OpenArgs("/tmp/example.pdf"))
	default:
		t.Skip("unsupported OS")
	}
}
