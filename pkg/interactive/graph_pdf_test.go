package interactive

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraphPDFKeepsOnlyLatest(t *testing.T) {
	dir := t.TempDir()
	newFile := func(name string) *string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, nil, 0644))
		return &path
	}

	first := newFile("first.pdf")
	second := newFile("second.pdf")

	replaceGraphPDF(first)
	require.FileExists(t, *first)

	replaceGraphPDF(second)
	require.NoFileExists(t, *first, "previous graph PDF must be removed")
	require.FileExists(t, *second)

	cleanupGraphPDF()
	require.NoFileExists(t, *second, "last graph PDF must be removed on exit")
	require.Nil(t, lastGraphPDF.Load())
}
