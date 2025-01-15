package replay

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

func TestFilterSleeveLines(t *testing.T) {
	tests := []struct {
		name     string
		lines    []string
		expected []string
	}{
		{
			name: "filter sleeve lines",
			lines: []string{
				"some random log",
				"hiii" + tag.LoggerName + " This is a sleeve log",
				tag.LoggerName + " Another sleeve log",
				"another random log",
			},
			expected: []string{
				"This is a sleeve log",
				"Another sleeve log",
			},
		},
		{
			name: "no sleeve lines",
			lines: []string{
				"some random log",
				"another random log",
			},
			expected: []string{},
		},
		{
			name:     "empty lines",
			lines:    []string{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterSleeveLines(tt.lines)
			assert.Equal(t, tt.expected, result)
		})
	}
}
