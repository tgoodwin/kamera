package tag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripLogKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Remove curly braces with ControllerOperationKey",
			input:    `haha ok {"LogType": "sleeve:controller-operation"}`,
			expected: `haha ok `,
		},
		{
			name:     "Remove curly braces with ObjectVersionKey",
			input:    `{"LogType": "sleeve:object-version"}`,
			expected: ``,
		},
		{
			name:     "No curly braces to remove",
			input:    `{"LogType": "other-log-type"}`,
			expected: `{"LogType": "other-log-type"}`,
		},
		{
			name:     "Mixed content",
			input:    `{"something": "else"} {"LogType": "sleeve:object-version", "message": "test"} INFO`,
			expected: `{"something": "else"}  INFO`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StripLogKey(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
