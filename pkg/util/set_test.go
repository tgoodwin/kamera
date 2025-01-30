package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_AnySet(t *testing.T) {
	compare := func(a, b int) bool {
		return a == b
	}
	set := NewAnySet[int](compare)
	set.Add(1)
	set.Add(2)
	set.Add(2)
	set.Add(2)
	set.Add(3)
	set.Add(3)

	expected := []int{1, 2, 3}
	assert.Equal(t, expected, set.Items())
}
