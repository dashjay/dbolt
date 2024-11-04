package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtils(t *testing.T) {
	GenTestKey(0)
	GenTestValue(0)

	assert.Panics(t, func() {
		Assertf(false, "")
	})

	assert.Panics(t, func() {
		Assert(false, "")
	})

	t.Logf("Murmur32(%d) = %d", 1, Murmur32(1))
	t.Logf("Murmur32(%d) = %d", 2, Murmur32(2))
}
