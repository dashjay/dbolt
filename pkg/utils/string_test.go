package utils

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeStringAndUnescapeString(t *testing.T) {
	input := bytes.Repeat([]byte(("value1")), 5)
	t.Log("input:", input)
	escaped := EscapeString(input)
	t.Log("escaped: ", escaped)
	unescaped := UnEscapeString(escaped)
	assert.Equal(t, input, unescaped)
	t.Log("unescaped: ", unescaped)
}

func BenchmarkEscapeStringAndUnescapeString(b *testing.B) {
	b.Run("escape-test", func(b *testing.B) {
		for _, size := range []int{10, 50, 100, 500, 1000} {
			input := bytes.Repeat([]byte{0x02, 0x11, 0x02, 0x01}, size)
			//escaped := EscapeString(input)
			b.Run(fmt.Sprintf("escape-%d", size), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = EscapeString(input)
				}
			})
		}
	})

	b.Run("unescape-test", func(b *testing.B) {
		for _, size := range []int{10, 50, 100, 500, 1000} {
			input := bytes.Repeat([]byte{0x02, 0x11, 0x02, 0x01}, size)
			escaped := EscapeString(input)
			b.Run(fmt.Sprintf("unescape-%d", size), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = UnEscapeString(escaped)
				}
			})
		}
	})
}
