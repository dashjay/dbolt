package utils

import (
	"bytes"
)

// EscapeString
// Strings are encoded as nul terminated strings,
// escape the nul byte so that strings contain no nul byte.
// replace all byte 0x00 -> 0x01 0x01
// replace all byte 0x01 -> 0x01 0x02
func EscapeString(in []byte) []byte {
	siz := EscapeStringSize(in)
	out := make([]byte, siz)
	pos := 0
	for _, char := range in {
		if char <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = char + 1
			pos += 2
		} else {
			out[pos] = char
			pos += 1
		}
	}
	out[siz-1] = 0
	return out
}

func EscapeStringSize(in []byte) int {
	zeros := bytes.Count(in, []byte{0x00})
	ones := bytes.Count(in, []byte{0x01})
	if zeros+ones == 0 {
		return len(in) + 1
	}
	return len(in) + zeros + ones + 1
}

// UnEscapeString
// 0x00 <- 0x01 0x01
// 0x01 <- 0x01 0x02
func UnEscapeString(in []byte) []byte {
	out := make([]byte, len(in))
	cur := 0
	for ptr := 0; ptr < len(in)-1; {
		if in[ptr] == 0x01 {
			if in[ptr+1] == 0x01 {
				out[cur] = 0x00
			} else if in[ptr+1] == 0x02 {
				out[cur] = 0x01
			} else {
				panic("invalid escaped string")
			}
			ptr += 2
		} else {
			out[cur] = in[ptr]
			ptr++
		}
		cur++
	}
	return out[:cur]
}
