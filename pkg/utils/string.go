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
	const x00 = 0x00
	const x01 = 0x01
	const x02 = 0x02
	siz := EscapeStringSize(in)
	out := make([]byte, siz)
	pos := 0
	for _, char := range in {
		switch char {
		case x00:
			// 0x00 -> 0x01 0x01
			out[pos+0] = x01
			out[pos+1] = x01
			pos += 2
		case x01:
			// 0x01 -> 0x02 0x02
			out[pos+0] = x01
			out[pos+1] = x02
			pos += 2
		default:
			out[pos] = char
			pos += 1
		}
	}
	out[siz-1] = 0
	return out
}

func EscapeStringSize(in []byte) int {
	const x00 = 0x00
	const x01 = 0x01
	zeros := bytes.Count(in, []byte{x00})
	ones := bytes.Count(in, []byte{x01})
	if zeros+ones == 0 {
		return len(in) + 1
	}
	return len(in) + zeros + ones + 1
}

// UnEscapeString
// 0x00 <- 0x01 0x01
// 0x01 <- 0x01 0x02
func UnEscapeString(in []byte) []byte {
	const x00 = 0x00
	const x01 = 0x01
	const x02 = 0x02
	out := make([]byte, len(in))
	cur := 0
	for ptr := 0; ptr < len(in)-1; {
		if in[ptr] == x01 {
			if in[ptr+1] == x01 { // 0x01 0x01
				out[cur] = x00 // -> 0x00
			} else if in[ptr+1] == x02 { // 0x01 0x02
				out[cur] = x01 // -> 0x01
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
