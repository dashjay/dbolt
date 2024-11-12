package utils

import "fmt"

func GenTestKey(i uint64) []byte {
	return []byte(fmt.Sprintf("key-%010d", i))
}

func GenTestValue(i uint64) []byte {
	return []byte(fmt.Sprintf("value-%010d", i))
}
