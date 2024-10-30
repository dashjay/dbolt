package utils

import "fmt"

func GenTestKey(i uint16) []byte {
	return []byte(fmt.Sprintf("key-%08d", i))
}

func GenTestValue(i uint16) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}
