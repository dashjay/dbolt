package utils

import (
	"github.com/sirupsen/logrus"
)

func Assert(cond bool, msg string) {
	if cond {
		return
	}
	logrus.Panicf(msg)
}

func Assertf(cond bool, msg string, args ...interface{}) {
	if cond {
		return
	}
	logrus.Panicf(msg, args...)
}

func Murmur32(k uint32) uint32 {
	k *= 0xcc9e2d51
	k = (k << 15) | (k >> 17)
	k *= 0x1b873593
	return k
}
