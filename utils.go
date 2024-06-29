package dbolt

import (
	"github.com/sirupsen/logrus"
)

func init() {
}

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
