package utils

import (
	"testing"

	"github.com/dashjay/dbolt/pkg/constants"
)

func TestPageBuffer(t *testing.T) {
	page := GetPage(constants.BtreePageSize)
	PutPage(page)
}
