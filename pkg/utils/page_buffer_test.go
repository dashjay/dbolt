package utils

import "testing"

func TestPageBuffer(t *testing.T) {
	page := GetPage()
	PutPage(page)
}
