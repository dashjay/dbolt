package utils

import (
	"sync"

	"github.com/dashjay/dbolt/pkg/constants"
)

var _pageBuffer = &sync.Pool{
	New: func() interface{} {
		return make([]byte, constants.BTREE_PAGE_SIZE)
	},
}

func GetPage() []byte {
	return _pageBuffer.Get().([]byte)
}

func PutPage(page []byte) {
	memsetRepeat(page, 0)
	_pageBuffer.Put(page)
}

func memsetRepeat(a []byte, v byte) {
	if len(a) == 0 {
		return
	}
	a[0] = v
	for bp := 1; bp < len(a); bp *= 2 {
		copy(a[bp:], a[:bp])
	}
}
