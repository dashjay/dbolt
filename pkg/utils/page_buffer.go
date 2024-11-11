package utils

import (
	"sync"

	"github.com/dashjay/dbolt/pkg/constants"
)

const (
	maxPageBufferSize = 3
)

var _pageBuffers [maxPageBufferSize + 1]sync.Pool

func init() {
	for i := 1; i <= maxPageBufferSize; i++ {
		_pageBuffers[i] = sync.Pool{New: func() interface{} {
			return make([]byte, constants.BTREE_PAGE_SIZE*i)
		}}
	}
}

func GetPage(pageSize int) []byte {
	Assertf(pageSize%constants.BTREE_PAGE_SIZE == 0, "the requested memory size %d is not an integer multiple of page size", pageSize)
	index := pageSize / constants.BTREE_PAGE_SIZE
	Assertf(index <= maxPageBufferSize, "the requested memory is too large: %d", pageSize)
	return _pageBuffers[index].Get().([]byte)
}

func PutPage(page []byte) {
	// no need to memeset the page
	//memsetRepeat(page, 0)
	Assertf(cap(page)%constants.BTREE_PAGE_SIZE == 0, "the requested memory size %d is not an integer multiple of page size", cap(page))
	index := cap(page) / constants.BTREE_PAGE_SIZE
	Assertf(index <= maxPageBufferSize, "the requested memory is too large: %d", cap(page))
	_pageBuffers[index].Put(page[:cap(page)])
}

//func memsetRepeat(a []byte, v byte) {
//	if len(a) == 0 {
//		return
//	}
//	a[0] = v
//	for bp := 1; bp < len(a); bp *= 2 {
//		copy(a[bp:], a[:bp])
//	}
//}
