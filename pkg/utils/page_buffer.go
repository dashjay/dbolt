package utils

import (
	"sync"

	"github.com/dashjay/dbolt/pkg/constants"
)

const (
	maxPageBufferSize = 4
)

//nolint:gochecknoglobals // page buffers
var _pageBuffers [maxPageBufferSize + 1]*sync.Pool

//nolint:gochecknoinits // init page buffers
func init() {
	initPageBuffer()
}

func initPageBuffer() {
	for i := 1; i < maxPageBufferSize; i++ {
		count := i
		_pageBuffers[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, count*constants.BtreePageSize)
			},
		}
	}
}

func GetPage(pageSize int) []byte {
	Assertf(pageSize%constants.BtreePageSize == 0, "the requested memory size %d is not an integer multiple of page size", pageSize)
	index := pageSize / constants.BtreePageSize
	Assertf(index <= maxPageBufferSize, "the requested memory is too large: %d", pageSize)
	return _pageBuffers[index].Get().([]byte)
}

func PutPage(page []byte) {
	Assertf(cap(page)%constants.BtreePageSize == 0, "the requested memory size %d is not an integer multiple of page size", cap(page))
	index := cap(page) / constants.BtreePageSize
	Assertf(index <= maxPageBufferSize, "the requested memory is too large: %d", cap(page))
	//nolint:staticcheck // bytes slice buffer
	_pageBuffers[index].Put(page[:cap(page)])
}

/*func memsetRepeat(a []byte, v byte) {
	if len(a) == 0 {
		return
	}
	a[0] = v
	for bp := 1; bp < len(a); bp *= 2 {
		copy(a[bp:], a[:bp])
	}
}*/
