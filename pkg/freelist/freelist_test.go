package freelist

import (
	"slices"
	"testing"

	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type L struct {
	free  List
	pages map[uint64][]byte // simulate disk pages
	// references
	added   []uint64
	removed []uint64
}

func newL() *L {
	pages := map[uint64][]byte{}
	pages[1] = utils.GetPage() // initial node
	nextPageID := uint64(1000) // [1000, 10000)

	getNode := func(ptr uint64) []byte {
		utils.Assertf(pages[ptr] != nil, "pages[%d] should not be nil", ptr)
		return pages[ptr]
	}
	newNode := func(node []byte) uint64 {
		utils.Assertf(pages[nextPageID] == nil, "pages[%d] should not be nil", nextPageID)
		current := nextPageID
		nextPageID++
		pages[current] = node
		return current
	}
	setNode := func(ptr uint64) []byte {
		utils.Assertf(pages[ptr] != nil, "pages[%d] should not be nil", ptr)
		return pages[ptr]
	}
	list := NewFreeList(getNode, newNode, setNode)
	list.SetMeta(1, 0, 1, 0)
	return &L{
		free:  list,
		pages: pages,
	}
}

func (l *L) push(ptr uint64) {
	utils.Assertf(l.pages[ptr] == nil, "push on non-empty page %d", ptr)
	l.pages[ptr] = utils.GetPage()
	l.free.PushTail(ptr)
	l.added = append(l.added, ptr)
}

func (l *L) pop() uint64 {
	ptr := l.free.PopHead()
	if ptr != 0 {
		l.removed = append(l.removed, ptr)
	}
	return ptr
}

func (l *L) verify() {
	l.free.assertCheckFreelist()

	// dump all pointers from `l.pages`
	var appended []uint64
	var ptrs []uint64
	for ptr := range l.pages {
		if 1000 <= ptr && ptr < 10000 {
			appended = append(appended, ptr)
		} else if ptr != 1 {
			utils.Assert(slices.Contains(l.added, ptr), "")
		}
		ptrs = append(ptrs, ptr)
	}
	// dump all pointers from the free list
	list, nodes := DebugFreelistDump(&l.free)

	// any pointer is either in the free list, a list node, or removed.
	utils.Assert(len(l.pages) == len(list)+len(nodes)+len(l.removed), "")
	combined := slices.Concat(list, nodes, l.removed)
	slices.Sort(combined)
	slices.Sort(ptrs)
	utils.Assert(slices.Equal(combined, ptrs), "")

	// any pointer is either the initial node, an allocated node, or added
	utils.Assert(len(l.pages) == 1+len(appended)+len(l.added), "")
	combined = slices.Concat([]uint64{1}, appended, l.added)
	slices.Sort(combined)
	utils.Assert(slices.Equal(combined, ptrs), "")
}

func TestGetFromEmptyList(t *testing.T) {
	l := newL()
	l.free.GetMeta()
	id := l.pop()
	assert.Equal(t, id, uint64(0))
	l.verify()
	l.push(2)
	l.verify()
	id = l.pop()
	assert.Equal(t, id, uint64(0))
	l.free.SetMaxSeq()
	id = l.pop()
	assert.Equal(t, id, uint64(2))
}

func TestFreeListEmptyFullEmpty(t *testing.T) {
	for N := 0; N < 2000; N++ {
		l := newL()
		for i := 0; i < N; i++ {
			l.push(10000 + uint64(i))
		}
		l.verify()

		utils.Assert(l.pop() == 0, "")
		l.free.SetMaxSeq()
		ptr := l.pop()
		for ptr != 0 {
			l.free.SetMaxSeq()
			ptr = l.pop()
		}
		l.verify()

		list, nodes := DebugFreelistDump(&l.free)
		utils.Assert(len(list) == 0, "")
		utils.Assert(len(nodes) == 1, "")
	}
}

func TestFreeListEmptyFullEmpty2(t *testing.T) {
	for N := 0; N < 2000; N++ {
		l := newL()
		for i := 0; i < N; i++ {
			l.push(10000 + uint64(i))
			l.free.SetMaxSeq() // allow self-reuse
		}
		l.verify()

		ptr := l.pop()
		for ptr != 0 {
			l.free.SetMaxSeq()
			ptr = l.pop()
		}
		l.verify()

		list, nodes := DebugFreelistDump(&l.free)
		utils.Assert(len(list) == 0, "")
		utils.Assert(len(nodes) == 1, "")
	}
}

func TestFreeListRandom(t *testing.T) {
	for N := 0; N < 1000; N++ {
		l := newL()
		for i := 0; i < 2000; i++ {
			ptr := uint64(10000 + utils.Murmur32(uint32(i)))
			if ptr%2 == 0 {
				l.push(ptr)
				l.free.SetMaxSeq()
			} else {
				x := l.pop()
				if x != 0 {
					assert.Len(t, l.free.getNode(x), constants.BTREE_PAGE_SIZE)
				}
			}
		}
		l.verify()
	}
}
