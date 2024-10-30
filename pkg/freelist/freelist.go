package freelist

import (
	"encoding/binary"

	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
)

// LNode node format:
// | next | pointers | unused |
// |  8B  |   n*8B   |   ...  |
type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (constants.BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// getters & setters
func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node[0:8])
}
func (node LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(node[0:8], next)
}
func (node LNode) getPtr(idx int) uint64 {
	offset := FREE_LIST_HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[offset:])

}
func (node LNode) setPtr(idx int, ptr uint64) {
	utils.Assert(idx < FREE_LIST_CAP, "setNode ptr overflow free list cap")
	offset := FREE_LIST_HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[offset:], ptr)

}

type List struct {
	// callbacks for managing on-disk pages
	getNode func(uint64) []byte // read a page
	newNode func([]byte) uint64 // append a new page
	setNode func(uint64) []byte // update existing page
	// persisted data in the meta page
	headPage uint64 // pointer to the list head node
	headSeq  uint64 // monotonic sequence number to index into the list head
	tailPage uint64
	tailSeq  uint64
	// in-memory states
	maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
}

func NewFreeList(getNodeCB func(uint64) []byte, newNodeCB func([]byte) uint64, setNodeCB func(uint64) []byte) List {
	return List{
		getNode:  getNodeCB,
		newNode:  newNodeCB,
		setNode:  setNodeCB,
		headPage: 0,
		headSeq:  0,
		tailPage: 0,
		tailSeq:  0,
		maxSeq:   0,
	}
}

// DebugFreelistDump returns the content and the list nodes
func DebugFreelistDump(free *List) (items []uint64, nodes []uint64) {
	ptr := free.headPage
	nodes = append(nodes, ptr)
	seq := free.headSeq
	for seq != free.tailSeq {
		utils.Assert(ptr != 0, "head page is not zero")
		node := LNode(free.getNode(ptr))
		items = append(items, node.getPtr(seq2idx(seq)))
		seq++
		if seq2idx(seq) == 0 {
			ptr = node.getNext()
			nodes = append(nodes, ptr)
		}
	}
	return
}

func (fl *List) SetMeta(headPage, headSeq, tailPage, tailSeq uint64) {
	fl.headPage = headPage
	fl.headSeq = headSeq
	fl.tailPage = tailPage
	fl.tailSeq = tailSeq
}

func (fl *List) GetMeta() (headPage, headSeq, tailPage, tailSeq uint64) {
	return fl.headPage, fl.headSeq, fl.tailPage, fl.tailSeq
}

// seq2idx every node's first 8 bytes is header, seq can be translated to the idx of free node in this page
func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

func (fl *List) assertCheckFreelist() {
	utils.Assertf(fl.headPage != 0 && fl.tailPage != 0, "headPage(%d) or tailPage(%d) should not be zero", fl.headPage, fl.tailPage)
	utils.Assertf(fl.headSeq != fl.tailSeq || fl.headPage == fl.tailPage, "headSeq(%d) should not equal to tailSeq(%d) expect they are in the same page", fl.headSeq, fl.tailSeq)
}

// get 1 item from the list head. return 0 on failure.
func (fl *List) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 { // the empty head node is recycled
		fl.PushTail(head)
	}
	return ptr
}

// remove 1 item from the head node, and remove the head node if empty.
func flPop(fl *List) (ptr uint64, head uint64) {
	fl.assertCheckFreelist()
	if fl.headSeq == fl.maxSeq {
		return 0, 0 // cannot advance
	}
	node := LNode(fl.getNode(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++
	// move to the next one if the head node is empty
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		utils.Assert(fl.headPage != 0, "invalid head page")
	}
	return
}

// add 1 item to the tail
func (fl *List) PushTail(ptr uint64) {
	fl.assertCheckFreelist()
	// add it to the tail node
	LNode(fl.setNode(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	// add a new tail node if it's full (the list is never empty)
	if seq2idx(fl.tailSeq) == 0 {
		// try to reuse from the list head
		next, head := flPop(fl) // may remove the head node
		if next == 0 {
			// or allocate a new node by appending
			next = fl.newNode(utils.GetPage())
		}
		// link to the new tail node
		LNode(fl.setNode(fl.tailPage)).setNext(next)
		fl.tailPage = next
		// also add the head node if it's removed
		if head != 0 {
			LNode(fl.setNode(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

// make the newly added items available for consumption
func (fl *List) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}
