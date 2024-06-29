package dbolt

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestBtree(t *testing.T) {
	const keyCount uint16 = 20
	t.Run("test node merge", func(t *testing.T) {
		leftNode := make(BNode, BTREE_PAGE_SIZE)
		leftNode.setHeader(BNODE_LEAF, keyCount)
		rightNode := make(BNode, BTREE_PAGE_SIZE)
		rightNode.setHeader(BNODE_LEAF, keyCount)

		for i := uint16(0); i < keyCount; i++ {
			nodeAppendKV(leftNode, i, 1, keyOf(i), valueOf(i))
			nodeAppendKV(rightNode, i, 1, keyOf(i+keyCount), valueOf(i+keyCount))
		}
		newNode := make(BNode, BTREE_PAGE_SIZE)
		nodeMerge(newNode, leftNode, rightNode)

		assert.Equal(t, 2*keyCount, newNode.nKeys())

		for i := uint16(0); i < keyCount; i++ {
			assert.Equal(t, keyOf(i), newNode.getKey(i))
			assert.Equal(t, valueOf(i), newNode.getVal(i))
			assert.Equal(t, keyOf(i+keyCount), newNode.getKey(i+keyCount))
			assert.Equal(t, valueOf(i+keyCount), newNode.getVal(i+keyCount))
		}
	})
}

func BenchmarkBtree(b *testing.B) {
	const keyCount uint16 = 70
	b.Run("bench mark node merge", func(b *testing.B) {
		leftNode := make(BNode, BTREE_PAGE_SIZE)
		leftNode.setHeader(BNODE_LEAF, keyCount)
		rightNode := make(BNode, BTREE_PAGE_SIZE)
		rightNode.setHeader(BNODE_LEAF, keyCount)

		for i := uint16(0); i < keyCount; i++ {
			nodeAppendKV(leftNode, i, 1, keyOf(i), valueOf(i))
			nodeAppendKV(rightNode, i, 1, keyOf(i+70), valueOf(i+70))
		}

		b.ResetTimer()
		newNode := make(BNode, BTREE_PAGE_SIZE)
		for i := 0; i < b.N; i++ {
			nodeMerge(newNode, leftNode, rightNode)
		}
	})
}

type C struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			getNode: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				Assert(ok, "get node not exists")
				return node
			},
			newNode: func(node []byte) uint64 {
				Assert(BNode(node).nBytes() <= BTREE_PAGE_SIZE, "assert failed: new node over size")
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				Assert(pages[ptr] == nil, "assert failed: page should not exists")
				pages[ptr] = node
				return ptr
			},
			delNode: func(ptr uint64) {
				Assert(pages[ptr] != nil, "assert failed: delete page not exists")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val // reference data
}

func (c *C) del(key string) bool {
	deleted := c.tree.Delete([]byte(key))
	delete(c.ref, key)
	return deleted
}

func (c *C) getUtil(node BNode, key []byte) ([]byte, bool) {
	switch node.bType() {
	case BNODE_NODE:
		for i := uint16(0); i < node.nKeys(); i++ {
			res, resume := c.getUtil(c.tree.getNode(node.getPtr(i)), key)
			if res != nil {
				return res, false
			}
			if resume {
				continue
			} else {
				return nil, false
			}
		}
	case BNODE_LEAF:
		idx := nodeLookupLEBinary(node, key)
		foundKey := node.getKey(idx)
		if bytes.Equal(foundKey, key) {
			return foundKey, false
		}
		// foundKey > key
		if bytes.Compare(foundKey, key) > 0 {
			return nil, false
		}
		return nil, true
	default:
		Assertf(false, "unknown node type %d", node.bType())
	}
	return nil, false
}

func (c *C) get(key string) ([]byte, bool) {
	if c.tree.root == 0 {
		return nil, false
	}
	return c.getUtil(c.tree.getNode(c.tree.root), []byte(key))
}

func (c *C) traversalUtil(node BNode, fn func(key []byte, val []byte)) {
	if node == nil {
		return
	}
	switch node.bType() {
	case BNODE_NODE:
		for i := uint16(0); i < node.nKeys(); i++ {
			subNode := c.tree.getNode(node.getPtr(i))
			c.traversalUtil(subNode, fn)
		}
	case BNODE_LEAF:
		for i := uint16(0); i < node.nKeys(); i++ {
			fn(node.getKey(i), node.getVal(i))
		}
	default:
		Assertf(false, "unknown node type: %v", node.bType())
	}
}

func (c *C) traversal(fn func(key []byte, val []byte)) {
	c.traversalUtil(c.tree.getNode(c.tree.root), fn)
}

func TestTree(t *testing.T) {
	ctree := newC()

	for i := uint16(0); i < 4096; i++ {
		ctree.add(string(keyOf(i)), string(valueOf(i)))
	}

	idx := uint16(0)
	next := func() (key []byte, val []byte) {
		a, b := keyOf(idx), valueOf(idx)
		idx++
		return a, b
	}

	first := true
	ctree.traversal(func(key []byte, val []byte) {
		if first {
			first = false
			return
		}
		expectKey, expectVal := next()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, val)
	})

	for i := uint16(0); i < 30; i++ {
		ctree.del(string(keyOf(i)))
	}

	idx = 30
	first = true
	ctree.traversal(func(key []byte, val []byte) {
		if first {
			first = false
			return
		}
		expectKey, expectVal := next()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, val)
	})
}
