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

	ctree := newC()

	for i := uint16(0); i < 4096; i++ {
		ctree.add(keyOf(i), valueOf(i))
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
		assert.True(t, ctree.del(keyOf(i)))
	}

	// delete key not exists
	for i := uint16(0); i < 30; i++ {
		assert.False(t, ctree.del(keyOf(i)))
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

	//t.Logf("%s", hex.Dump(ctree.tree.getNode(ctree.tree.root)))

	for i := uint16(1000); i < 4096; i++ {
		val, ok := ctree.get(keyOf(i))
		assert.True(t, ok)
		assert.Equal(t, valueOf(i), val)
	}
}

func BenchmarkBtree(b *testing.B) {
	const keyCount uint16 = 30
	b.Run("benchmark node merge", func(b *testing.B) {
		leftNode := make(BNode, BTREE_PAGE_SIZE)
		leftNode.setHeader(BNODE_LEAF, keyCount)
		rightNode := make(BNode, BTREE_PAGE_SIZE)
		rightNode.setHeader(BNODE_LEAF, keyCount)

		for i := uint16(0); i < keyCount; i++ {
			nodeAppendKV(leftNode, i, 1, keyOf(i), valueOf(i))
			nodeAppendKV(rightNode, i, 1, keyOf(i+keyCount), valueOf(i+keyCount))
		}

		b.ResetTimer()
		newNode := make(BNode, BTREE_PAGE_SIZE)
		for i := 0; i < b.N; i++ {
			nodeMerge(newNode, leftNode, rightNode)
		}
	})

	b.Run("benchmark tree add", func(b *testing.B) {
		ctree := newC()
		for i := 0; i < b.N; i++ {
			idx := uint16(i % BTREE_PAGE_SIZE)
			ctree.add(keyOf(idx), valueOf(idx))
		}
	})

	ctree := newC()
	for i := uint16(0); i < 4096; i++ {
		ctree.add(keyOf(i), valueOf(i))
	}

	b.Run("benchmark tree delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx := uint16(i % BTREE_PAGE_SIZE)
			ctree.del(keyOf(idx))
		}
	})

	// recover
	for i := uint16(0); i < 4096; i++ {
		ctree.add(keyOf(i), valueOf(i))
	}

	b.Run("benchmark tree get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = ctree.get(keyOf(uint16(i % 8192)))
		}
	})

	b.Run("benchmark tree traversal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctree.traversal(func(key []byte, val []byte) {
				// do nothing
			})
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

func (c *C) add(key, val []byte) {
	c.tree.Insert(key, val)
	c.ref[string(key)] = string(val) // reference data
}

func (c *C) del(key []byte) bool {
	deleted := c.tree.Delete(key)
	delete(c.ref, string(key))
	return deleted
}

// getUtil return value and if found
func (c *C) getUtil(node BNode, key []byte) ([]byte, bool) {
	if node == nil {
		return nil, false
	}
	switch node.bType() {
	case BNODE_NODE:
		idx := nodeLookupLEBinary(node, key)
		n := bytes.Compare(node.getKey(idx), key)
		if n <= 0 {
			return c.getUtil(c.tree.getNode(node.getPtr(idx)), key)
		}
		return c.getUtil(c.tree.getNode(node.getPtr(idx-1)), key)
	case BNODE_LEAF:
		idx := nodeLookupLEBinary(node, key)
		if bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
		return nil, false
	default:
		Assertf(false, "assert failed: unknown node type %d", node.bType())
	}
	Assertf(false, "assert failed: never happend here getUtil.")
	return nil, false
}

func (c *C) get(key []byte) ([]byte, bool) {
	if c.tree.root == 0 {
		return nil, false
	}
	return c.getUtil(c.tree.getNode(c.tree.root), key)
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
