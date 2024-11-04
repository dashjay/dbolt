package btree

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/dashjay/dbolt/pkg/bnode"
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
	"github.com/schollz/progressbar/v3"
	"github.com/stretchr/testify/assert"
)

func TestBtree(t *testing.T) {
	t.Run("test node merge", func(t *testing.T) {
		const keyCount uint16 = 20
		leftNode := make(bnode.Node, constants.BTREE_PAGE_SIZE)
		leftNode.SetHeader(bnode.NodeTypeLeaf, keyCount)
		rightNode := make(bnode.Node, constants.BTREE_PAGE_SIZE)
		rightNode.SetHeader(bnode.NodeTypeLeaf, keyCount)

		for i := uint16(0); i < keyCount; i++ {
			bnode.NodeAppendKVOrPtr(leftNode, i, 1, utils.GenTestKey(i), utils.GenTestValue(i))
			bnode.NodeAppendKVOrPtr(rightNode, i, 1, utils.GenTestKey(i+keyCount), utils.GenTestValue(i+keyCount))
		}
		newNode := make(bnode.Node, constants.BTREE_PAGE_SIZE)
		nodeMerge(newNode, leftNode, rightNode)

		assert.Equal(t, 2*keyCount, newNode.KeyCounts())

		for i := uint16(0); i < keyCount; i++ {
			assert.Equal(t, utils.GenTestKey(i), newNode.GetKey(i))
			assert.Equal(t, utils.GenTestValue(i), newNode.GetVal(i))
			assert.Equal(t, utils.GenTestKey(i+keyCount), newNode.GetKey(i+keyCount))
			assert.Equal(t, utils.GenTestValue(i+keyCount), newNode.GetVal(i+keyCount))
		}
	})

	ctree := newC()

	for i := uint16(0); i < 4096; i++ {
		ctree.add(utils.GenTestKey(i), utils.GenTestValue(i))

		// add the same keys again!
		ctree.add(utils.GenTestKey(i), utils.GenTestValue(i))
	}

	idx := uint16(0)
	next := func() (key []byte, val []byte) {
		a, b := utils.GenTestKey(idx), utils.GenTestValue(idx)
		idx++
		return a, b
	}

	ctree.traversal(func(key []byte, val []byte) {
		expectKey, expectVal := next()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, val)
	})

	for i := uint16(0); i < 30; i++ {
		assert.True(t, ctree.del(utils.GenTestKey(i)))
	}

	// delete key not exists
	for i := uint16(0); i < 30; i++ {
		assert.False(t, ctree.del(utils.GenTestKey(i)))
	}

	idx = 30
	ctree.traversal(func(key []byte, val []byte) {
		expectKey, expectVal := next()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, val)
	})

	for i := uint16(1000); i < 4096; i++ {
		val, ok := ctree.get(utils.GenTestKey(i))
		assert.True(t, ok)
		assert.Equal(t, utils.GenTestValue(i), val)
	}

	// random delete
	for i := 1; i < 4096; i++ {
		key := utils.GenTestKey(uint16(rand.Intn(i)))
		_, exists := ctree.get(key)
		deleted := ctree.del(key)
		assert.True(t, exists == deleted)
	}

	// delete all left keys
	var leftKeys [][]byte
	ctree.traversal(func(key []byte, val []byte) {
		leftKeys = append(leftKeys, append([]byte(nil), key...))
	})
	for _, key := range leftKeys {
		assert.True(t, ctree.del(key))
	}

	ctree.tree.DebugGetNode(ctree.tree.Root())
}

func BenchmarkBtree(b *testing.B) {
	const keyCount uint16 = 30
	b.Run("benchmark node merge", func(b *testing.B) {
		leftNode := make(bnode.Node, constants.BTREE_PAGE_SIZE)
		leftNode.SetHeader(bnode.NodeTypeLeaf, keyCount)
		rightNode := make(bnode.Node, constants.BTREE_PAGE_SIZE)
		rightNode.SetHeader(bnode.NodeTypeLeaf, keyCount)

		for i := uint16(0); i < keyCount; i++ {
			bnode.NodeAppendKVOrPtr(leftNode, i, 1, utils.GenTestKey(i), utils.GenTestValue(i))
			bnode.NodeAppendKVOrPtr(rightNode, i, 1, utils.GenTestKey(i+keyCount), utils.GenTestValue(i+keyCount))
		}

		b.ResetTimer()
		newNode := make(bnode.Node, constants.BTREE_PAGE_SIZE)
		for i := 0; i < b.N; i++ {
			nodeMerge(newNode, leftNode, rightNode)
		}
	})

	b.Run("benchmark tree add", func(b *testing.B) {
		ctree := newC()
		for i := 0; i < b.N; i++ {
			idx := uint16(i % constants.BTREE_PAGE_SIZE)
			ctree.add(utils.GenTestKey(idx), utils.GenTestValue(idx))
		}
	})

	ctree := newC()
	for i := uint16(0); i < 4096; i++ {
		ctree.add(utils.GenTestKey(i), utils.GenTestValue(i))
	}

	b.Run("benchmark tree delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx := uint16(i % constants.BTREE_PAGE_SIZE)
			ctree.del(utils.GenTestKey(idx))
		}
	})

	// recover
	for i := uint16(0); i < 4096; i++ {
		ctree.add(utils.GenTestKey(i), utils.GenTestValue(i))
	}

	b.Run("benchmark tree get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = ctree.get(utils.GenTestKey(uint16(i % 8192)))
		}
	})

	b.Run("benchmark tree traversal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctree.traversal(func(key []byte, val []byte) {
				_ = key
				_ = val
			})
		}
	})
}

type C struct {
	tree  Tree
	ref   map[string]string     // the reference data
	pages map[uint64]bnode.Node // in-memory pages
}

func newC() *C {
	pages := map[uint64]bnode.Node{}
	getNode := func(ptr uint64) []byte {
		node, ok := pages[ptr]
		utils.Assert(ok, "get node not exists")
		return node
	}
	newNode := func(node []byte) uint64 {
		utils.Assert(bnode.Node(node).SizeBytes() <= constants.BTREE_PAGE_SIZE, "assertion failed: new node over size")
		ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
		utils.Assert(pages[ptr] == nil, "assertion failed: page should not exists")
		pages[ptr] = node
		return ptr
	}
	delNode := func(ptr uint64) {
		utils.Assert(pages[ptr] != nil, "assertion failed: delete page not exists")
		delete(pages, ptr)
	}
	tree := NewTree(getNode, newNode, delNode)
	tree.SetRoot(0)
	return &C{
		tree:  tree,
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

func (c *C) get(key []byte) ([]byte, bool) {
	return c.tree.Get(key)
}

func (c *C) traversalUtil(node bnode.Node, fn func(key []byte, val []byte), first *bool) {
	if node == nil {
		return
	}

	switch node.Type() {
	case bnode.NodeTypeNode:
		for i := uint16(0); i < node.KeyCounts(); i++ {
			subNode := c.tree.getNode(node.GetPtr(i))
			c.traversalUtil(subNode, fn, first)
		}
	case bnode.NodeTypeLeaf:
		for i := uint16(0); i < node.KeyCounts(); i++ {
			if *first {
				*first = false
				utils.Assertf(len(node.GetKey(i)) == 0, "first key is empty but has length %d, idx: %d", len(node.GetKey(i)), i)
				utils.Assertf(len(node.GetVal(i)) == 0, "first value is empty but has length %d, idx: %d", len(node.GetVal(i)), i)
				continue
			}
			fn(node.GetKey(i), node.GetVal(i))
		}
	default:
		utils.Assertf(false, "unknown node type: %v", node.Type())
	}
}

func (c *C) traversal(fn func(key []byte, val []byte)) {
	first := true
	c.traversalUtil(c.tree.getNode(c.tree.root), fn, &first)
}

func TestBTreeWithProgressingBar(t *testing.T) {
	cTree := newC()

	// for coverage (get & delete on empty tree
	{
		val, exists := cTree.get([]byte(nil))
		assert.Nil(t, val)
		assert.False(t, exists)
		deleted := cTree.del([]byte(nil))
		assert.False(t, deleted)
	}

	KeyOfInt := func(i int) []byte {
		return []byte(fmt.Sprintf("key-%08d", i))
	}
	ValueOfInt := func(i int) []byte {
		return []byte(fmt.Sprintf("value-%08d", i))
	}
	const N = 1_000_000 // 1 million
	bar := progressbar.Default(N, "adding keys")
	for i := 0; i < N; i++ {
		bar.Add(1)
		cTree.add(KeyOfInt(i), ValueOfInt(i))
	}

	bar = progressbar.Default(N, "getting keys")

	for i := 0; i < N; i++ {
		bar.Add(1)
		val, exists := cTree.get(KeyOfInt(i))
		assert.True(t, exists)
		assert.Equal(t, val, ValueOfInt(i))
		val, exists = cTree.get(append(KeyOfInt(i), []byte("not-exists")...))
		assert.False(t, exists)
		assert.Nil(t, val)
	}

	idx := 0
	nextKeyValuePair := func() ([]byte, []byte) {
		defer func() {
			idx++
		}()
		return KeyOfInt(idx), ValueOfInt(idx)
	}

	bar = progressbar.Default(N, "iter keys")
	cTree.traversal(func(key []byte, val []byte) {
		bar.Add(1)
		expectKey, expectVal := nextKeyValuePair()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, val)
	})

	bar = progressbar.Default(N, "del keys")
	for i := 0; i < N; i++ {
		bar.Add(1)
		deleted := cTree.del(KeyOfInt(i))
		assert.True(t, deleted)
	}

	bar = progressbar.Default(N, "del keys again")
	for i := 0; i < N; i++ {
		bar.Add(1)
		deleted := cTree.del(KeyOfInt(i))
		assert.False(t, deleted)
	}
}

func TestTreeCursor(t *testing.T) {
	cTree := newC()

	const count = math.MaxUint16
	for i := uint16(0); i < count; i++ {
		cTree.add(utils.GenTestKey(i), utils.GenTestValue(i))
	}

	cursor := cTree.tree.NewTreeCursor()
	key, value := cursor.SeekToFirst()
	assert.Equal(t, utils.GenTestKey(0), key)
	assert.Equal(t, utils.GenTestValue(0), value)

	for i := uint16(1); i < count; i++ {
		key, value = cursor.Next()
		assert.Equal(t, utils.GenTestKey(i), key)
		assert.Equal(t, utils.GenTestValue(i), value)
		assert.Equal(t, utils.GenTestKey(i), cursor.Key())
		assert.Equal(t, utils.GenTestValue(i), cursor.Value())
	}

	key, value = cursor.Seek(utils.GenTestKey(math.MaxUint16 / 2))
	assert.Equal(t, key, utils.GenTestKey(math.MaxUint16/2))
	assert.Equal(t, value, utils.GenTestValue(math.MaxUint16/2))

	for i := uint16(math.MaxUint16/2 + 1); i < math.MaxUint16; i++ {
		key, value = cursor.Next()
		assert.Equal(t, utils.GenTestKey(i), key)
		assert.Equal(t, utils.GenTestValue(i), value)
	}
}
