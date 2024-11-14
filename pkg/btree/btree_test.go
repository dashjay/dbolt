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
	"github.com/stretchr/testify/assert"
)

func TestBtree(t *testing.T) {
	t.Run("test node merge", func(t *testing.T) {
		const keyCount uint16 = 20
		leftNode := make(bnode.Node, constants.BtreePageSize)
		leftNode.SetHeader(bnode.NodeTypeLeaf, uint16(keyCount))
		rightNode := make(bnode.Node, constants.BtreePageSize)
		rightNode.SetHeader(bnode.NodeTypeLeaf, uint16(keyCount))

		for i := uint16(0); i < keyCount; i++ {
			bnode.NodeAppendKVOrPtr(leftNode, i, 1, utils.GenTestKey(uint64(i)), utils.GenTestValue(uint64(i)))
			bnode.NodeAppendKVOrPtr(rightNode, i, 1, utils.GenTestKey(uint64(i+keyCount)), utils.GenTestValue(uint64(i+keyCount)))
		}
		newNode := make(bnode.Node, constants.BtreePageSize)
		nodeMerge(newNode, leftNode, rightNode)

		assert.Equal(t, 2*keyCount, newNode.KeyCounts())

		for i := uint16(0); i < keyCount; i++ {
			assert.Equal(t, utils.GenTestKey(uint64(i)), newNode.GetKey(i))
			assert.Equal(t, utils.GenTestValue(uint64(i)), newNode.GetVal(i))
			assert.Equal(t, utils.GenTestKey(uint64(i+keyCount)), newNode.GetKey(i+keyCount))
			assert.Equal(t, utils.GenTestValue(uint64(i+keyCount)), newNode.GetVal(i+keyCount))
		}
	})

	ctree := newC()

	for i := uint64(0); i < 4096; i++ {
		ctree.add(utils.GenTestKey(i), utils.GenTestValue(i))

		// add the same keys again!
		ctree.add(utils.GenTestKey(i), utils.GenTestValue(i))
	}

	idx := uint64(0)
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

	for i := uint64(0); i < 30; i++ {
		assert.True(t, ctree.del(utils.GenTestKey(i)))
	}

	// delete key not exists
	for i := uint64(0); i < 30; i++ {
		assert.False(t, ctree.del(utils.GenTestKey(i)))
	}

	idx = 30
	ctree.traversal(func(key []byte, val []byte) {
		expectKey, expectVal := next()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, val)
	})

	for i := uint64(1000); i < 4096; i++ {
		val, ok := ctree.get(utils.GenTestKey(i))
		assert.True(t, ok)
		assert.Equal(t, utils.GenTestValue(i), val)
	}

	// random delete
	for i := 1; i < 4096; i++ {
		key := utils.GenTestKey(uint64(rand.Intn(i)))
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
		leftNode := make(bnode.Node, constants.BtreePageSize)
		leftNode.SetHeader(bnode.NodeTypeLeaf, keyCount)
		rightNode := make(bnode.Node, constants.BtreePageSize)
		rightNode.SetHeader(bnode.NodeTypeLeaf, keyCount)

		for i := uint16(0); i < keyCount; i++ {
			bnode.NodeAppendKVOrPtr(leftNode, i, 1, utils.GenTestKey(uint64(i)), utils.GenTestValue(uint64(i)))
			bnode.NodeAppendKVOrPtr(rightNode, i, 1, utils.GenTestKey(uint64(i+keyCount)), utils.GenTestValue(uint64(i+keyCount)))
		}

		b.ResetTimer()
		newNode := make(bnode.Node, constants.BtreePageSize)
		for i := 0; i < b.N; i++ {
			nodeMerge(newNode, leftNode, rightNode)
		}
	})

	const OpCount = 1_000_000 // 1 million

	genTestKey := func(i uint64) []byte {
		return []byte(fmt.Sprintf("key-%08d", i))
	}

	genTestValue := func(i uint64) []byte {
		return []byte(fmt.Sprintf("value-%08d", i))
	}

	getTree := func() *C {
		ctree := newC()
		bar := progressbar.NewOptions64(OpCount, progressbar.OptionSetDescription("adding data"))
		for i := uint64(0); i < OpCount; i++ {
			_ = bar.Add(1)
			ctree.add(genTestKey(i), genTestValue(i))
		}
		return ctree
	}

	var ctree *C
	b.Run("benchmark tree add", func(b *testing.B) {
		ctree = getTree()
	})
	b.Run("benchmark tree get", func(b *testing.B) {
		b.ResetTimer()
		bar := progressbar.NewOptions64(OpCount, progressbar.OptionSetDescription("getting data"))
		for i := uint64(0); i < OpCount; i++ {
			_ = bar.Add(1)
			_, _ = ctree.get(genTestKey(i))
		}
	})
	b.Run("benchmark tree delete", func(b *testing.B) {
		bar := progressbar.NewOptions64(OpCount, progressbar.OptionSetDescription("deleting data"))
		b.ResetTimer()
		for i := uint64(0); i < OpCount; i++ {
			_ = bar.Add(1)
			ctree.del(genTestKey(i))
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
		utils.Assert(bnode.Node(node).SizeBytes() <= constants.BtreePageSize, "assertion failed: new node over size")
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
	const N = 100_000 // 100k
	bar := progressbar.NewOptions(N, progressbar.OptionSetDescription("adding keys"))
	for i := 0; i < N; i++ {
		bar.Add(1)
		cTree.add(KeyOfInt(i), ValueOfInt(i))
	}

	bar = progressbar.NewOptions(N, progressbar.OptionSetDescription("getting keys"))

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

	bar = progressbar.NewOptions(N, progressbar.OptionSetDescription("iter keys"))
	cursor := cTree.tree.NewTreeCursor()

	for key, value := cursor.SeekToFirst(); key != nil; key, value = cursor.Next() {
		bar.Add(1)
		expectKey, expectVal := nextKeyValuePair()
		assert.Equal(t, expectKey, key)
		assert.Equal(t, expectVal, value)
	}

	bar = progressbar.NewOptions(N, progressbar.OptionSetDescription("del keys"))
	for i := 0; i < N; i++ {
		bar.Add(1)
		deleted := cTree.del(KeyOfInt(i))
		assert.True(t, deleted)
	}

	bar = progressbar.NewOptions(N, progressbar.OptionSetDescription("del keys again"))
	for i := 0; i < N; i++ {
		bar.Add(1)
		deleted := cTree.del(KeyOfInt(i))
		assert.False(t, deleted)
	}
}

func TestTreeCursor(t *testing.T) {
	cTree := newC()

	const count = math.MaxUint16
	for i := uint64(0); i < count; i++ {
		cTree.add(utils.GenTestKey(i), utils.GenTestValue(i))
	}

	cursor := cTree.tree.NewTreeCursor()
	key, value := cursor.SeekToFirst()
	assert.Equal(t, utils.GenTestKey(0), key)
	assert.Equal(t, utils.GenTestValue(0), value)

	for i := uint64(1); i < count; i++ {
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
		assert.Equal(t, utils.GenTestKey(uint64(i)), key)
		assert.Equal(t, utils.GenTestValue(uint64(i)), value)
	}

	keyNotExists := append(utils.GenTestKey(math.MaxUint16/2), '0')
	key, value = cursor.Seek(keyNotExists)
	assert.Nil(t, key)
	assert.Nil(t, value)
	key, value = cursor.Next()
	assert.Equal(t, utils.GenTestKey(math.MaxUint16/2+1), key)
	assert.Equal(t, utils.GenTestValue(math.MaxUint16/2+1), value)
}
