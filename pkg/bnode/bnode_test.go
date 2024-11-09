package bnode

import (
	"testing"

	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestBnode(t *testing.T) {
	var originNode = make(Node, constants.BTREE_PAGE_SIZE)

	const keyCount uint16 = 70
	originNode.SetHeader(NodeTypeLeaf, keyCount)

	assert.Equal(t, NodeTypeLeaf, originNode.Type(), "should be leaf node")
	assert.Equal(t, keyCount, originNode.KeyCounts(), "should have %d keys, %d got", keyCount, originNode.KeyCounts())

	for i := uint16(0); i < keyCount; i++ {
		NodeAppendKVOrPtr(originNode, i, 0, utils.GenTestKey(uint64(i)), utils.GenTestValue(uint64(i)))
	}

	for i := uint16(0); i < originNode.KeyCounts(); i++ {
		assert.Equal(t, utils.GenTestKey(uint64(i)), originNode.GetKey(i))
		assert.Equal(t, utils.GenTestValue(uint64(i)), originNode.GetVal(i))
	}

	t.Logf("originNode bytes: %d", originNode.SizeBytes())

	newNode := make(Node, constants.BTREE_PAGE_SIZE)

	newNode.SetHeader(NodeTypeLeaf, keyCount)

	NodeAppendKVOrPtrRange(newNode, originNode, 0, 0, keyCount)

	assert.Equal(t, originNode, newNode)

	for i := uint16(0); i < originNode.KeyCounts(); i++ {
		assert.Equal(t, originNode.GetKey(i), newNode.GetKey(i))
		assert.Equal(t, originNode.GetVal(i), newNode.GetVal(i))
	}

	t.Run("test node append range 2", func(t *testing.T) {
		tmpNode := make(Node, constants.BTREE_PAGE_SIZE)
		tmpNode.SetHeader(NodeTypeLeaf, keyCount)
		nodeAppendRange2(tmpNode, originNode, 0, 0, keyCount)
		for i := uint16(0); i < originNode.KeyCounts(); i++ {
			assert.Equal(t, originNode.GetKey(i), tmpNode.GetKey(i))
			assert.Equal(t, originNode.GetVal(i), tmpNode.GetVal(i))
			if i >= 1 {
				assert.Equal(t, originNode._offsetPos(i), tmpNode._offsetPos(i))
			}
		}

		// if we pass the following test, we can use nodeAppendRange2 to replace NodeAppendKVOrPtrRange
		// for higher performance
		//tmpNode2 := make(Node, constants.BTREE_PAGE_SIZE)
		//tmpNode2.SetHeader(NodeTypeLeaf, keyCount)
		//NodeAppendKVOrPtrRange(tmpNode2, originNode, 0, 0, keyCount)
		//assert.Equal(t, tmpNode, tmpNode2)
	})

	// [0, 1, 2, 3, 4, ......, n]
	// insert to idx
	// [0, 1, 2, 3, "inserted", 4,.... n]
	t.Run("test node insert", func(t *testing.T) {
		leafInsertTest := func(idx uint16) {
			tmpNode := make(Node, constants.BTREE_PAGE_SIZE)
			tmpNode.SetHeader(NodeTypeLeaf, keyCount+1)
			testKey := []byte("inserted-key")
			testVal := []byte("inserted-value")
			LeafInsert(tmpNode, originNode, idx, testKey, testVal)
			assert.Equal(t, testKey, tmpNode.GetKey(idx))
			assert.Equal(t, testVal, tmpNode.GetVal(idx))

			for i := uint16(0); i < tmpNode.KeyCounts(); i++ {
				if i == idx {
					assert.Equal(t, testKey, tmpNode.GetKey(i))
					assert.Equal(t, testVal, tmpNode.GetVal(i))
				} else if i < idx {
					assert.Equal(t, utils.GenTestKey(uint64(i)), tmpNode.GetKey(i))
					assert.Equal(t, utils.GenTestValue(uint64(i)), tmpNode.GetVal(i))
				} else {
					assert.Equal(t, utils.GenTestKey(uint64(i-1)), tmpNode.GetKey(i))
					assert.Equal(t, utils.GenTestValue(uint64(i-1)), tmpNode.GetVal(i))
				}
			}
		}
		for i := uint16(0); i < originNode.KeyCounts(); i++ {
			leafInsertTest(i)
		}
	})

	t.Run("test leaf node update x", func(t *testing.T) {
		leafUpdateTest := func(idx uint16) {
			tmpNode := make(Node, constants.BTREE_PAGE_SIZE)
			tmpNode.SetHeader(NodeTypeLeaf, keyCount)
			testKey := []byte("updated-key")
			testVal := []byte("updated-value")
			LeafUpdate(tmpNode, originNode, idx, testKey, testVal)
			assert.Equal(t, testKey, tmpNode.GetKey(idx))
			assert.Equal(t, testVal, tmpNode.GetVal(idx))
			for i := uint16(0); i < tmpNode.KeyCounts(); i++ {
				if i == idx {
					assert.Equal(t, testKey, tmpNode.GetKey(i))
					assert.Equal(t, testVal, tmpNode.GetVal(i))
				} else {
					assert.Equal(t, utils.GenTestKey(uint64(i)), tmpNode.GetKey(i))
					assert.Equal(t, utils.GenTestValue(uint64(i)), tmpNode.GetVal(i))
				}
			}
		}
		for i := uint16(0); i < originNode.KeyCounts(); i++ {
			leafUpdateTest(i)
		}
	})

	t.Run("test leaf node delete x", func(t *testing.T) {
		leafDeleteTest := func(idx uint16) {
			tmpNode := make(Node, constants.BTREE_PAGE_SIZE)
			LeafDelete(tmpNode, originNode, idx)
			assert.Equal(t, keyCount-1, tmpNode.KeyCounts())
			for i := uint16(0); i < tmpNode.KeyCounts(); i++ {
				if i < idx {
					assert.Equal(t, utils.GenTestKey(uint64(i)), tmpNode.GetKey(i))
					assert.Equal(t, utils.GenTestValue(uint64(i)), tmpNode.GetVal(i))
				} else {
					assert.Equal(t, utils.GenTestKey(uint64(i+1)), tmpNode.GetKey(i))
					assert.Equal(t, utils.GenTestValue(uint64(i+1)), tmpNode.GetVal(i))
				}
			}
		}

		for i := uint16(0); i < originNode.KeyCounts(); i++ {
			leafDeleteTest(i)
		}
	})

	t.Run("test search", func(t *testing.T) {
		for i := uint16(0); i < originNode.KeyCounts(); i++ {
			idx := NodeLookupLE(originNode, utils.GenTestKey(uint64(i)))
			assert.Equal(t, i, idx)
		}
	})

	t.Run("test bin search", func(t *testing.T) {
		for i := uint16(0); i < originNode.KeyCounts(); i++ {
			idx := NodeLookupLEBinary(originNode, utils.GenTestKey(uint64(i)))
			assert.Equal(t, i, idx)
		}
	})

	t.Run("test node split 2 / split 3", func(t *testing.T) {
		bigNode := make(Node, constants.BTREE_PAGE_SIZE*2)
		bigNode.SetHeader(NodeTypeLeaf, 167)
		for i := uint16(0); i < bigNode.KeyCounts(); i++ {
			NodeAppendKVOrPtr(bigNode, i, 0, utils.GenTestKey(uint64(i)), utils.GenTestValue(uint64(i)))
		}
		// must bigger than one page
		assert.Greater(t, bigNode.SizeBytes(), uint16(constants.BTREE_PAGE_SIZE))

		leftNode := make(Node, constants.BTREE_PAGE_SIZE)
		rightNode := make(Node, constants.BTREE_PAGE_SIZE)
		nodeSplit2(leftNode, rightNode, bigNode)

		nextKey := uint16(0)

		getNextKeys := func() []byte {
			key := utils.GenTestKey(uint64(nextKey))
			nextKey++
			return key
		}

		t.Logf("bigNode has %d keys, after spliting left node has %d keys and right node has %d keys",
			bigNode.KeyCounts(), leftNode.KeyCounts(), rightNode.KeyCounts())
		for i := uint16(0); i < leftNode.KeyCounts(); i++ {
			assert.Equal(t, leftNode.GetKey(i), getNextKeys())
		}

		for i := uint16(0); i < rightNode.KeyCounts(); i++ {
			assert.Equal(t, rightNode.GetKey(i), getNextKeys())
		}

		n, nodes := NodeSplit3(bigNode)
		assert.Equal(t, n, uint16(2))
		assert.Nil(t, nodes[2])

		assert.Equal(t, leftNode, nodes[0])
		assert.Equal(t, rightNode, nodes[1])

		superBigNode := make(Node, constants.BTREE_PAGE_SIZE*3)
		superBigNode.SetHeader(NodeTypeLeaf, 256)

		for i := uint16(0); i < superBigNode.KeyCounts(); i++ {
			NodeAppendKVOrPtr(superBigNode, i, 0, utils.GenTestKey(uint64(i)), utils.GenTestValue(uint64(i)))
		}

		n, nodes = NodeSplit3(superBigNode)
		assert.Equal(t, n, uint16(3))
		t.Logf("suprtBigNode has %d keys, after spliting there are 3 nodes with nkeys: %v",
			superBigNode.KeyCounts(), []uint16{nodes[0].KeyCounts(), nodes[1].KeyCounts(), nodes[2].KeyCounts()})
		// reset the next key
		nextKey = 0
		for _, node := range nodes {
			assert.NotNil(t, node)
			for i := uint16(0); i < node.KeyCounts(); i++ {
				assert.Equal(t, node.GetKey(i), getNextKeys())
			}
		}
	})

	t.Run("test node split 3 with small node", func(t *testing.T) {
		tmpNode := make(Node, constants.BTREE_PAGE_SIZE)
		tmpNode.SetHeader(NodeTypeLeaf, keyCount)
		NodeAppendKVOrPtrRange(tmpNode, originNode, 0, 0, keyCount)

		n, nodes := NodeSplit3(tmpNode)
		assert.Equal(t, uint16(1), n)
		assert.NotNil(t, nodes[0])
		assert.Nil(t, nodes[1])
		assert.Nil(t, nodes[2])
	})
}

func BenchmarkNode(b *testing.B) {
	var bnode = make(Node, constants.BTREE_PAGE_SIZE)
	const keyCount uint16 = 70
	bnode.SetHeader(NodeTypeLeaf, keyCount)
	for i := uint16(0); i < keyCount; i++ {
		NodeAppendKVOrPtr(bnode, i, 1, utils.GenTestKey(uint64(i)), utils.GenTestValue(uint64(i)))
	}

	b.Run("benchmark NodeAppendKVOrPtrRange", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			newNode := make(Node, constants.BTREE_PAGE_SIZE)
			newNode.SetHeader(NodeTypeLeaf, keyCount)
			NodeAppendKVOrPtrRange(newNode, bnode, 0, 0, keyCount)
		}
	})

	b.Run("benchmark nodeAppendRange2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			newNode := make(Node, constants.BTREE_PAGE_SIZE)
			newNode.SetHeader(NodeTypeLeaf, keyCount)
			nodeAppendRange2(newNode, bnode, 0, 0, keyCount)
		}
	})

	b.Run("benchmark LeafInsert", func(b *testing.B) {
		testKey := []byte("inserted-key")
		testVal := []byte("inserted-value")
		for i := 0; i < b.N; i++ {
			newNode := make(Node, constants.BTREE_PAGE_SIZE)
			newNode.SetHeader(NodeTypeLeaf, keyCount+1)
			LeafInsert(newNode, bnode, uint16(i)%keyCount, testKey, testVal)
		}
	})

	b.Run("benchmark LeafUpdate", func(b *testing.B) {
		testKey := []byte("updated-key")
		testVal := []byte("updated-value")
		for i := 0; i < b.N; i++ {
			newNode := make(Node, constants.BTREE_PAGE_SIZE)
			newNode.SetHeader(NodeTypeLeaf, keyCount)
			LeafUpdate(newNode, bnode, uint16(i)%keyCount, testKey, testVal)
		}
	})

	b.Run("benchmark search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = NodeLookupLE(bnode, utils.GenTestKey(uint64(i%int(keyCount))))
		}
	})

	b.Run("benchmark bin search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = NodeLookupLEBinary(bnode, utils.GenTestKey(uint64(i%int(keyCount))))
		}
	})
}
