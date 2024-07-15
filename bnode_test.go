package dbolt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func keyOf(i uint16) []byte {
	return []byte(fmt.Sprintf("key-%08d", i))
}

func valueOf(i uint16) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}

func TestBnode(t *testing.T) {
	var bnode = make(BNode, BTREE_PAGE_SIZE)

	const keyCount uint16 = 70
	bnode.setHeader(BNODE_LEAF, keyCount)

	assert.Equal(t, uint16(BNODE_LEAF), bnode.bType(), "should be leaf node")
	assert.Equal(t, keyCount, bnode.nKeys(), "should have %d keys, %d got", keyCount, bnode.nKeys())

	for i := uint16(0); i < keyCount; i++ {
		nodeAppendKVOrPtr(bnode, i, 1, keyOf(i), valueOf(i))
	}

	for i := uint16(0); i < bnode.nKeys(); i++ {
		assert.Equal(t, keyOf(i), bnode.getKey(i))
		assert.Equal(t, valueOf(i), bnode.getVal(i))
	}

	t.Logf("bnode bytes: %d", bnode.nBytes())

	newNode := make(BNode, BTREE_PAGE_SIZE)

	newNode.setHeader(BNODE_LEAF, keyCount)

	nodeAppendKVOrPtrRange(newNode, bnode, 0, 0, keyCount)

	//assert.Equal(t, bnode, newNode)

	for i := uint16(0); i < bnode.nKeys(); i++ {
		assert.Equal(t, bnode.getKey(i), newNode.getKey(i))
		assert.Equal(t, bnode.getVal(i), newNode.getVal(i))
	}

	t.Run("test node append range 2", func(t *testing.T) {
		newNode := make(BNode, BTREE_PAGE_SIZE)
		newNode.setHeader(BNODE_LEAF, keyCount)
		nodeAppendRange2(newNode, bnode, 0, 0, keyCount)
		for i := uint16(0); i < bnode.nKeys(); i++ {
			assert.Equal(t, bnode.getKey(i), newNode.getKey(i))
			assert.Equal(t, bnode.getVal(i), newNode.getVal(i))

			//assert.Equal(t, bnode.getPtr(i), newNode.getPtr(i))
			if i >= 1 {
				assert.Equal(t, bnode._offsetPos(i), newNode._offsetPos(i))
			}
		}
	})

	t.Run("test node insert", func(t *testing.T) {
		leafInsertTest := func(idx uint16) {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			newNode.setHeader(BNODE_LEAF, keyCount+1)
			testKey := []byte("inserted-key")
			testVal := []byte("inserted-value")
			leafInsert(newNode, bnode, idx, testKey, testVal)
			assert.Equal(t, testKey, newNode.getKey(idx))
			assert.Equal(t, testVal, newNode.getVal(idx))

			for i := uint16(0); i < newNode.nKeys(); i++ {
				if i == idx {
					continue
				} else if i < idx {
					assert.Equal(t, keyOf(i), newNode.getKey(i))
					assert.Equal(t, valueOf(i), newNode.getVal(i))
				} else {
					assert.Equal(t, keyOf(i-1), newNode.getKey(i))
					assert.Equal(t, valueOf(i-1), newNode.getVal(i))
				}
			}
		}
		for i := uint16(0); i < bnode.nKeys(); i++ {
			leafInsertTest(i)
		}
	})

	t.Run("test leaf node update x", func(t *testing.T) {
		leafUpdateTest := func(idx uint16) {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			newNode.setHeader(BNODE_LEAF, keyCount)
			testKey := []byte("updated-key")
			testVal := []byte("updated-value")
			leafUpdate(newNode, bnode, idx, testKey, testVal)
			assert.Equal(t, testKey, newNode.getKey(idx))
			assert.Equal(t, testVal, newNode.getVal(idx))
			for i := uint16(0); i < newNode.nKeys(); i++ {
				if i == idx {
					continue
				}
				assert.Equal(t, keyOf(i), newNode.getKey(i))
				assert.Equal(t, valueOf(i), newNode.getVal(i))
			}
		}
		for i := uint16(0); i < bnode.nKeys(); i++ {
			leafUpdateTest(i)
		}
	})

	t.Run("test leaf node delete x", func(t *testing.T) {
		leafDeleteTest := func(idx uint16) {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			leafDelete(newNode, bnode, idx)
			for i := uint16(0); i < newNode.nKeys(); i++ {
				if i == idx {
					// has benn deleted
					continue
				} else if i < idx {
					assert.Equal(t, keyOf(i), newNode.getKey(i))
					assert.Equal(t, valueOf(i), newNode.getVal(i))
				} else {
					assert.Equal(t, keyOf(i+1), newNode.getKey(i))
					assert.Equal(t, valueOf(i+1), newNode.getVal(i))
				}
			}
		}

		for i := uint16(0); i < bnode.nKeys(); i++ {
			leafDeleteTest(i)
		}
	})

	t.Run("test search", func(t *testing.T) {
		for i := uint16(1); i < bnode.nKeys(); i++ {
			idx := nodeLookupLE(bnode, keyOf(i))
			assert.Equal(t, i, idx)
		}
	})

	t.Run("test bin search", func(t *testing.T) {
		for i := uint16(1); i < bnode.nKeys(); i++ {
			idx := nodeLookupLEBinary(bnode, keyOf(i))
			assert.Equal(t, i, idx)
		}
	})

	t.Run("test node split 2 / split 3", func(t *testing.T) {
		bigNode := make(BNode, BTREE_PAGE_SIZE*2)
		bigNode.setHeader(BNODE_LEAF, 191)
		for i := uint16(0); i < bigNode.nKeys(); i++ {
			nodeAppendKVOrPtr(bigNode, i, 0, keyOf(i), valueOf(i))
		}
		// must bigger than one page
		assert.Greater(t, bigNode.nBytes(), uint16(BTREE_PAGE_SIZE))

		leftNode := make(BNode, BTREE_PAGE_SIZE)
		rightNode := make(BNode, BTREE_PAGE_SIZE)
		nodeSplit2(leftNode, rightNode, bigNode)

		nextKey := uint16(0)

		getNextKeys := func() []byte {
			key := keyOf(nextKey)
			nextKey++
			return key
		}

		t.Logf("bigNode has %d keys, after spliting left node has %d keys and right node has %d keys",
			bigNode.nKeys(), leftNode.nKeys(), rightNode.nKeys())
		for i := uint16(0); i < leftNode.nKeys(); i++ {
			assert.Equal(t, leftNode.getKey(i), getNextKeys())
		}

		for i := uint16(0); i < rightNode.nKeys(); i++ {
			assert.Equal(t, rightNode.getKey(i), getNextKeys())
		}

		n, nodes := nodeSplit3(bigNode)
		assert.Equal(t, n, uint16(2))
		assert.Nil(t, nodes[2])

		assert.Equal(t, leftNode, nodes[0])
		assert.Equal(t, rightNode, nodes[1])

		superBigNode := make(BNode, BTREE_PAGE_SIZE*3)
		superBigNode.setHeader(BNODE_LEAF, 256)

		for i := uint16(0); i < superBigNode.nKeys(); i++ {
			nodeAppendKVOrPtr(superBigNode, i, 0, keyOf(i), valueOf(i))
		}

		n, nodes = nodeSplit3(superBigNode)
		assert.Equal(t, n, uint16(3))
		t.Logf("suprtBigNode has %d keys, after spliting there are 3 nodes with nkeys: %v",
			superBigNode.nKeys(), []uint16{nodes[0].nKeys(), nodes[1].nKeys(), nodes[2].nKeys()})
		// reset next key
		nextKey = 0
		for _, node := range nodes {
			assert.NotNil(t, node)
			for i := uint16(0); i < node.nKeys(); i++ {
				assert.Equal(t, node.getKey(i), getNextKeys())
			}
		}
	})
}

func BenchmarkNode(b *testing.B) {
	var bnode = make(BNode, BTREE_PAGE_SIZE)
	const keyCount uint16 = 70
	bnode.setHeader(BNODE_LEAF, keyCount)
	for i := uint16(0); i < keyCount; i++ {
		nodeAppendKVOrPtr(bnode, i, 1, keyOf(i), valueOf(i))
	}

	b.Run("benchmark nodeAppendKVOrPtrRange", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			newNode.setHeader(BNODE_LEAF, keyCount)
			nodeAppendKVOrPtrRange(newNode, bnode, 0, 0, keyCount)
		}
	})

	b.Run("benchmark nodeAppendRange2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			newNode.setHeader(BNODE_LEAF, keyCount)
			nodeAppendRange2(newNode, bnode, 0, 0, keyCount)
		}
	})

	b.Run("benchmark leafInsert", func(b *testing.B) {
		testKey := []byte("inserted-key")
		testVal := []byte("inserted-value")
		for i := 0; i < b.N; i++ {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			newNode.setHeader(BNODE_LEAF, keyCount+1)
			leafInsert(newNode, bnode, uint16(i)%keyCount, testKey, testVal)
		}
	})

	b.Run("benchmark leafUpdate", func(b *testing.B) {
		testKey := []byte("updated-key")
		testVal := []byte("updated-value")
		for i := 0; i < b.N; i++ {
			newNode := make(BNode, BTREE_PAGE_SIZE)
			newNode.setHeader(BNODE_LEAF, keyCount)
			leafUpdate(newNode, bnode, uint16(i)%keyCount, testKey, testVal)
		}
	})

	b.Run("benchmark search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = nodeLookupLE(bnode, keyOf(uint16(i%int(keyCount))))
		}
	})

	b.Run("benchmark bin search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = nodeLookupLEBinary(bnode, keyOf(uint16(i%int(keyCount))))
		}
	})
}
