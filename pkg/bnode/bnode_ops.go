package bnode

import (
	"bytes"

	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
)

// NodeLookupLE returns the first kid node whose range intersects the key. (kid[i] <= key)
func NodeLookupLE(node Node, key []byte) uint16 {
	nKeys := node.KeyCounts()
	found := uint16(0)

	// WARNING the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	for i := uint16(1); i < nKeys; i++ {
		cmp := bytes.Compare(node.GetKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// binSearch copied from sort.Search but update to `	i, j := 1, n`
// due to 'the first key is a copy from the parent node'
func binSearch(n int, f func(int) bool) int {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if !f(h) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func NodeLookupLEBinary(node Node, key []byte) uint16 {
	nKeys := node.KeyCounts()
	found := binSearch(int(nKeys-1), func(i int) bool {
		cmp := bytes.Compare(node.GetKey(uint16(i)), key)
		if cmp < 0 {
			return false
		} else {
			return true
		}
	})
	return uint16(found)
}

// LeafInsert add a new key to a leaf node
// NOTICE: LeafInsert do only one insertion op on read-only oldNode
// and generated buffer after inserting key to newNode
func LeafInsert(
	newNode Node, oldNode Node, idx uint16,
	key []byte, val []byte,
) {
	newNode.SetHeader(NodeTypeLeaf, oldNode.KeyCounts()+1) // setup the header

	// copy all keys before the place to insert
	NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)
	// insert the specified key
	NodeAppendKVOrPtr(newNode, idx, 0, key, val)
	// copy left keys
	NodeAppendKVOrPtrRange(newNode, oldNode, idx+1, idx, oldNode.KeyCounts()-idx)
}

// LeafUpdate update an exists key to a leaf node
func LeafUpdate(
	newNode Node, oldNode Node, idx uint16,
	key []byte, val []byte,
) {
	// newNode has same size as oldNode
	newNode.SetHeader(NodeTypeLeaf, oldNode.KeyCounts()) // setup the header

	NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)
	NodeAppendKVOrPtr(newNode, idx, 0, key, val)
	NodeAppendKVOrPtrRange(newNode, oldNode, idx+1, idx+1, oldNode.KeyCounts()-idx-1)
}

// LeafDelete remove a key from a leaf node
func LeafDelete(newNode Node, oldNode Node, idx uint16) {
	newNode.SetHeader(NodeTypeLeaf, oldNode.KeyCounts()-1)
	NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)                             // [0, idx)
	NodeAppendKVOrPtrRange(newNode, oldNode, idx, idx+1, oldNode.KeyCounts()-idx-1) // [idx+1, n-idx-1)
}

// NodeAppendKVOrPtr set key-value pair or ptr(to other nodes) into the target node
// if node type is leaf, key-value pair will be inserted into this node, will be omitted (ptr means nothing for leaf node)
// if node type is node, ptr will be set to ptr list, key will be used as the first key of the node which ptr pointer to
// (value means nothing for node type node)
func NodeAppendKVOrPtr(newNode Node, idx uint16, ptr uint64, key []byte, val []byte) {
	keySize := uint16(len(key))
	valueSize := uint16(len(val))

	utils.Assertf(keySize <= constants.BtreeMaxKeySize, "assertion failed: oversize key, len: %d", keySize)
	utils.Assertf(valueSize <= constants.BtreeMaxValSize, "assertion failed: oversize value, len: %d", keySize)
	// ptrs
	newNode.setPtr(idx, ptr)
	// KVs
	pos := newNode._kvPos(idx)

	// | 2 bytes(key size) | 2 bytes(value size) | key | value |
	// ^ off set of last entry
	constants.BinaryAlgorithm.PutUint16(newNode[pos:pos+constants.Uint16Size], keySize)
	constants.BinaryAlgorithm.PutUint16(newNode[pos+constants.Uint16Size:], valueSize)

	nodeLeftSize := uint16(len(newNode[pos:]))
	utils.Assertf(nodeLeftSize >= keySize+valueSize+2*constants.Uint16Size,
		"assertion failed: nodeLeftSize(%d) have no space for next key-value pair which needs %d bytes", nodeLeftSize, keySize+valueSize+constants.Uint16Size*2)

	//nolint:gomnd // 2 uint16 size is the key and value size.
	keyOffset := pos + 2*constants.Uint16Size
	copied := copy(newNode[keyOffset:], key)
	utils.Assertf(uint16(copied) == keySize, "assertion failed: copied size %d mismatched the key len: %d", copied, keySize)
	valueOffset := keyOffset + uint16(copied)
	copied = copy(newNode[valueOffset:], val)
	utils.Assertf(uint16(copied) == valueSize, "assertion failed: copied size %d mismatched the value len: %d", copied, valueSize)

	// WARNING set offset for next key
	offsetForNextKey := newNode.getOffset(idx) + 2*constants.Uint16Size + keySize + valueSize
	newNode.setOffset(idx+1, offsetForNextKey)
}

// NodeAppendKVOrPtrRange copy multiple KVs into the position from the old node
func NodeAppendKVOrPtrRange(
	newNode Node, oldNode Node,
	dstNew uint16, srcOld uint16, n uint16,
) {
	for i := uint16(0); i < n; i++ {
		NodeAppendKVOrPtr(newNode, dstNew+i, oldNode.GetPtr(srcOld+i), oldNode.GetKey(srcOld+i), oldNode.GetVal(srcOld+i))
	}
}

// nodeAppendRange2 UNSAFE I've always been confused that
// the node generated by this function is somewhat different from the original node.
// But I didn't dig into it.
//
//nolint:unused // TODO: fix for higher performance
func nodeAppendRange2(newNode Node, oldNode Node,
	dstNew uint16, srcOld uint16, n uint16) {
	dataStart := oldNode._kvPos(srcOld)
	dataEnd := oldNode._kvPos(srcOld + n)

	offsetStart := newNode.getOffset(dstNew)
	oldOffset := oldNode.getOffset(srcOld)

	for i := uint16(0); i < n; i++ {
		// this is only kv should be copied but not ptr
		// newNode.setPtr(dstNew+i, oldNode.GetPtr(srcOld+n))
		newNode.setOffset(dstNew+i, offsetStart+(oldNode.getOffset(srcOld+i)-oldOffset))
	}

	// place the items
	copy(newNode[newNode._kvPos(dstNew):], oldNode[dataStart:dataEnd])
}

// split an oversize node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left Node, right Node, old Node) {
	//nolint:gomnd // split one node into 2 nodes, node have least 2 keys
	utils.Assertf(old.KeyCounts() >= 2, "assertion failed: try to split a node with only %d key", old.KeyCounts())
	//nolint:gomnd // split one node into 2 nodes, let's say we cut it down the middle
	nLeft := old.KeyCounts() / 2

	leftBytes := func() uint16 {
		return constants.BNodeHeader +
			8*nLeft +
			2*nLeft +
			old.getOffset(nLeft)
	}
	for leftBytes() > constants.BtreePageSize {
		nLeft--
	}
	utils.Assert(nLeft >= 1, "assertion failed: new node can not env store only one key")

	rightBytes := func() uint16 {
		return old.SizeBytes() - leftBytes() + constants.BNodeHeader
	}

	for rightBytes() > constants.BtreePageSize {
		nLeft++
	}

	utils.Assert(nLeft < old.KeyCounts(), "assertion failed: split node failed")
	nRight := old.KeyCounts() - nLeft

	left.SetHeader(old.Type(), nLeft)
	right.SetHeader(old.Type(), nRight)

	NodeAppendKVOrPtrRange(left, old, 0, 0, nLeft)
	NodeAppendKVOrPtrRange(right, old, 0, nLeft, nRight)

	// the left half may be still too big
	utils.Assertf(right.SizeBytes() <= constants.BtreePageSize, "assertion failed: node size too big after split: %d", right.SizeBytes())
}

// NodeSplit3 split a node if it's too big. the results are 1~3 nodes.
func NodeSplit3(old Node) []Node {
	if old.SizeBytes() <= constants.BtreePageSize {
		old = old[:constants.BtreePageSize]
		return []Node{old} // not split
	}
	left := Node(utils.GetPage(constants.BtreePageSizeFor2)) // might be split later
	right := Node(utils.GetPage(constants.BtreePageSize))
	nodeSplit2(left, right, old)
	if left.SizeBytes() <= constants.BtreePageSize {
		left = left[:constants.BtreePageSize]
		return []Node{left, right} // 2 nodes
	}

	leftleft := Node(utils.GetPage(constants.BtreePageSize))
	middle := Node(utils.GetPage(constants.BtreePageSize))
	nodeSplit2(leftleft, middle, left)
	utils.Assertf(leftleft.SizeBytes() <= constants.BtreePageSize,
		"assertion failed: leftleft.size %d should less thant page size %d", leftleft.SizeBytes(), constants.BtreePageSize)
	utils.PutPage(left)
	return []Node{leftleft, middle, right} // 3 nodes
}
