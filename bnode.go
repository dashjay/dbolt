package dbolt

import (
	"bytes"
	"encoding/binary"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// assert BTREE_PAGE_SIZE bigger than page with a very big key and value
const _ uint = BTREE_PAGE_SIZE - (HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE)

/*
BNode format
| type | nkeys |  pointers  |   offsets  | key-values | unused |
|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

for key-values pair
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
*/
type BNode []byte

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

var _binaryAlgorithm = binary.LittleEndian

func (node BNode) bType() uint16 {
	return _binaryAlgorithm.Uint16(node[0:2])
}
func (node BNode) nKeys() uint16 {
	return _binaryAlgorithm.Uint16(node[2:4])
}

// setHeader determines the type of the Node and the number of keys, and the value should cannot be changed
func (node BNode) setHeader(bType uint16, nKeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], bType)
	binary.LittleEndian.PutUint16(node[2:4], nKeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	Assertf(idx <= node.nKeys(), "assert failed: get ptr %d out of key nums %d", idx, node.nKeys())
	pos := HEADER + 8*idx
	return _binaryAlgorithm.Uint64(node[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	_binaryAlgorithm.PutUint64(node[HEADER+8*idx:], val)
}

func (node BNode) offsetPos(idx uint16) uint16 {
	Assertf(1 <= idx && idx <= node.nKeys(), "assert failed: offsetPos idx %d out of key nums %d", idx, node.nKeys())

	return HEADER + // header = 4 bytes
		8*node.nKeys() + // pointer is uint64 * key
		2*(idx-1) //
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	offset := _binaryAlgorithm.Uint16(node[node.offsetPos(idx):])
	Assertf(offset != 0, "assert failed: offset for idx %d not be set", idx)
	return offset
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	if idx == 0 {
		return
	}
	Assert(offset != 0, "assert failed: offset can not to be set to zero")
	_binaryAlgorithm.PutUint16(node[node.offsetPos(idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	Assertf(idx <= node.nKeys(), "assert failed: idx %d out of key nums %d", idx, node.nKeys())
	return HEADER +
		8*node.nKeys() +
		2*node.nKeys() +
		node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	Assertf(idx < node.nKeys(), "assert failed: idx %d out of key nums %d", idx, node.nKeys())
	pos := node.kvPos(idx)
	kLen := _binaryAlgorithm.Uint16(node[pos:])
	return node[pos+4:][:kLen]
}

func (node BNode) getVal(idx uint16) []byte {
	Assertf(idx < node.nKeys(), "assert failed: idx %d out of key nums %d", idx, node.nKeys())
	pos := node.kvPos(idx)
	kLen := _binaryAlgorithm.Uint16(node[pos:])
	valLen := _binaryAlgorithm.Uint16(node[pos+2:])
	return node[pos+4:][kLen : kLen+valLen]
}

// nBytes returns the used size of node
func (node BNode) nBytes() uint16 {
	return node.kvPos(node.nKeys())
}

// nodeLookupLE returns the first kid node whose range intersects the key. (kid[i] <= key)
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nKeys()
	found := uint16(0)

	// WARNING the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
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

func nodeLookupLEBinary(node BNode, key []byte) uint16 {
	nkeys := node.nKeys()
	found := binSearch(int(nkeys-1), func(i int) bool {
		cmp := bytes.Compare(node.getKey(uint16(i)), key)
		if cmp < 0 {
			return false
		} else {
			return true
		}
	})
	return uint16(found)
}

// leafInsert add a new key to a leaf node
func leafInsert(
	newNode BNode, oldNode BNode, idx uint16,
	key []byte, val []byte,
) {
	newNode.setHeader(BNODE_LEAF, oldNode.nKeys()+1) // setup the header

	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nKeys()-idx)
}

// leafUpdate update an exists key to a leaf node
func leafUpdate(
	newNode BNode, oldNode BNode, idx uint16,
	key []byte, val []byte,
) {

	// newNode has same size as oldNode
	newNode.setHeader(BNODE_LEAF, oldNode.nKeys()) // setup the header

	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nKeys()-idx-1)
}

// leafDelete remove a key from a leaf node
func leafDelete(newNode BNode, oldNode BNode, idx uint16) {
	newNode.setHeader(BNODE_LEAF, oldNode.nKeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                         // [0, idx)
	nodeAppendRange(newNode, oldNode, idx, idx+1, oldNode.nKeys()-idx-1) // [idx+1, n-idx-1)
}

// nodeAppendKV copy a KV into the position
func nodeAppendKV(newNode BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	keySize := len(key)
	valueSize := len(val)

	Assertf(keySize <= BTREE_MAX_KEY_SIZE, "assert failed: oversize key, len: %d", keySize)
	Assertf(valueSize <= BTREE_MAX_VAL_SIZE, "assert failed: oversize value, len: %d", keySize)
	// ptrs
	newNode.setPtr(idx, ptr)
	// KVs
	pos := newNode.kvPos(idx)
	binary.LittleEndian.PutUint16(newNode[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newNode[pos+2:], uint16(len(val)))

	copied := copy(newNode[pos+4:], key)
	Assertf(copied == keySize, "assert failed: copied size %d mismatched the key len: %d", copied, keySize)
	copied = copy(newNode[pos+4+uint16(len(key)):], val)
	Assertf(copied == valueSize, "assert failed: copied size %d mismatched the value len: %d", copied, valueSize)

	// WARNING set offset for next key
	newNode.setOffset(idx+1, newNode.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// nodeAppendRange copy multiple KVs into the position from the old node
func nodeAppendRange(
	newNode BNode, oldNode BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	for i := uint16(0); i < n; i++ {
		nodeAppendKV(newNode, dstNew+i, oldNode.getPtr(srcOld+i), oldNode.getKey(srcOld+i), oldNode.getVal(srcOld+i))
	}
}

// nodeAppendRange2 UNSAFE I've always been confused that
// node generated by this function is somewhat different from the original node.
// But I didn't dig into it.
func nodeAppendRange2(newNode BNode, oldNode BNode,
	dstNew uint16, srcOld uint16, n uint16) {
	dataStart := oldNode.kvPos(srcOld)
	dataEnd := oldNode.kvPos(srcOld + n)

	offsetStart := newNode.getOffset(dstNew)
	oldOffset := oldNode.getOffset(srcOld)

	for i := uint16(0); i < n; i++ {
		// this is only kv should be copied but not ptr
		//newNode.setPtr(dstNew+i, oldNode.getPtr(srcOld+n))

		newNode.setOffset(dstNew+i, offsetStart+(oldNode.getOffset(srcOld+i)-oldOffset))
	}

	// place the items
	copy(newNode[newNode.kvPos(dstNew):], oldNode[dataStart:dataEnd])
}

// split an oversize node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	Assert(old.nKeys() > 2, "assert failed: try to split a node with less 2 keys")
	nLeft := old.nKeys() / 2

	leftBytes := func() uint16 {
		return HEADER +
			8*nLeft +
			2*nLeft +
			old.getOffset(nLeft)
	}
	for leftBytes() > BTREE_PAGE_SIZE {
		nLeft--
	}
	Assert(nLeft >= 1, "assert failed: new node can not env store only one key")

	rightBytes := func() uint16 {
		return old.nBytes() - leftBytes() + HEADER
	}

	for rightBytes() > BTREE_PAGE_SIZE {
		nLeft++
	}

	Assert(nLeft < old.nKeys(), "assert failed: split node failed")
	nRight := old.nKeys() - nLeft

	left.setHeader(old.bType(), nLeft)
	right.setHeader(old.bType(), nRight)

	nodeAppendRange(left, old, 0, 0, nLeft)
	nodeAppendRange(right, old, 0, nLeft, nRight)

	// the left half may be still too big
	Assertf(right.nBytes() <= BTREE_PAGE_SIZE, "assert failed: node size too big after split: %d", right.nBytes())
}

// nodeSplit3 split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nBytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	Assertf(leftleft.nBytes() <= BTREE_PAGE_SIZE,
		"assert failed: leftleft.size %d should less thant page size %d", leftleft.nBytes(), BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}
