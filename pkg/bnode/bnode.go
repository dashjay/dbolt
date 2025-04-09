package bnode

import (
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
)

/*
Node format
| type | nkeys |  pointers  |   offsets  | key-values | unused |
|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

for key-values pair
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
*/
type Node []byte

// NodeType mark the type of the node in first 2 bytes of node.
type NodeType uint16

const (
	NodeTypeNode NodeType = 1 // internal nodes without values
	NodeTypeLeaf NodeType = 2 // leaf nodes with values
)

// Type return the type of the node.
func (node Node) Type() NodeType {
	return NodeType(constants.BinaryAlgorithm.Uint16(node[0:2]))
}

// KeyCounts return the entry num on this node.
func (node Node) KeyCounts() uint16 {
	return constants.BinaryAlgorithm.Uint16(node[2:4])
}

// SetHeader assign the type of the Node and the number of entries, BTW the value SHOULD NOT be changed after create.
func (node Node) SetHeader(bType NodeType, nKeys uint16) {
	constants.BinaryAlgorithm.PutUint16(node[0:2], uint16(bType))
	constants.BinaryAlgorithm.PutUint16(node[2:4], nKeys)
}

// _ptrPos return the offset on node for the idx pointer
func (node Node) _ptrPos(idx uint16) uint16 {
	return constants.BNodeHeader + 8*idx
}

// GetPtr return the pointer node id of the idx pointer
func (node Node) GetPtr(idx uint16) uint64 {
	utils.Assertf(idx <= node.KeyCounts(), "assertion failed: get ptr %d out of key nums %d", idx, node.KeyCounts())
	return constants.BinaryAlgorithm.Uint64(node[node._ptrPos(idx):])
}

// setPtr set the idx pointer(ndoe id) on node.
func (node Node) setPtr(idx uint16, val uint64) {
	utils.Assertf(idx <= node.KeyCounts(), "assertion failed: set ptr %d out of key nums %d", idx, node.KeyCounts())
	constants.BinaryAlgorithm.PutUint64(node[node._ptrPos(idx):], val)
}

// _offsetPos return offset on node to store the key-value offset of the idx
func (node Node) _offsetPos(idx uint16) uint16 {
	utils.Assertf(1 <= idx && idx <= node.KeyCounts(), "assertion failed: _offsetPos idx %d out of key nums %d", idx, node.KeyCounts())

	return constants.BNodeHeader + // header = 4 bytes
		8*node.KeyCounts() + // pointer is uint64 * key
		2*(idx-1) //
}

// getOffset return the offset of the key-value on node.
func (node Node) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	offset := constants.BinaryAlgorithm.Uint16(node[node._offsetPos(idx):])
	utils.Assertf(offset != 0, "assertion failed: offset for idx %d not be set", idx)
	return offset
}

// setOffset set the offset of the key-value on node
// wse often set the offset of the n+1th key-value after writing the nth key-value pair.
func (node Node) setOffset(idx uint16, offset uint16) {
	if idx == 0 {
		return
	}
	utils.Assert(offset != 0, "assertion failed: offset can not to be set to zero")
	constants.BinaryAlgorithm.PutUint16(node[node._offsetPos(idx):], offset)
}

// _kvPos is the offset of key-value pair from the first key.
func (node Node) _kvPos(idx uint16) uint16 {
	utils.Assertf(idx <= node.KeyCounts(), "assertion failed: idx %d out of key nums %d", idx, node.KeyCounts())
	return constants.BNodeHeader +
		8*node.KeyCounts() +
		2*node.KeyCounts() +
		node.getOffset(idx)
}

// GetKey return the idx_th key on this node.
func (node Node) GetKey(idx uint16) []byte {
	pos := node._kvPos(idx)
	kLen := constants.BinaryAlgorithm.Uint16(node[pos:])
	return node[pos+4:][:kLen]
}

// GetVal return the idx_th value on this node.
func (node Node) GetVal(idx uint16) []byte {
	pos := node._kvPos(idx)
	kLen := constants.BinaryAlgorithm.Uint16(node[pos:])
	valLen := constants.BinaryAlgorithm.Uint16(node[pos+2:])
	return node[pos+4:][kLen : kLen+valLen]
}

// SizeBytes returns the used size of node
func (node Node) SizeBytes() uint16 {
	return node._kvPos(node.KeyCounts())
}
