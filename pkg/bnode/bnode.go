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

type NodeType uint16

const (
	NodeTypeNode NodeType = 1 // internal nodes without values
	NodeTypeLeaf NodeType = 2 // leaf nodes with values
)

func (node Node) Type() NodeType {
	return NodeType(constants.BinaryAlgorithm.Uint16(node[0:2]))
}
func (node Node) KeyCounts() uint16 {
	return constants.BinaryAlgorithm.Uint16(node[2:4])
}

// SetHeader determines the type of the Node and the number of keys, and the value should cannot be changed
func (node Node) SetHeader(bType NodeType, nKeys uint16) {
	constants.BinaryAlgorithm.PutUint16(node[0:2], uint16(bType))
	constants.BinaryAlgorithm.PutUint16(node[2:4], nKeys)
}

func (node Node) _ptrPos(idx uint16) uint16 {
	return constants.BNodeHeader + 8*idx
}

// pointers
func (node Node) GetPtr(idx uint16) uint64 {
	utils.Assertf(idx <= node.KeyCounts(), "assertion failed: get ptr %d out of key nums %d", idx, node.KeyCounts())
	return constants.BinaryAlgorithm.Uint64(node[node._ptrPos(idx):])
}
func (node Node) setPtr(idx uint16, val uint64) {
	utils.Assertf(idx <= node.KeyCounts(), "assertion failed: set ptr %d out of key nums %d", idx, node.KeyCounts())
	constants.BinaryAlgorithm.PutUint64(node[node._ptrPos(idx):], val)
}

// _offsetPos return where the offset of idx placed
func (node Node) _offsetPos(idx uint16) uint16 {
	utils.Assertf(1 <= idx && idx <= node.KeyCounts(), "assertion failed: _offsetPos idx %d out of key nums %d", idx, node.KeyCounts())

	return constants.BNodeHeader + // header = 4 bytes
		8*node.KeyCounts() + // pointer is uint64 * key
		2*(idx-1) //
}

func (node Node) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	offset := constants.BinaryAlgorithm.Uint16(node[node._offsetPos(idx):])
	utils.Assertf(offset != 0, "assertion failed: offset for idx %d not be set", idx)
	return offset
}

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

func (node Node) GetKey(idx uint16) []byte {
	pos := node._kvPos(idx)
	kLen := constants.BinaryAlgorithm.Uint16(node[pos:])
	return node[pos+4:][:kLen]
}

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
