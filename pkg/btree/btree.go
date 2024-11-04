package btree

import (
	"bytes"

	"github.com/dashjay/dbolt/pkg/bnode"
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
)

type Tree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	getNode func(uint64) []byte // dereference a pointer
	newNode func([]byte) uint64 // allocate a new page
	delNode func(uint64)        // deallocate a page
}

func NewTree(getNodeCB func(uint64) []byte, newNodeCB func([]byte) uint64, delNodeCB func(uint64)) Tree {
	return Tree{
		root:    0,
		getNode: getNodeCB,
		newNode: newNodeCB,
		delNode: delNodeCB,
	}
}

func (this *Tree) DebugGetNode(ptr uint64) []byte {
	return this.getNode(ptr)
}

func (this *Tree) Root() uint64 {
	return this.root
}

func (this *Tree) SetRoot(in uint64) {
	this.root = in
}

func (this *Tree) Insert(key []byte, val []byte) {
	if this.root == 0 {
		// create the first node (leaf type)
		rootBNode := bnode.Node(utils.GetPage())
		rootBNode.SetHeader(bnode.NodeTypeLeaf, 2)
		// a dummy key, this makes this tree cover the whole key space.
		// thus a lookup can always find a containing node.
		bnode.NodeAppendKVOrPtr(rootBNode, 0, 0, nil, nil)
		bnode.NodeAppendKVOrPtr(rootBNode, 1, 0, key, val)
		this.root = this.newNode(rootBNode)
		return
	}

	// node type of root will be always leaf type until it's size is more than one page (nSplit > 1)
	node := this._treeInsert(this.getNode(this.root), key, val)
	nSplit, split := bnode.NodeSplit3(node)
	this.delNode(this.root)
	if nSplit > 1 {
		// the root was split, add a new level.
		root := bnode.Node(utils.GetPage())
		// the root will become a node type
		// after this, size of bnode will become the size of sub node instead of key nums
		root.SetHeader(bnode.NodeTypeNode, nSplit)
		for i, subNode := range split[:nSplit] {
			bnode.NodeAppendKVOrPtr(root, uint16(i),
				this.newNode(subNode), subNode.GetKey(0), nil)
		}
		this.root = this.newNode(root)
	} else {
		this.root = this.newNode(split[0])
	}
}

// Get value by key
func (this *Tree) Get(key []byte) ([]byte, bool) {
	if this.root == 0 {
		return nil, false
	}
	return this._treeGet(this.getNode(this.root), key)
}

// Delete deletes a key and returns whether the key was there
func (this *Tree) Delete(key []byte) bool {
	if this.root == 0 {
		return false
	}
	newNode := this._treeDelete(this.getNode(this.root), key)
	if len(newNode) == 0 {
		return false
	}
	this.root = this.newNode(newNode)
	return true
}

// nodeReplaceKidN replace a link with one or multiple links
func (this *Tree) nodeReplaceKidN(newNode bnode.Node, oldNode bnode.Node, idx uint16, kids ...bnode.Node,
) {
	inc := uint16(len(kids))
	newNode.SetHeader(bnode.NodeTypeNode, oldNode.KeyCounts()+inc-1)
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)
	for i, node := range kids {
		bnode.NodeAppendKVOrPtr(newNode, idx+uint16(i), this.newNode(node), node.GetKey(0), nil)
		//                     ^position      ^pointer            ^key                      ^val
	}
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, idx+inc, idx+1, oldNode.KeyCounts()-(idx+1))
}

// nodeReplace2Kid is used in deleting nodes.
func (this *Tree) nodeReplace2Kid(
	newNode bnode.Node, oldNode bnode.Node, idx uint16, ptr uint64, key []byte,
) {
	newNode.SetHeader(bnode.NodeTypeNode, oldNode.KeyCounts()-1)
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)
	bnode.NodeAppendKVOrPtr(newNode, idx, ptr, key, nil)
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, idx+1, idx+2, oldNode.KeyCounts()-(idx+2))
}

// _nodeInsert is part of the _treeInsert: KV insertion to an internal node
func (this *Tree) _nodeInsert(newNode bnode.Node, node bnode.Node, idx uint16,
	key []byte, val []byte,
) {
	// get the idx by idx
	kptr := node.GetPtr(idx)
	// recursive insertion to the kid node
	knode := this._treeInsert(this.getNode(kptr), key, val)
	// split the result
	nsplit, split := bnode.NodeSplit3(knode)
	// deallocate the kid node
	this.delNode(kptr)
	// update the kid links
	this.nodeReplaceKidN(newNode, node, idx, split[:nsplit]...)
}

// _treeInsert insert a KV into a node.
// BNode return by this function may be oversize.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func (this *Tree) _treeInsert(node bnode.Node, key []byte, val []byte) bnode.Node {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	newNode := bnode.Node(make([]byte, 2*constants.BTREE_PAGE_SIZE))

	// where to insert the key?
	idx := bnode.NodeLookupLE(node, key)

	// act depending on the node type
	switch node.Type() {
	case bnode.NodeTypeLeaf:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.GetKey(idx)) {
			// found the key, update it.
			bnode.LeafUpdate(newNode, node, idx, key, val)
		} else {
			// insert it after the position.
			bnode.LeafInsert(newNode, node, idx+1, key, val)
		}
	case bnode.NodeTypeNode:
		// internal node, insert it to a kid node.
		this._nodeInsert(newNode, node, idx, key, val)
	default:
		utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
	}
	return newNode
}

// shouldMerge give indict whether the updated kid should be merged with a sibling?
func (this *Tree) shouldMerge(node bnode.Node, idx uint16, updated bnode.Node) (int, bnode.Node) {
	if updated.SizeBytes() > constants.BTREE_PAGE_SIZE/4 {
		return 0, bnode.Node{}
	}

	// merge with left
	if idx > 0 {
		sibling := bnode.Node(this.getNode(node.GetPtr(idx - 1)))
		merged := sibling.SizeBytes() + updated.SizeBytes() - constants.BNODE_HEADER
		if merged <= constants.BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	// merge with right
	if idx+1 < node.KeyCounts() {
		sibling := bnode.Node(this.getNode(node.GetPtr(idx + 1)))
		merged := sibling.SizeBytes() + updated.SizeBytes() - constants.BNODE_HEADER
		if merged <= constants.BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}

	return 0, bnode.Node{}
}

func nodeMerge(newNode bnode.Node, leftNode bnode.Node, rightNode bnode.Node) {
	mergedSize := leftNode.SizeBytes() + rightNode.SizeBytes() - constants.BNODE_HEADER
	utils.Assertf(mergedSize < constants.BTREE_PAGE_SIZE, "assertion failed: node merged size is too large: %d", mergedSize)

	newNode.SetHeader(leftNode.Type(), leftNode.KeyCounts()+rightNode.KeyCounts())
	bnode.NodeAppendKVOrPtrRange(newNode, leftNode, 0, 0, leftNode.KeyCounts())
	bnode.NodeAppendKVOrPtrRange(newNode, rightNode, leftNode.KeyCounts(), 0, rightNode.KeyCounts())
}

// _treeGet get specified key-value from the node
// value will be copied
func (this *Tree) _treeGet(node bnode.Node, key []byte) ([]byte, bool) {
	idx := bnode.NodeLookupLEBinary(node, key)
	switch node.Type() {
	case bnode.NodeTypeNode:
		n := bytes.Compare(node.GetKey(idx), key)
		if n <= 0 {
			return this._treeGet(this.getNode(node.GetPtr(idx)), key)
		}
		return this._treeGet(this.getNode(node.GetPtr(idx-1)), key)
	case bnode.NodeTypeLeaf:
		if bytes.Equal(node.GetKey(idx), key) {
			return node.GetVal(idx), true
		}
		return nil, false
	default:
		utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
	}
	return nil, false
}

// _treeDelete delete a KV from a node.
// the caller is responsible for deallocating the input node
// and merge the result node with sibling
func (this *Tree) _treeDelete(node bnode.Node, key []byte) bnode.Node {
	// the result node.
	newNode := bnode.Node(utils.GetPage())

	// which key should be deleted
	idx := bnode.NodeLookupLE(node, key)

	// act depending on the node type
	switch node.Type() {
	case bnode.NodeTypeNode:
		return this._nodeDelete(node, idx, key)
		// delete key from a node
	case bnode.NodeTypeLeaf:
		// leaf, node.getKey(idx) == key
		if bytes.Equal(key, node.GetKey(idx)) {
			// found the key, delete it.
			bnode.LeafDelete(newNode, node, idx)
		} else {
			return nil // indicate not found
		}
	default:
		utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
	}
	return newNode
}

// delete a key from an internal node; part of the _treeDelete()
func (this *Tree) _nodeDelete(node bnode.Node, idx uint16, key []byte) bnode.Node {
	// recurse into the kid
	kptr := node.GetPtr(idx)
	updated := this._treeDelete(this.getNode(kptr), key)
	if len(updated) == 0 {
		return bnode.Node{} // not found
	}
	this.delNode(kptr)

	newNode := bnode.Node(utils.GetPage())
	// assertCheckFreelist for merging
	mergeDir, sibling := this.shouldMerge(node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := bnode.Node(utils.GetPage())
		nodeMerge(merged, sibling, updated)
		this.delNode(node.GetPtr(idx - 1))
		this.nodeReplace2Kid(newNode, node, idx-1, this.newNode(merged), merged.GetKey(0))
	case mergeDir > 0: // right
		merged := bnode.Node(utils.GetPage())
		nodeMerge(merged, updated, sibling)
		this.delNode(node.GetPtr(idx + 1))
		this.nodeReplace2Kid(newNode, node, idx, this.newNode(merged), merged.GetKey(0))
	case updated.KeyCounts() == 0:
		utils.Assertf(node.KeyCounts() == 1 && idx == 0, "") // 1 empty child but no sibling
		newNode.SetHeader(bnode.NodeTypeNode, 0)             // the parent becomes empty too
	case updated.KeyCounts() > 0: // no merge
		this.nodeReplaceKidN(newNode, node, idx, updated)
	default:
		utils.Assertf(false, "should not happend.(merged: %d, updated.nKeys(): %d)", mergeDir, updated.KeyCounts())
	}
	return newNode
}

type nodeRef struct {
	node uint64
	idx  uint16
}

type TreeCursor struct {
	tree  *Tree
	stack *utils.Stack[nodeRef]
}

func (this *Tree) NewTreeCursor() *TreeCursor {
	return &TreeCursor{
		tree:  this,
		stack: nil,
	}
}

func (this *TreeCursor) SeekToFirst() ([]byte, []byte) {
	this.stack = &utils.Stack[nodeRef]{}
	this.stack.Push(nodeRef{node: this.tree.root, idx: 0})
	for !this.stack.Empty() {
		top := this.stack.Top()
		node := bnode.Node(this.tree.getNode(top.node))
		switch node.Type() {
		case bnode.NodeTypeLeaf:
			// 第一个 key 是一个空 key
			this.stack.TopRef().idx += 1
			return node.GetKey(1), node.GetVal(1)
		case bnode.NodeTypeNode:
			ptr := node.GetPtr(0)
			this.stack.Push(nodeRef{
				node: ptr,
				idx:  0,
			})
			continue
		default:
			utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
		}
	}
	return nil, nil
}

func (this *TreeCursor) Seek(key []byte) ([]byte, []byte) {
	this.stack = &utils.Stack[nodeRef]{}
	rootNode := bnode.Node(this.tree.getNode(this.tree.Root()))
	idx := bnode.NodeLookupLE(rootNode, key)
	this.stack.Push(nodeRef{
		node: this.tree.root,
		idx:  idx,
	})
	for !this.stack.Empty() {
		top := this.stack.Top()
		node := bnode.Node(this.tree.getNode(top.node))
		idx := bnode.NodeLookupLE(node, key)
		switch node.Type() {
		case bnode.NodeTypeLeaf:
			if bytes.Equal(node.GetKey(idx), key) {
				return node.GetKey(idx), node.GetVal(idx)
			} else {
				return nil, nil
			}
		case bnode.NodeTypeNode:
			ptr := node.GetPtr(idx)
			nextLevelIdx := bnode.NodeLookupLE(this.tree.getNode(ptr), key)
			this.stack.Push(nodeRef{node: ptr, idx: nextLevelIdx})
			continue
		}
	}
	return nil, nil
}

func (this *TreeCursor) Key() []byte {
	top := this.stack.Top()
	node := bnode.Node(this.tree.getNode(top.node))
	return node.GetKey(top.idx)
}

func (this *TreeCursor) Value() []byte {
	top := this.stack.Top()
	node := bnode.Node(this.tree.getNode(top.node))
	return node.GetVal(top.idx)
}

func (this *TreeCursor) Next() ([]byte, []byte) {
	for !this.stack.Empty() {
		top := this.stack.TopRef()
		node := bnode.Node(this.tree.getNode(top.node))
		top.idx += 1
		if top.idx >= node.KeyCounts() {
			this.stack.Pop()
			continue
		}
		if node.Type() == bnode.NodeTypeLeaf {
			return node.GetKey(top.idx), node.GetVal(top.idx)
		} else if node.Type() == bnode.NodeTypeNode {
			newNode := node.GetPtr(top.idx)
			this.stack.Push(nodeRef{node: newNode, idx: 0})
			nextNode := bnode.Node(this.tree.getNode(newNode))
			for nextNode.Type() == bnode.NodeTypeNode {
				newNode = nextNode.GetPtr(0)
				this.stack.Push(nodeRef{node: newNode, idx: 0})
				nextNode = this.tree.getNode(newNode)
			}
			utils.Assertf(nextNode.Type() == bnode.NodeTypeLeaf, "assertion failed: leafNode type %d", nextNode.Type())
			return nextNode.GetKey(0), nextNode.GetVal(0)
		} else {
			utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
		}
	}
	return nil, nil
}
