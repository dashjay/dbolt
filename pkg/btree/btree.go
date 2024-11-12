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

func (tree *Tree) DebugGetNode(ptr uint64) []byte {
	return tree.getNode(ptr)
}

func (tree *Tree) Root() uint64 {
	return tree.root
}

func (tree *Tree) SetRoot(in uint64) {
	tree.root = in
}

func (tree *Tree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node (leaf type)
		rootBNode := bnode.Node(utils.GetPage(constants.BtreePageSize))
		//nolint:gomnd // first inserted key will be inserted into the root node with a fake empty key nil, take 2 size.
		rootBNode.SetHeader(bnode.NodeTypeLeaf, 2)
		// a fake key, makes the tree cover the whole key space.
		// so that a lookup can always find a containing node.
		bnode.NodeAppendKVOrPtr(rootBNode, 0, 0, nil, nil)
		bnode.NodeAppendKVOrPtr(rootBNode, 1, 0, key, val)
		tree.root = tree.newNode(rootBNode)
		return
	}

	// node type of root will be always leaf type until it's size is more than one page (nSplit > 1)
	node := tree._treeInsert(tree.getNode(tree.root), key, val)
	splits := bnode.NodeSplit3(node)
	tree.delNode(tree.root)
	if len(splits) > 1 {
		// the root was splits, add a new level.
		root := bnode.Node(utils.GetPage(constants.BtreePageSize))
		// the root will become a node type
		// after tree, size of bnode will become the size of sub node instead of key nums
		root.SetHeader(bnode.NodeTypeNode, uint16(len(splits)))
		for i, subNode := range splits {
			bnode.NodeAppendKVOrPtr(root, uint16(i),
				tree.newNode(subNode), subNode.GetKey(0), nil)
		}
		tree.root = tree.newNode(root)
	} else {
		tree.root = tree.newNode(splits[0])
	}
}

// Get value by key
func (tree *Tree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return tree._treeGet(tree.getNode(tree.root), key)
}

// Delete deletes a key and returns whether the key was there
func (tree *Tree) Delete(key []byte) bool {
	if tree.root == 0 {
		return false
	}
	oldNode := tree.getNode(tree.root)
	newNode := tree._treeDelete(oldNode, key)
	if len(newNode) == 0 {
		return false
	}
	tree.delNode(tree.root)
	tree.root = tree.newNode(newNode)
	return true
}

// nodeReplaceKidN replace a link with one or multiple links
func (tree *Tree) nodeReplaceKidN(newNode bnode.Node, oldNode bnode.Node, idx uint16, kids ...bnode.Node,
) {
	inc := uint16(len(kids))
	newNode.SetHeader(bnode.NodeTypeNode, oldNode.KeyCounts()+inc-1)
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)
	for i, node := range kids {
		bnode.NodeAppendKVOrPtr(newNode, idx+uint16(i), tree.newNode(node), node.GetKey(0), nil)
		//                     ^position      ^pointer            ^key                      ^val
	}
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, idx+inc, idx+1, oldNode.KeyCounts()-(idx+1))
}

// nodeReplace2Kid is used in deleting nodes.
func (tree *Tree) nodeReplace2Kid(
	newNode bnode.Node, oldNode bnode.Node, idx uint16, ptr uint64, key []byte,
) {
	newNode.SetHeader(bnode.NodeTypeNode, oldNode.KeyCounts()-1)
	// copy from oldNode -> newNode from [0, idx)
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, 0, 0, idx)
	// update the idx position on new Node with the specified ptr/key(firstKey)
	bnode.NodeAppendKVOrPtr(newNode, idx, ptr, key, nil)
	//nolint:gomnd // then update the newNode's [idx+1, ... with the oldNode's [idx+2,...
	bnode.NodeAppendKVOrPtrRange(newNode, oldNode, idx+1, idx+2, oldNode.KeyCounts()-(idx+2))
}

// _nodeInsert is part of the _treeInsert: KV insertion to an internal node
func (tree *Tree) _nodeInsert(newNode bnode.Node, node bnode.Node, idx uint16,
	key []byte, val []byte,
) {
	// get the idx by idx
	kptr := node.GetPtr(idx)
	// recursive insertion to the kid node
	knode := tree._treeInsert(tree.getNode(kptr), key, val)
	// split the result
	splits := bnode.NodeSplit3(knode)
	// deallocate the kid node
	tree.delNode(kptr)
	// update the kid links
	tree.nodeReplaceKidN(newNode, node, idx, splits...)
}

// _treeInsert insert a KV into a node.
// BNode return by this function may be oversize.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func (tree *Tree) _treeInsert(node bnode.Node, key []byte, val []byte) bnode.Node {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	newNode := bnode.Node(utils.GetPage(constants.BtreePageSizeFor2))

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
		tree._nodeInsert(newNode, node, idx, key, val)
	default:
		utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
	}
	return newNode
}

// shouldMerge give indict whether the updated kid should be merged with a sibling?
func (tree *Tree) shouldMerge(node bnode.Node, idx uint16, updated bnode.Node) (int, bnode.Node) {
	if updated.SizeBytes() > constants.BtreePageSize/4 {
		return 0, bnode.Node{}
	}

	// merge with left
	if idx > 0 {
		sibling := bnode.Node(tree.getNode(node.GetPtr(idx - 1)))
		merged := sibling.SizeBytes() + updated.SizeBytes() - constants.BNodeHeader
		if merged <= constants.BtreePageSize {
			return -1, sibling // left
		}
	}

	// merge with right
	if idx+1 < node.KeyCounts() {
		sibling := bnode.Node(tree.getNode(node.GetPtr(idx + 1)))
		merged := sibling.SizeBytes() + updated.SizeBytes() - constants.BNodeHeader
		if merged <= constants.BtreePageSize {
			return +1, sibling // right
		}
	}

	return 0, bnode.Node{}
}

func nodeMerge(newNode bnode.Node, leftNode bnode.Node, rightNode bnode.Node) {
	mergedSize := leftNode.SizeBytes() + rightNode.SizeBytes() - constants.BNodeHeader
	utils.Assertf(mergedSize <= constants.BtreePageSize, "assertion failed: node merged size is too large: %d", mergedSize)

	newNode.SetHeader(leftNode.Type(), leftNode.KeyCounts()+rightNode.KeyCounts())
	bnode.NodeAppendKVOrPtrRange(newNode, leftNode, 0, 0, leftNode.KeyCounts())
	bnode.NodeAppendKVOrPtrRange(newNode, rightNode, leftNode.KeyCounts(), 0, rightNode.KeyCounts())
}

// _treeGet get specified key-value from the node
// value will be copied
func (tree *Tree) _treeGet(node bnode.Node, key []byte) ([]byte, bool) {
	idx := bnode.NodeLookupLEBinary(node, key)
	switch node.Type() {
	case bnode.NodeTypeNode:
		n := bytes.Compare(node.GetKey(idx), key)
		if n <= 0 {
			return tree._treeGet(tree.getNode(node.GetPtr(idx)), key)
		}
		return tree._treeGet(tree.getNode(node.GetPtr(idx-1)), key)
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
func (tree *Tree) _treeDelete(node bnode.Node, key []byte) bnode.Node {
	// which key should be deleted
	idx := bnode.NodeLookupLE(node, key)

	// act depending on the node type
	switch node.Type() {
	case bnode.NodeTypeNode:
		return tree._nodeDelete(node, idx, key)
		// delete key from a node
	case bnode.NodeTypeLeaf:
		newNode := bnode.Node(utils.GetPage(constants.BtreePageSize))
		// leaf, node.getKey(idx) == key
		if bytes.Equal(key, node.GetKey(idx)) {
			// found the key, delete it.
			bnode.LeafDelete(newNode, node, idx)
		} else {
			return nil // indicate not found
		}
		return newNode
	default:
		utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
	}
	return nil
}

// delete a key from an internal node; part of the _treeDelete()
func (tree *Tree) _nodeDelete(node bnode.Node, idx uint16, key []byte) bnode.Node {
	// recurse into the kid
	kptr := node.GetPtr(idx)
	updated := tree._treeDelete(tree.getNode(kptr), key)
	if len(updated) == 0 {
		return bnode.Node{} // not found
	}
	tree.delNode(kptr)

	newNode := bnode.Node(utils.GetPage(constants.BtreePageSize))
	mergeDir, sibling := tree.shouldMerge(node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := bnode.Node(utils.GetPage(constants.BtreePageSize))
		nodeMerge(merged, sibling, updated)
		tree.delNode(node.GetPtr(idx - 1))
		tree.nodeReplace2Kid(newNode, node, idx-1, tree.newNode(merged), merged.GetKey(0))
	case mergeDir > 0: // right
		merged := bnode.Node(utils.GetPage(constants.BtreePageSize))
		nodeMerge(merged, updated, sibling)
		tree.delNode(node.GetPtr(idx + 1))
		tree.nodeReplace2Kid(newNode, node, idx, tree.newNode(merged), merged.GetKey(0))
	case updated.KeyCounts() == 0:
		utils.Assertf(node.KeyCounts() == 1 && idx == 0, "") // 1 empty child but no sibling
		newNode.SetHeader(bnode.NodeTypeNode, 0)             // the parent becomes empty too
	case updated.KeyCounts() > 0: // no merge
		tree.nodeReplaceKidN(newNode, node, idx, updated)
	default:
		utils.Assertf(false, "should not happened .(merged: %d, updated.nKeys(): %d)", mergeDir, updated.KeyCounts())
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

func (tree *Tree) NewTreeCursor() *TreeCursor {
	return &TreeCursor{
		tree:  tree,
		stack: nil,
	}
}

func (c *TreeCursor) SeekToFirst() ([]byte, []byte) {
	if c.tree.Root() == 0 {
		return nil, nil
	}
	c.stack = &utils.Stack[nodeRef]{}
	c.stack.Push(nodeRef{node: c.tree.root, idx: 0})
	for !c.stack.Empty() {
		top := c.stack.Top()
		node := bnode.Node(c.tree.getNode(top.node))
		switch node.Type() {
		case bnode.NodeTypeLeaf:
			// 第一个 key 是一个空 key
			c.stack.TopRef().idx += 1
			return node.GetKey(1), node.GetVal(1)
		case bnode.NodeTypeNode:
			ptr := node.GetPtr(0)
			c.stack.Push(nodeRef{
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

func (c *TreeCursor) Seek(key []byte) ([]byte, []byte) {
	if c.tree.Root() == 0 {
		return nil, nil
	}
	c.stack = &utils.Stack[nodeRef]{}
	rootNode := bnode.Node(c.tree.getNode(c.tree.Root()))
	idx := bnode.NodeLookupLE(rootNode, key)
	c.stack.Push(nodeRef{
		node: c.tree.root,
		idx:  idx,
	})
	for !c.stack.Empty() {
		top := c.stack.Top()
		node := bnode.Node(c.tree.getNode(top.node))
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
			nextLevelIdx := bnode.NodeLookupLE(c.tree.getNode(ptr), key)
			c.stack.Push(nodeRef{node: ptr, idx: nextLevelIdx})
			continue
		}
	}
	return nil, nil
}

func (c *TreeCursor) Key() []byte {
	top := c.stack.Top()
	node := bnode.Node(c.tree.getNode(top.node))
	return node.GetKey(top.idx)
}

func (c *TreeCursor) Value() []byte {
	top := c.stack.Top()
	node := bnode.Node(c.tree.getNode(top.node))
	return node.GetVal(top.idx)
}

func (c *TreeCursor) Next() ([]byte, []byte) {
	for !c.stack.Empty() {
		top := c.stack.TopRef()
		node := bnode.Node(c.tree.getNode(top.node))
		top.idx += 1
		if top.idx >= node.KeyCounts() {
			c.stack.Pop()
			continue
		}
		if node.Type() == bnode.NodeTypeLeaf {
			return node.GetKey(top.idx), node.GetVal(top.idx)
		} else if node.Type() == bnode.NodeTypeNode {
			newNode := node.GetPtr(top.idx)
			c.stack.Push(nodeRef{node: newNode, idx: 0})
			nextNode := bnode.Node(c.tree.getNode(newNode))
			for nextNode.Type() == bnode.NodeTypeNode {
				newNode = nextNode.GetPtr(0)
				c.stack.Push(nodeRef{node: newNode, idx: 0})
				nextNode = c.tree.getNode(newNode)
			}
			utils.Assertf(nextNode.Type() == bnode.NodeTypeLeaf, "assertion failed: leafNode type %d", nextNode.Type())
			return nextNode.GetKey(0), nextNode.GetVal(0)
		} else {
			utils.Assertf(false, "assertion failed: unknown nodeType %d", node.Type())
		}
	}
	return nil, nil
}
