package dbolt

import (
	"bytes"
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	getNode func(uint64) []byte // dereference a pointer
	newNode func([]byte) uint64 // allocate a new page
	delNode func(uint64)        // deallocate a page
}

// nodeReplaceKidN replace a link with one or multiple links
func (this *BTree) nodeReplaceKidN(newNode BNode, oldNode BNode, idx uint16, kids ...BNode,
) {
	inc := uint16(len(kids))
	newNode.setHeader(BNODE_NODE, oldNode.nKeys()+inc-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(newNode, idx+uint16(i), this.newNode(node), node.getKey(0), nil)
		//                     ^position      ^pointer            ^key                      ^val
	}
	nodeAppendRange(newNode, oldNode, idx+inc, idx+1, oldNode.nKeys()-(idx+1))
}

// nodeReplace2Kid is used for deleting
func nodeReplace2Kid(
	newNode BNode, oldNode BNode, idx uint16, ptr uint64, key []byte,
) {
	newNode.setHeader(BNODE_NODE, oldNode.nKeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, ptr, key, nil)
	nodeAppendRange(newNode, oldNode, idx+1, idx+2, oldNode.nKeys()-(idx+2))
}

// nodeInsert is part of the treeInsert: KV insertion to an internal node
func (this *BTree) nodeInsert(newNode BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	// get the ptr by idx
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	knode := this.treeInsert(this.getNode(kptr), key, val)
	// split the result
	nsplit, split := nodeSplit3(knode)
	// deallocate the kid node
	this.delNode(kptr)
	// update the kid links
	this.nodeReplaceKidN(newNode, node, idx, split[:nsplit]...)
}

// treeInsert insert a KV into a node.
// BNode return by this function may be oversize.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func (this *BTree) treeInsert(node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	newNode := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// where to insert the key?
	idx := nodeLookupLE(node, key)

	// act depending on the node type
	switch node.bType() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(newNode, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		this.nodeInsert(newNode, node, idx, key, val)
	default:
		Assertf(false, "assertion failed: unknown nodeType %d", node.bType())
	}
	return newNode
}

func (this *BTree) Insert(key []byte, val []byte) {
	if this.root == 0 {
		// create the first node
		rootBNode := BNode(make([]byte, BTREE_PAGE_SIZE))
		rootBNode.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes this tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(rootBNode, 0, 0, nil, nil)
		nodeAppendKV(rootBNode, 1, 0, key, val)
		this.root = this.newNode(rootBNode)
		return
	}

	node := this.treeInsert(this.getNode(this.root), key, val)
	nSplit, split := nodeSplit3(node)
	this.delNode(this.root)
	if nSplit > 1 {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nSplit)
		for i, knode := range split[:nSplit] {
			ptr, k := this.newNode(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, k, nil)
		}
		this.root = this.newNode(root)
	} else {
		this.root = this.newNode(split[0])
	}
}

// Delete deletes a key and returns whether the key was there
func (this *BTree) Delete(key []byte) bool {
	if this.root == 0 {
		return false
	}
	newNode := this.treeDelete(this.getNode(this.root), key)
	if len(newNode) == 0 {
		return false
	}
	this.root = this.newNode(newNode)
	return true
}

// shouldMerge give indict whether the updated kid should be merged with a sibling?
func (this *BTree) shouldMerge(node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	// merge with left
	if idx > 0 {
		sibling := BNode(this.getNode(node.getPtr(idx - 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	// merge with right
	if idx+1 < node.nKeys() {
		sibling := BNode(this.getNode(node.getPtr(idx + 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}

	return 0, BNode{}
}

func nodeMerge(newNode BNode, leftNode BNode, rightNode BNode) {
	mergedSize := leftNode.nBytes() + rightNode.nBytes() - HEADER
	Assertf(mergedSize < BTREE_PAGE_SIZE, "assertion failed: node merged size is too large: %d", mergedSize)

	newNode.setHeader(leftNode.bType(), leftNode.nKeys()+rightNode.nKeys())
	nodeAppendRange(newNode, leftNode, 0, 0, leftNode.nKeys())
	nodeAppendRange(newNode, rightNode, leftNode.nKeys(), 0, rightNode.nKeys())
}

// treeDelete delete a KV from a node.
// the caller is responsible for deallocating the input node
// and merge the result node with sibling
func (this *BTree) treeDelete(node BNode, key []byte) BNode {
	// the result node.
	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))

	// which key should be deleted
	idx := nodeLookupLE(node, key)

	// act depending on the node type
	switch node.bType() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) == key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, delete it.
			leafDelete(newNode, node, idx)
		} else {
			return nil // indicate not found
		}
	case BNODE_NODE:
		return this.nodeDelete(node, idx, key)
		// internal node, insert it to a kid node.
		//this.nodeInsert(newNode, node, idx, key, val)
	default:
		Assertf(false, "assertion failed: unknown nodeType %d", node.bType())
	}
	return newNode
}

// delete a key from an internal node; part of the treeDelete()
func (this *BTree) nodeDelete(node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := this.treeDelete(this.getNode(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	this.delNode(kptr)

	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := this.shouldMerge(node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		this.delNode(node.getPtr(idx - 1))
		nodeReplace2Kid(newNode, node, idx-1, this.newNode(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		this.delNode(node.getPtr(idx + 1))
		nodeReplace2Kid(newNode, node, idx, this.newNode(merged), merged.getKey(0))
	case updated.nKeys() == 0:
		Assertf(node.nKeys() == 1 && idx == 0, "") // 1 empty child but no sibling
		newNode.setHeader(BNODE_NODE, 0)           // the parent becomes empty too
	case updated.nKeys() > 0: // no merge
		this.nodeReplaceKidN(newNode, node, idx, updated)
	default:
		Assertf(false, "should not happend.(merged: %d, updated.nKeys(): %d)", mergeDir, updated.nKeys())
	}
	return newNode
}
