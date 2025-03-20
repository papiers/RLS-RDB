package types

import (
	"bytes"

	"db-practice/util"
)

type BTree struct {
	// pointer（非零页码）
	root uint64
	// 用于管理磁盘上页面的回调
	get func(uint64) BNode // 解引用指针
	new func(BNode) uint64 // 分配新页面
	del func(uint64)       // 解除分配页面
}

// treeInsert 将一个KV插入节中，结果可能会分裂成两个节点。
// 调用者负责释放输入节点的内存并拆分和分配结果节点。
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {

	// 允许超过1页 如果超过将会被分开
	newNode := BNode{data: make([]byte, 2*BTreePageSize)}
	idx := nodeLookupLE(node, key)

	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val)
		} else {
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNodeNode:
		// 内部节点，将其插入到子节点。
		nodeInsert(tree, newNode, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return newNode
}

// nodeInsert treeInsert()的部分: KV插入到中间节点
func nodeInsert(tree *BTree, newNode BNode, node BNode, idx uint16, key []byte, val []byte) {
	// 获取并释放子节点
	kPtr := node.getPtr(idx)
	kNode := tree.get(kPtr)
	tree.del(kPtr)
	// 递归插入到子节点
	kNode = treeInsert(tree, kNode, key, val)
	// 拆分结果
	nSplit, split := nodeSplit3(kNode)
	// 更新子结点链接
	nodeReplaceKidN(tree, newNode, node, idx, split[:nSplit]...)
}

// nodeSplit2 将大于允许的节点拆分为2个节点，第2个节点始终适合页面。
func nodeSplit2(left BNode, right BNode, old BNode) {
	util.Assert(old.nKeys() >= 2)

	// 最初的猜测
	nLeft := old.nKeys() / 2

	// 尝试适配左半部分
	leftBytes := func() uint16 {
		return Header + 8*nLeft + 2*nLeft + old.getOffset(nLeft)
	}
	for leftBytes() > BTreePageSize {
		nLeft--
	}
	util.Assert(nLeft >= 1)

	// 尝试适配右半部分
	rightBytes := func() uint16 {
		return old.nBytes() - leftBytes() + Header
	}
	for rightBytes() > BTreePageSize {
		nLeft++
	}
	util.Assert(nLeft < old.nKeys())
	nRight := old.nKeys() - nLeft

	left.setHeader(old.bType(), nLeft)
	right.setHeader(old.bType(), nRight)
	nodeAppendRange(left, old, 0, 0, nLeft)
	nodeAppendRange(right, old, 0, nLeft, nRight)

	util.Assert(right.nBytes() <= BTreePageSize)
}

// nodeSplit3 如果节点太大，则拆分节点。结果可能是 1~3 个节点。
// 最坏情况下 有个一个大KV在中间
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BTreePageSize {
		old.data = old.data[:BTreePageSize]
		return 1, [3]BNode{old}
	}
	// 以后可能会拆分
	left := BNode{make([]byte, 2*BTreePageSize)}
	right := BNode{make([]byte, BTreePageSize)}
	nodeSplit2(left, right, old)
	if left.nBytes() <= BTreePageSize {
		left.data = left.data[:BTreePageSize]
		return 2, [3]BNode{left, right}
	}
	// 左侧节点仍然太大
	leftOfLeft := BNode{make([]byte, BTreePageSize)}
	middle := BNode{make([]byte, BTreePageSize)}
	nodeSplit2(leftOfLeft, middle, left)
	return 3, [3]BNode{leftOfLeft, middle, right}
}

// nodeReplaceKidN 将链接替换为多个链接
func nodeReplaceKidN(tree *BTree, dstNode BNode, srcNode BNode, idx uint16, kids ...BNode) {
	dstNode.setHeader(BNodeNode, srcNode.nKeys()+uint16(len(kids))-1)
	nodeAppendRange(dstNode, srcNode, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(dstNode, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(dstNode, srcNode, idx+uint16(len(kids)), idx+1, srcNode.nKeys()-(idx+1))
}

// treeDelete 从树中删除键
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)
	// 根据节点类型执行操作
	switch node.bType() {
	case BNodeLeaf:
		if !bytes.Equal(key, node.getKey(idx)) {
			// 未找到
			return BNode{}
		}
		// 删除叶子结点中的键
		newNode := BNode{data: make([]byte, BTreePageSize)}
		leafDelete(newNode, node, idx)
		return newNode
	case BNodeNode:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node!")
	}
}

// nodeDelete treeDelete()的一部分
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// 递归到那个孩子
	kPtr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kPtr), key)
	if len(updated.data) == 0 {
		// 未找到
		return BNode{}
	}
	tree.del(kPtr)
	newNode := BNode{data: make([]byte, BTreePageSize)}
	// 检查合并
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTreePageSize)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTreePageSize)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(newNode, node, idx, tree.new(merged), merged.getKey(0))
	default:
		if updated.nKeys() == 0 {
			// kid 在删除后为空，并且没有要合并的兄弟姐妹。
			// 当它的父母只有一个孩子时，就会发生这种情况。
			// 丢弃空 kid 并将 parent 作为空节点返回。
			util.Assert(node.nKeys() == 1 && idx == 0)
			newNode.setHeader(BNodeNode, 0)
			// 空节点将在到达 root 之前被消除。
		} else {
			nodeReplaceKidN(tree, newNode, node, idx, updated)
		}
	}
	return newNode
}

// nodeReplace2Kid 将2个相邻key替换为1个
func nodeReplace2Kid(dstNode BNode, srcNode BNode, idx uint16, ptr uint64, key []byte) {
	dstNode.setHeader(BNodeNode, srcNode.nKeys()-1)
	nodeAppendRange(dstNode, srcNode, 0, 0, idx)
	nodeAppendKV(dstNode, idx, ptr, key, nil)
	nodeAppendRange(dstNode, srcNode, idx+1, idx+2, srcNode.nKeys()-(idx+2))
}

// shouldMerge 更新后的孩子是否应该与兄弟姐妹合并
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.bType() > BTreePageSize/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.bType() + updated.bType() - Header
		if merged <= BTreePageSize {
			return -1, sibling
		}
	}
	if idx+1 < node.nKeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.bType() + updated.bType() - Header
		if merged <= BTreePageSize {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

// nodeGetKey tree.Get()的一部分
func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		} else {
			return nil, false
		}
	case BNodeNode:
		return nodeGetKey(tree, tree.get(node.getPtr(idx)), key)
	default:
		panic("bad node!")
	}
}

// Get 从树中获取值
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return nodeGetKey(tree, tree.get(tree.root), key)
}

// Delete 从树中删除键
func (tree *BTree) Delete(key []byte) bool {
	util.Assert(len(key) != 0)
	util.Assert(len(key) <= BTreeMaxKeySize)
	if tree.root == 0 {
		return false
	}
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}
	tree.del(tree.root)
	if updated.bType() == BNodeNode && updated.nKeys() == 1 {
		// remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

// Insert 从树中插入键值对
func (tree *BTree) Insert(key []byte, val []byte) {
	util.Assert(len(key) != 0)
	util.Assert(len(key) <= BTreeMaxKeySize)
	util.Assert(len(val) <= BTreeMaxValSize)
	if tree.root == 0 {
		// 创建第一个节点
		root := BNode{data: make([]byte, BTreePageSize)}
		root.setHeader(BNodeLeaf, 2)
		// 一个虚拟键，这使得树覆盖整个键空间。
		// 因此，查找总是可以找到包含的节点。
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	node := tree.get(tree.root)
	tree.del(tree.root)
	node = treeInsert(tree, node, key, val)
	nSplit, split := nodeSplit3(node)
	if nSplit > 1 {
		// 根被分割，添加新级别。
		root := BNode{data: make([]byte, BTreePageSize)}
		root.setHeader(BNodeNode, nSplit)
		for i, kNode := range split[:nSplit] {
			ptr, key := tree.new(kNode), kNode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}
