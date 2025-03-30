package core

import (
	"bytes"

	"db-practice/util"
)

type BTree struct {
	// pointer（非零页码）
	root uint64
	// 用于管理磁盘上页面的回调
	get func(uint64) []byte // 解引用指针
	new func([]byte) uint64 // 分配新页面
	del func(uint64)        // 解除分配页面
}

// 更新模式
const (
	ModeUpsert     = 0 // 插入或替换
	ModeUpdateOnly = 1 // 更新存在的key
	ModeInsertOnly = 2 // 仅添加新key
)

type UpdateReq struct {
	tree *BTree

	Added   bool // 添加了新key
	Updated bool // 已添加新key或更改旧key

	Key  []byte
	Val  []byte
	Mode int
}

// treeInsert 将一个KV插入节中，结果可能会分裂成两个节点。
// 调用者负责释放输入节点的内存并拆分和分配结果节点。
func treeInsert(req *UpdateReq, node BNode) BNode {

	// 允许超过1页 如果超过将会被分开
	newNode := BNode(make([]byte, 2*BTreePageSize))
	idx := nodeLookupLE(node, req.Key)

	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(req.Key, node.getKey(idx)) {
			if req.Mode == ModeInsertOnly {
				return BNode{}
			}
			if bytes.Equal(req.Val, node.getVal(idx)) {
				return BNode{}
			}
			leafUpdate(newNode, node, idx, req.Key, req.Val)
			req.Updated = true
		} else {
			if req.Mode == ModeUpdateOnly {
				return BNode{}
			}
			leafInsert(newNode, node, idx+1, req.Key, req.Val)
			req.Updated = true
			req.Added = true
		}
	case BNodeNode:
		// 内部节点，将其插入到子节点。
		// 获取并释放子节点
		kPtr := node.getPtr(idx)
		kNode := req.tree.get(kPtr)
		// 递归插入到子节点
		kNode = treeInsert(req, kNode)
		if len(kNode) == 0 {
			return BNode{}
		}
		req.tree.del(kPtr)
		// 拆分结果
		nSplit, split := nodeSplit3(kNode)
		// 更新子结点链接
		nodeReplaceKidN(req.tree, newNode, node, idx, split[:nSplit]...)
	default:
		panic("bad node!")
	}
	return newNode
}

// nodeSplit2 将大于允许的节点拆分为2个节点，第2个节点始终适合页面。
func nodeSplit2(left, right, old BNode) {
	util.Assert(old.nKeys() >= 2)

	// 最初的猜测
	nLeft := old.nKeys() / 2

	// 尝试适配左半部分
	leftBytes := func() uint16 {
		return Header + PointerSize*nLeft + offsetSize*nLeft + old.getOffset(nLeft)
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
	util.Assert(old.nBytes() <= 3*BTreePageSize+2*Header)
	if old.nBytes() <= BTreePageSize {
		old = old[:BTreePageSize]
		return 1, [3]BNode{old}
	}
	// 以后可能会拆分
	left := BNode(make([]byte, 2*BTreePageSize))
	right := BNode(make([]byte, BTreePageSize))
	nodeSplit2(left, right, old)
	if left.nBytes() <= BTreePageSize {
		left = left[:BTreePageSize]
		return 2, [3]BNode{left, right}
	}
	// 左侧节点仍然太大
	leftOfLeft := BNode(make([]byte, BTreePageSize))
	middle := BNode(make([]byte, BTreePageSize))
	nodeSplit2(leftOfLeft, middle, left)
	util.Assert(leftOfLeft.nBytes() <= BTreePageSize)

	return 3, [3]BNode{leftOfLeft, middle, right}
}

// nodeReplaceKidN 将节点中idx节点替换为多个子节点。
func nodeReplaceKidN(tree *BTree, dstNode, srcNode BNode, idx uint16, kids ...BNode) {
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
			return BNode{}
		}
		// 删除叶子结点中的键
		newNode := BNode(make([]byte, BTreePageSize))
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
	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kPtr)
	newNode := BNode(make([]byte, BTreePageSize))
	// 检查合并
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTreePageSize))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTreePageSize))
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

// shouldMerge 判断更新后的孩子是否应该与兄弟姐妹合并
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTreePageSize/4 {
		return 0, BNode{}
	}
	merge := func(idx uint16) (BNode, bool) {
		sibling := BNode(tree.get(node.getPtr(idx)))
		ok := sibling.nBytes()+updated.nBytes()-Header <= BTreePageSize
		return sibling, ok
	}
	if idx > 0 {
		if sibling, ok := merge(idx - 1); ok {
			return -1, sibling
		}
	}
	if idx+1 < node.nKeys() {
		if sibling, ok := merge(idx + 1); ok {
			return 1, sibling
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
	if len(updated) == 0 {
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

// Update 更新树中的键值对
func (tree *BTree) Update(req *UpdateReq) bool {
	util.Assert(len(req.Key) != 0)
	util.Assert(len(req.Key) <= BTreeMaxKeySize)
	util.Assert(len(req.Val) <= BTreeMaxValSize)

	if tree.root == 0 {
		// 创建第一个节点
		root := BNode(make([]byte, BTreePageSize))
		root.setHeader(BNodeLeaf, 2)
		// 一个虚拟键，这使得树覆盖整个键空间。
		// 因此，查找总是可以找到包含的节点。
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.root = tree.new(root)
		req.Added = true
		req.Updated = true
		return true
	}

	req.tree = tree
	updated := treeInsert(req, tree.get(tree.root))
	if len(updated) == 0 {
		return false
	}
	nSplit, split := nodeSplit3(updated)
	tree.del(tree.root)
	if nSplit > 1 {
		// 根被分割，添加新级别。
		root := BNode(make([]byte, BTreePageSize))
		root.setHeader(BNodeNode, nSplit)
		for i, kNode := range split[:nSplit] {
			ptr, key := tree.new(kNode), kNode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
	return true
}

// Upsert 更新或插入键值对
func (tree *BTree) Upsert(key []byte, val []byte) bool {
	return tree.Update(&UpdateReq{Key: key, Val: val})
}
