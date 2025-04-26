package core

import (
	"bytes"

	"db-practice/util"
)

// BIter B-tree 迭代器
type BIter struct {
	tree *BTree
	path []BNode  // 从根到叶
	pos  []uint16 // 结点的位置
}

// iterIsFirst 判断迭代器是否指向第一个键
func iterIsFirst(iter *BIter) bool {
	for _, pos := range iter.pos {
		if pos != 0 {
			return false
		}
	}
	// 第一个key是一个假哨兵
	return true
}

// iterIsEnd 判断迭代器是否指向最后一个键
func iterIsEnd(iter *BIter) bool {
	last := len(iter.path) - 1
	return last < 0 || iter.pos[last] >= iter.path[last].nKeys()
}

// Valid 判断迭代器是否有效
func (iter *BIter) Valid() bool {
	return !(iterIsFirst(iter) || iterIsEnd(iter))
}

// Deref 当前 KV 对的值
func (iter *BIter) Deref() ([]byte, []byte) {
	util.Assert(iter.Valid())
	last := len(iter.path) - 1
	node := iter.path[last]
	pos := iter.pos[last]
	return node.getKey(pos), node.getVal(pos)
}

// iterNext 移动到下一个键
func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nKeys() {
		iter.pos[level]++ // 在此节点内移动
	} else if level > 0 {
		iterNext(iter, level-1) // 移动到同级节点
	} else {
		leaf := len(iter.pos) - 1
		iter.pos[leaf]++
		util.Assert(iter.pos[leaf] == iter.path[leaf].nKeys())
		return // 越过最后一个键
	}
	if level+1 < len(iter.pos) { // 更新子节点
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

// iterPrev 移动到上一个键
func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // 在此节点内移动
	} else if level > 0 {
		iterPrev(iter, level-1) // 移动到同级节点
	} else {
		panic("invalid iterPrev")
	}
	if level+1 < len(iter.pos) { // 更新子节点
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nKeys() - 1
	}
}

// Prev 移动到上一个键
func (iter *BIter) Prev() {
	if !iterIsFirst(iter) {
		iterPrev(iter, len(iter.path)-1)
	}
}

// Next 移动到下一个键
func (iter *BIter) Next() {
	if !iterIsEnd(iter) {
		iterNext(iter, len(iter.path)-1)
	}
}

// SeekLE 找到小于或等于 input 键的最近位置
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := BNode(tree.get(ptr))
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		ptr = node.getPtr(idx)
	}
	return iter
}

const (
	CmpGe = +3 // >=
	CmpGt = +2 // >
	CmpLt = -2 // <
	CmpLe = -3 // <=
)

// cmpOK key cmp ref
func cmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CmpGe:
		return r >= 0
	case CmpGt:
		return r > 0
	case CmpLt:
		return r < 0
	case CmpLe:
		return r <= 0
	default:
		panic("invalid cmp")
	}
}

// Seek 找到关于 'cmp' 关系的离键最近的位置
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	util.Assert(iterIsFirst(iter) || !iterIsEnd(iter))
	if cmp != CmpLe {
		cur := []byte(nil) // 哨兵 key
		if !iterIsFirst(iter) {
			cur, _ = iter.Deref()
		}
		if len(key) == 0 || !cmpOK(cur, cmp, key) {
			// off by one
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}
	if iter.Valid() {
		cur, _ := iter.Deref()
		util.Assert(cmpOK(cur, cmp, key))
	}
	return iter
}
