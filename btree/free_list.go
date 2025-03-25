package btree

import (
	"encoding/binary"

	"db-practice/util"
)

// LNode
// | next | pointers | unused |
// |  8B  |   n*8B   |  ...   |
type LNode []byte

const FreeListHeader = 8
const FreeListCap = (BTreePageSize - FreeListHeader) / 8

// getNext 获取下一个页面的指针
func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node)
}

// setNext 设置下一个页面的指针
func (node LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(node, next)
}

// getPtr 获取第 idx 个指针
func (node LNode) getPtr(idx int) uint64 {
	util.Assert(idx >= 0 && idx < FreeListCap)
	offset := FreeListHeader + idx*8
	return binary.LittleEndian.Uint64(node[offset:])
}

// setPtr 设置第 idx 个指针
func (node LNode) setPtr(idx int, ptr uint64) {
	util.Assert(idx >= 0 && idx < FreeListCap)
	offset := FreeListHeader + idx*8
	binary.LittleEndian.PutUint64(node[offset:], ptr)
}

// FreeList 空闲页面链表
type FreeList struct {
	get func(uint64) []byte
	new func([]byte) uint64
	set func(uint64) []byte

	headPage uint64 // 指向 FreeList 链表头节点的页面 ID
	headSeq  uint64 // 一个单调递增的序列号，用于索引链表头节点中的空闲页面
	tailPage uint64
	tailSeq  uint64

	maxSeq uint64
}

// PopHead 弹出头部指针
func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

// flPop 从头部弹出指针
func flPop(fl *FreeList) (ptr uint64, head uint64) {
	fl.check()

	if fl.headSeq == fl.maxSeq {
		return
	}

	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++

	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		util.Assert(fl.headPage != 0)
	}

	return
}

// PushTail 添加一个新指针到尾部
func (fl *FreeList) PushTail(ptr uint64) {
	// 将其添加到 tail 节点
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	// 如果 tail 节点已满，则添加新的 tail 节点（列表永远不会为空）
	if seq2idx(fl.tailSeq) == 0 {
		// 尝试从列表头重用
		next, head := flPop(fl)
		if next == 0 {
			next = fl.new(make([]byte, BTreePageSize))
		}
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

// SetMaxSeq 设置最大序列号
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

// check 检查 FreeList 的完整性
func (fl *FreeList) check() {
	util.Assert(fl.headPage != 0 && fl.tailPage != 0)
	util.Assert(fl.headSeq != fl.tailSeq || fl.headPage == fl.tailPage)
}

// seq2idx 返回第 n 个指针的索引
func seq2idx(seq uint64) int {
	return int(seq % FreeListCap)
}
