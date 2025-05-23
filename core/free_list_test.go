package core

import (
	"slices"
	"testing"

	"db-practice/util"
)

// 测试 FreeList
type L struct {
	free  FreeList
	pages map[uint64][]byte // simulate disk pages
	// references
	added   []uint64
	removed []uint64
}

// 新建 FreeList
func newL() *L {
	pages := map[uint64][]byte{}
	pages[1] = make([]byte, BTreePageSize) // initial node
	appendNum := uint64(1000)              // [1000, 10000)
	return &L{
		free: FreeList{
			get: func(ptr uint64) []byte {
				util.Assert(pages[ptr] != nil)
				return pages[ptr]
			},
			set: func(ptr uint64) []byte {
				util.Assert(pages[ptr] != nil)
				return pages[ptr]
			},
			new: func(node []byte) uint64 {
				util.Assert(pages[appendNum] == nil)
				pages[appendNum] = node
				appendNum++
				return appendNum - 1
			},
			headPage: 1, // initial node
			tailPage: 1,
		},
		pages: pages,
	}
}

// flDump  返回内容和结点
func flDump(free *FreeList) (list []uint64, nodes []uint64) {
	ptr := free.headPage
	nodes = append(nodes, ptr)
	for seq := free.headSeq; seq != free.tailSeq; {
		util.Assert(ptr != 0)
		node := LNode(free.get(ptr))
		list = append(list, node.getPtr(seq2idx(seq)))
		seq++
		if seq2idx(seq) == 0 {
			ptr = node.getNext()
			nodes = append(nodes, ptr)
		}
	}
	return
}

// push 添加一个新结点到 FreeList
func (l *L) push(ptr uint64) {
	util.Assert(l.pages[ptr] == nil)
	l.pages[ptr] = make([]byte, BTreePageSize)
	l.free.PushTail(ptr)
	l.added = append(l.added, ptr)
}

// pop 返回一个新结点
func (l *L) pop() uint64 {
	ptr := l.free.PopHead()
	if ptr != 0 {
		l.removed = append(l.removed, ptr)
	}
	return ptr
}

// verify 验证 FreeList 的正确性
func (l *L) verify() {
	l.free.check()

	// dump all pointers from `l.pages`
	var appended []uint64
	var ptrs []uint64
	for ptr := range l.pages {
		if 1000 <= ptr && ptr < 10000 {
			appended = append(appended, ptr)
		} else if ptr != 1 {
			util.Assert(slices.Contains(l.added, ptr))
		}
		ptrs = append(ptrs, ptr)
	}
	// dump all pointers from the free list
	list, nodes := flDump(&l.free)

	// any pointer is either in the free list, a list node, or removed.
	util.Assert(len(l.pages) == len(list)+len(nodes)+len(l.removed))
	combined := slices.Concat(list, nodes, l.removed)
	slices.Sort(combined)
	slices.Sort(ptrs)
	util.Assert(slices.Equal(combined, ptrs))

	// any pointer is either the initial node, an allocated node, or added
	util.Assert(len(l.pages) == 1+len(appended)+len(l.added))
	combined = slices.Concat([]uint64{1}, appended, l.added)
	slices.Sort(combined)
	util.Assert(slices.Equal(combined, ptrs))
}

func TestFreeListEmptyFullEmpty(t *testing.T) {
	for N := 0; N < 2000; N++ {
		l := newL()
		for i := 0; i < N; i++ {
			l.push(10000 + uint64(i))
		}
		l.verify()

		util.Assert(l.pop() == 0)
		l.free.SetMaxSeq()
		ptr := l.pop()
		for ptr != 0 {
			l.free.SetMaxSeq()
			ptr = l.pop()
		}
		l.verify()

		list, nodes := flDump(&l.free)
		util.Assert(len(list) == 0)
		util.Assert(len(nodes) == 1)
		// println("N", N)
	}
}

func TestFreeListEmptyFullEmpty2(t *testing.T) {
	for N := 0; N < 2000; N++ {
		l := newL()
		for i := 0; i < N; i++ {
			l.push(10000 + uint64(i))
			l.free.SetMaxSeq() // allow self-reuse
		}
		l.verify()

		ptr := l.pop()
		for ptr != 0 {
			l.free.SetMaxSeq()
			ptr = l.pop()
		}
		l.verify()

		list, nodes := flDump(&l.free)
		util.Assert(len(list) == 0)
		util.Assert(len(nodes) == 1)
		// println("N", N)
	}
}

func TestFreeListRandom(t *testing.T) {
	for N := 0; N < 1000; N++ {
		l := newL()
		for i := 0; i < 2000; i++ {
			ptr := uint64(10000 + fmix32(uint32(i)))
			if ptr%2 == 0 {
				l.push(ptr)
			} else {
				l.pop()
			}
		}
		l.verify()
	}
}
