package types

import "db-practice/util"

const BNodeFreeList = 3
const FreeListHeader = 4 + 8 + 8
const FreeListCap = (BTreePageSize - FreeListHeader) / 8

type FreeList struct {
	head uint64

	get func(uint64) BNode
	new func(BNode) uint64
	use func(uint64, BNode)
}

// Total 列表中的项目数
func (fl *FreeList) Total() int {
	return 0
}

// Get 获取第 n 个指针
func (fl *FreeList) Get(topn int) uint64 {
	util.Assert(0 <= topn && topn < fl.Total())
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		util.Assert(next != 0)
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

// Update 删除 'popn' 指针并添加一些新指针
func (fl *FreeList) Update(popn int, freed []uint64) {
	util.Assert(popn <= fl.Total())
	if popn == 0 && len(freed) == 0 {
		return
	}
	total := fl.Total()
	var reuse []uint64

	for fl.head != 0 && len(reuse)*FreeListCap < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head)
		if popn >= flnSize(node) {
			popn -= flnSize(node)
		} else {
			remain := flnSize(node) - popn
			popn = 0
			for remain > 0 && len(reuse)*FreeListCap < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		total -= flnSize(node)
		fl.head = flnNext(node)
	}
	util.Assert(len(reuse)*FreeListCap >= len(freed) || fl.head == 0)
	flPush(fl, freed, reuse)
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

// flPush
func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		newNode := BNode{make([]byte, BTreePageSize)}
		size := len(freed)
		if size > FreeListCap {
			size = FreeListCap
		}
		flnSetHeader(newNode, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(newNode, uint16(i), ptr)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, newNode)
		} else {
			fl.head = fl.new(newNode)
		}
	}
	util.Assert(len(reuse) == 0)
}

func flnSize(node BNode) int {
	return 0
}

func flnNext(node BNode) uint64 {
	return 0
}

func flnPtr(node BNode, idx int) uint64 {
	return 0
}

func flnSetPtr(node BNode, size uint16, next uint64) {

}

func flnSetHeader(node BNode, size uint16, next uint64) {

}

func flnSetTotal(node BNode, total uint64) {

}
