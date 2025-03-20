package types

import (
	"fmt"
	"testing"
	"unsafe"

	"db-practice/util"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				util.Assert(ok)
				return node
			},
			new: func(node BNode) uint64 {
				util.Assert(node.nBytes() <= BTreePageSize)
				ptr := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				util.Assert(pages[ptr].data == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				util.Assert(pages[ptr].data != nil)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) *C {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
	return c
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) dump() ([]string, []string) {
	var keys []string
	var vals []string

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := c.tree.get(ptr)
		nkeys := node.nKeys()
		if node.bType() == BNodeLeaf {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.getPtr(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(c.tree.root)
	util.Assert(keys[0] == "")
	util.Assert(vals[0] == "")
	return keys[1:], vals[1:]
}

func (c *C) print() {
	for k, v := range c.pages {
		fmt.Println(k, v.String())
	}
	fmt.Println()
}

func TestBTreeIncLength(t *testing.T) {
	c := newC()
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("000000key%d", i)
		value := fmt.Sprintf("value%d", i)
		c.add(key, value).print()
	}
}
