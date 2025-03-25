package btree

import (
	"fmt"
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
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				util.Assert(ok)
				return node
			},
			new: func(node []byte) uint64 {
				util.Assert(BNode(node).nBytes() <= BTreePageSize)
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				util.Assert(pages[ptr] == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				util.Assert(pages[ptr] != nil)
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

func (c *C) get(key string) (string, bool) {
	val, ok := c.tree.Get([]byte(key))
	return string(val), ok
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
		node := BNode(c.tree.get(ptr))
		nKeys := node.nKeys()
		if node.bType() == BNodeLeaf {
			for i := uint16(0); i < nKeys; i++ {
				keys = append(keys, string(node.getKey(i)))
				vals = append(vals, string(node.getVal(i)))
			}
		} else {
			for i := uint16(0); i < nKeys; i++ {
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
