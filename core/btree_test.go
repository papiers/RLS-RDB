package core

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	is "github.com/stretchr/testify/require"

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

// add 添加键值对
func (c *C) add(key string, val string) {
	c.tree.Upsert([]byte(key), []byte(val))
	c.ref[key] = val
}

// del 删除键值对
func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

// dump 遍历树，返回所有键值对
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

type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (s sortIF) Len() int {
	return s.len
}
func (s sortIF) Less(i, j int) bool {
	return s.less(i, j)
}
func (s sortIF) Swap(i, j int) {
	s.swap(i, j)
}

// verify 验证树是否正确
func (c *C) verify(t *testing.T) {
	keys, vals := c.dump()

	rKeys := []string{""}
	rVals := []string{""}
	for k, v := range c.ref {
		rKeys = append(rKeys, k)
		rVals = append(rVals, v)
	}
	rKeys, rVals = rKeys[1:], rVals[1:]

	is.Equal(t, len(rKeys), len(keys))
	sort.Sort(sortIF{
		len:  len(rKeys),
		less: func(i, j int) bool { return rKeys[i] < rKeys[j] },
		swap: func(i, j int) {
			rKeys[i], rKeys[j] = rKeys[j], rKeys[i]
			rVals[i], rVals[j] = rVals[j], rVals[i]
		},
	})

	is.Equal(t, rKeys, keys)
	is.Equal(t, rVals, vals)

	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nKeys := node.nKeys()
		util.Assert(nKeys >= 1)
		if node.bType() == BNodeLeaf {
			return
		}
		for i := uint16(0); i < nKeys; i++ {
			key := node.getKey(i)
			kid := BNode(c.tree.get(node.getPtr(i)))
			is.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(c.tree.get(c.tree.root))
}

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func commonTestBasic(t *testing.T, hasher func(uint32) uint32) {
	c := newC()
	c.add("k", "v")
	c.verify(t)

	// insert
	for i := 0; i < 250000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		val := fmt.Sprintf("vvv%d", hasher(uint32(-i)))
		c.add(key, val)
		if i < 2000 {
			c.verify(t)
		}
	}
	c.verify(t)

	// del
	for i := 2000; i < 250000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		is.True(t, c.del(key))
	}
	c.verify(t)

	// overwrite
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		val := fmt.Sprintf("vvv%d", hasher(uint32(+i)))
		c.add(key, val)
		c.verify(t)
	}

	is.False(t, c.del("kk"))

	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", hasher(uint32(i)))
		is.True(t, c.del(key))
		c.verify(t)
	}

	c.add("k", "v2")
	c.verify(t)
	c.del("k")
	c.verify(t)

	// the dummy empty key
	is.Equal(t, 1, len(c.pages))
	is.Equal(t, uint16(1), BNode(c.tree.get(c.tree.root)).nKeys())
}

func TestBTreeBasicAscending(t *testing.T) {
	commonTestBasic(t, func(h uint32) uint32 { return +h })
}

func TestBTreeBasicDescending(t *testing.T) {
	commonTestBasic(t, func(h uint32) uint32 { return -h })
}

func TestBTreeBasicRand(t *testing.T) {
	commonTestBasic(t, fmix32)
}

func TestBTreeRandLength(t *testing.T) {
	c := newC()
	for i := 0; i < 2000; i++ {
		kLen := fmix32(uint32(2*i+0)) % BTreeMaxKeySize
		vLen := fmix32(uint32(2*i+1)) % BTreeMaxValSize
		if kLen == 0 {
			continue
		}

		key := make([]byte, kLen)
		for j := range key {
			key[j] = byte(rand.Intn(256))
		}
		val := make([]byte, vLen)
		// rand.Read(val)
		c.add(string(key), string(val))
		c.verify(t)
	}
}

func TestBTreeIncLength(t *testing.T) {
	for l := 1; l < BTreeMaxKeySize+BTreeMaxValSize; l++ {
		c := newC()

		kLen := l
		if kLen > BTreeMaxKeySize {
			kLen = BTreeMaxKeySize
		}
		vLen := l - kLen
		key := make([]byte, kLen)
		val := make([]byte, vLen)

		factor := BTreePageSize / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			for v := range key {
				key[v] = byte(rand.Intn(256))
			}
			c.add(string(key), string(val))
		}
		c.verify(t)
	}
}

func printSliceInfo(s []string) {
	fmt.Printf("s: %v, is nil: %t, len: %d, cap: %d\n", s, s == nil, len(s), cap(s))
}
