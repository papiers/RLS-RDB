package core

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	is "github.com/stretchr/testify/require"

	"db-practice/util"
)

// 测试DB
type D struct {
	db  KV
	ref map[string]string
}

// noFsync 用于测试，不进行fsync操作
func noFsync(int) error {
	return nil
}

// newD 创建测试用的DB
func newD() *D {
	err := os.Remove("test.db")
	util.Assert(err == nil || os.IsNotExist(err))

	d := &D{}
	d.ref = map[string]string{}
	d.db.Path = "test.db"
	d.db.Fsync = noFsync // faster
	err = d.db.Open()
	util.Assert(err == nil)
	return d
}

// reopen 关闭并重新打开DB
func (d *D) reopen() {
	d.db.Close()
	d.db = KV{Path: d.db.Path, Fsync: d.db.Fsync}
	err := d.db.Open()
	util.Assert(err == nil)
}

// dispose 关闭并删除DB
func (d *D) dispose() {
	d.db.Close()
	err := os.Remove("test.db")
	util.Assert(err == nil)
}

// add 添加键值对
func (d *D) add(key string, val string) {
	_, _ = d.db.Set([]byte(key), []byte(val))
	d.ref[key] = val
}

// get 获取键对应的值
func (d *D) del(key string) bool {
	delete(d.ref, key)
	deleted, err := d.db.Del([]byte(key))
	util.Assert(err == nil)
	return deleted
}

// dump 遍历树，返回所有键值对
func (d *D) dump() ([]string, []string) {
	return dump(&d.db.tree)
}

// verify 验证DB的一致性
func (d *D) verify(t *testing.T) {
	keys, vals := d.dump()

	rKeys, rVals := []string{""}, []string{""}
	for k, v := range d.ref {
		rKeys = append(rKeys, k)
		rVals = append(rVals, v)
	}
	rKeys, rVals = rKeys[1:], rVals[1:]

	is.Equal(t, len(rKeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rKeys),
		less: func(i, j int) bool { return rKeys[i] < rKeys[j] },
		swap: func(i, j int) {
			rKeys[i], rKeys[j] = rKeys[j], rKeys[i]
			rVals[i], rVals[j] = rVals[j], rVals[i]
		},
	})

	is.Equal(t, rKeys, keys)
	is.Equal(t, rVals, vals)

	// track visited pages
	pages := make([]uint8, d.db.page.flushed)
	pages[0] = 1
	pages[d.db.tree.root] = 1
	// verify node structures
	var nodeVerify func(BNode)
	nodeVerify = func(node BNode) {
		nKeys := node.nKeys()
		util.Assert(nKeys >= 1)
		if node.bType() == BNodeLeaf {
			return
		}
		for i := uint16(0); i < nKeys; i++ {
			ptr := node.getPtr(i)
			is.Zero(t, pages[ptr])
			pages[ptr] = 1 // tree node
			key := node.getKey(i)
			kid := BNode(d.db.tree.get(node.getPtr(i)))
			is.Equal(t, key, kid.getKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(d.db.tree.get(d.db.tree.root))

	// 空闲链表
	list, nodes := flDump(&d.db.free)
	for _, ptr := range nodes {
		is.Zero(t, pages[ptr])
		pages[ptr] = 2 // free list node
	}
	for _, ptr := range list {
		is.Zero(t, pages[ptr])
		pages[ptr] = 3 // free list content
	}
	for _, flag := range pages {
		is.NotZero(t, flag) // every page is accounted for
	}
}

// funcTestKVBasic 测试KV的基本功能
func funcTestKVBasic(t *testing.T, reopen bool) {
	c := newD()
	defer c.dispose()

	c.add("k", "v")
	c.verify(t)

	// insert
	for i := 0; i < 25000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(-i)))
		c.add(key, val)
		if i < 2000 {
			c.verify(t)
		}
	}
	c.verify(t)
	if reopen {
		c.reopen()
		c.verify(t)
	}
	t.Log("insertion done")

	// del
	for i := 2000; i < 25000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
	}
	c.verify(t)
	if reopen {
		c.reopen()
		c.verify(t)
	}
	t.Log("deletion done")

	// overwrite
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		val := fmt.Sprintf("vvv%d", fmix32(uint32(+i)))
		c.add(key, val)
		c.verify(t)
	}

	is.False(t, c.del("kk"))

	// remove all
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		is.True(t, c.del(key))
		c.verify(t)
	}
	if reopen {
		c.reopen()
		c.verify(t)
	}

	c.add("k", "v2")
	c.verify(t)
	c.del("k")
	c.verify(t)
}

func TestKVBasic(t *testing.T) {
	funcTestKVBasic(t, false)
	funcTestKVBasic(t, true)
}

// fsyncErr 模拟fsync错误
func fsyncErr(errors ...int) func(int) error {
	return func(int) error {
		fail := errors[0]
		errors = errors[1:]
		if fail != 0 {
			return fmt.Errorf("fsync error")
		} else {
			return nil
		}
	}
}

func TestKVFsyncErr(t *testing.T) {
	c := newD()
	defer c.dispose()

	set := func(key []byte, val []byte) error {
		updated, err := c.db.Set(key, val)
		util.Assert(err == nil || !updated)
		return err
	}
	get := c.db.Get

	err := set([]byte("k"), []byte("1"))
	util.Assert(err == nil)
	val, ok := get([]byte("k"))
	util.Assert(ok && string(val) == "1")

	c.db.Fsync = fsyncErr(1)
	err = set([]byte("k"), []byte("2"))
	util.Assert(err != nil)
	val, ok = get([]byte("k"))
	util.Assert(ok && string(val) == "1")

	c.db.Fsync = noFsync
	err = set([]byte("k"), []byte("3"))
	util.Assert(err == nil)
	val, ok = get([]byte("k"))
	util.Assert(ok && string(val) == "3")

	c.db.Fsync = fsyncErr(0, 1)
	err = set([]byte("k"), []byte("4"))
	util.Assert(err != nil)
	val, ok = get([]byte("k"))
	util.Assert(ok && string(val) == "3")

	c.db.Fsync = noFsync
	err = set([]byte("k"), []byte("5"))
	util.Assert(err == nil)
	val, ok = get([]byte("k"))
	util.Assert(ok && string(val) == "5")

	c.db.Fsync = fsyncErr(0, 1)
	err = set([]byte("k"), []byte("6"))
	util.Assert(err != nil)
	val, ok = get([]byte("k"))
	util.Assert(ok && string(val) == "5")
}

func TestKVRandLength(t *testing.T) {
	c := newD()
	defer c.dispose()

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

func TestKVIncLength(t *testing.T) {
	for l := 1; l < BTreeMaxKeySize+BTreeMaxValSize; l++ {
		c := newD()

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

		c.dispose()
	}
}

// fileSize 获取文件大小
func fileSize(path string) int64 {
	fInfo, err := os.Stat(path)
	util.Assert(err == nil)
	return fInfo.Size()
}

// test the free list: file size do not increase under various operations
func TestKVFileSize(t *testing.T) {
	c := newD()
	fill := func(seed int) {
		for i := 0; i < 2000; i++ {
			key := fmt.Sprintf("key%d", fmix32(uint32(i)))
			val := fmt.Sprintf("vvv%010d", fmix32(uint32(seed*2000+i)))
			c.add(key, val)
		}
	}
	fill(0)
	fill(1)
	size := fileSize(c.db.Path)

	// update the same key
	fill(2)
	util.Assert(size == fileSize(c.db.Path))

	// remove everything
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key%d", fmix32(uint32(i)))
		c.del(key)
	}
	util.Assert(size == fileSize(c.db.Path))

	// add them back
	fill(3)
	util.Assert(size == fileSize(c.db.Path))
}
