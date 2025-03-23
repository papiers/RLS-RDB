package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"

	"db-practice/util"
)

type KV struct {
	Path string
	// 内部
	fp   *os.File
	tree BTree
	free FreeList
	mmap struct {
		file   int      // 文件大小，可以大于数据库大小
		total  int      // mmap 大小，可以大于文件大小
		chunks [][]byte // 多个 mmap，可以是非连续的
	}
	page struct {
		flushed uint64   // 数据库大小（页数）
		temp    [][]byte // 新分配的页面
		nFree   int
		nAppend int
		updates map[uint64][]byte
	}
}

// pageGet 解引用指针。
func (db *KV) pageGet(ptr uint64) BNode {
	if page, ok := db.page.updates[ptr]; ok {
		util.Assert(page != nil)
		return BNode{page}
	}
	return pageGetMapped(db, ptr)
}

// pageGetMapped 获取映射页面。
func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTreePageSize
		if ptr < end {
			offset := BTreePageSize * (ptr - start)
			return BNode{chunk[offset : offset+BTreePageSize]}
		}
		start = end
	}
	panic("bad ptr")
}

// pageNew 分配一个新页面。
func (db *KV) pageNew(node BNode) uint64 {
	util.Assert(len(node.data) <= BTreePageSize)
	ptr := uint64(0)
	if db.page.nFree < db.free.Total() {
		ptr = db.free.Get(db.page.nFree)

		db.page.nAppend++
	} else {
		ptr = db.page.flushed + uint64(db.page.nAppend)
		db.page.nAppend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

// pageDel 解除分配页面.
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// pageAppend 分配一个新页面。
func (db *KV) pageAppend(node BNode) uint64 {
	util.Assert(len(node.data) <= BTreePageSize)
	ptr := db.page.flushed + uint64(db.page.nAppend)
	db.page.nAppend++
	db.page.updates[ptr] = node.data
	return ptr
}

// pageUse 更新页面。
func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}

// Open 打开数据库。
func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// Close 关闭数据库。
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		if err != nil {
			panic("error")
		}
	}
	_ = db.fp.Close()
}

// Get 获取值。
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// Set 设置值。
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

// Del 删除值。
func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// flushPages 刷新页面。
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

// writePages 将临时页面写入文件。
func writePages(db *KV) error {
	nPages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFile(db, nPages); err != nil {
		return err
	}
	if err := extendMmap(db, nPages); err != nil {
		return err
	}

	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr).data, page)
	}

	return nil
}

// syncPages 同步文件。
func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	if err := masterLoad(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// mmapInit 创建覆盖整个文件的初始 mMap。
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%BTreePageSize != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}
	mmapSize := 64 << 20
	util.Assert(mmapSize%BTreePageSize == 0)
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmapSize 可以大于文件
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
}

// extendMmap 通过添加新映射来扩展 mmap。
func extendMmap(db *KV, nPages int) error {
	if db.mmap.total >= nPages*BTreePageSize {
		return nil
	}
	// 地址空间翻倍
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

const DbSig = "BuildYourOwnDB06"

// masterLoad 母版页格式。
// 它包含指向根和其他重要位的指针。
// | sig | btree_root | page_used |
// | 16B | 8B | 8B |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// 空文件，则将在第一次写入时创建母版页。
		db.page.flushed = 1 // 为母版页保留
		return nil
	}
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	// 验证页面
	if !bytes.Equal([]byte(DbSig), data[:16]) {
		return errors.New("bad signature")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file/BTreePageSize))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("bad master page")
	}
	db.tree.root = root
	db.page.flushed = used
	return nil
}

// masterStore 更新母版页。它必须是原子的。
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], DbSig)
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// 注意：通过 mmap 更新页面不是原子的。
	// 改用 'pwrite()' syscall。
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// extendFile 将文件至少扩展到 'nPages'.
func extendFile(db *KV, nPages int) error {
	filePages := db.mmap.file / BTreePageSize
	if filePages >= nPages {
		return nil
	}
	for filePages < nPages {
		// 文件大小呈指数级增长,
		// 这样我们就不必为每次更新扩展文件.
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * BTreePageSize
	// mac 不支持 fallocate
	// err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	// 使用 file.Truncate 来扩展文件大小
	err := db.fp.Truncate(int64(fileSize))
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	db.mmap.file = fileSize
	return nil
}
