package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"

	"db-practice/util"
)

type KV struct {
	Path string

	fd   int
	tree BTree
	free FreeList
	mmap struct {
		total  int      // mmap 大小，可以大于文件大小
		chunks [][]byte // 多个 mmap，可以是非连续的
	}
	page struct {
		flushed uint64 // 数据库大小（页数）
		nappend uint64
		updates map[uint64][]byte
	}
	failed bool
}

// pageRead 读取一个页面
func (db *KV) pageRead(ptr uint64) []byte {
	util.Assert(ptr < db.page.flushed+db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	return db.pageReadFile(ptr)
}

// pageAppend 分配一个新页面。
func (db *KV) pageAppend(node []byte) uint64 {
	util.Assert(len(node) == BTreePageSize)
	ptr := db.page.flushed + db.page.nappend
	db.page.nappend++
	util.Assert(db.page.updates[ptr] != nil)
	db.page.updates[ptr] = node
	return ptr
}

// pageAlloc 分配一个新页面
func (db *KV) pageAlloc(node []byte) uint64 {
	util.Assert(len(node) == BTreePageSize)
	if ptr := db.free.PopHead(); ptr != 0 {
		db.page.updates[ptr] = node
		return ptr
	}
	return db.pageAppend(node)
}

// pageWrite 更新一个存在的页面
func (db *KV) pageWrite(ptr uint64) []byte {
	util.Assert(ptr < db.page.flushed+db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	node := make([]byte, BTreePageSize)
	copy(node, db.pageReadFile(ptr))
	db.page.updates[ptr] = node
	return node
}

// pageReadFile 从文件中读取一个页面
func (db *KV) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTreePageSize
		if ptr < end {
			offset := BTreePageSize * (ptr - start)
			return chunk[offset : offset+BTreePageSize]
		}
		start = end
	}
	panic("bad ptr")
}

// Open 打开数据库。
func (db *KV) Open() error {
	db.page.updates = make(map[uint64][]byte)

	db.tree.get = db.pageRead
	db.tree.new = db.pageAlloc
	db.tree.del = db.free.PushTail

	db.free.get = db.pageRead
	db.free.new = db.pageAppend
	db.free.set = db.pageWrite

	var err error
	// 打开或创建 DB 文件
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}

	// 获取文件大小
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}

	// 创建初始 mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}

	// 阅读 meta 页面
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)

}

// Close 关闭数据库
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		util.Assert(err == nil)
	}
	_ = syscall.Close(db.fd)
}

// Get 获取值
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// Set 设置值
func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}

// Del 删除值
func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	if !db.tree.Delete(key) {
		return false, nil
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

// updateFile 更新文件 设置、删除时调用
// 通过两阶段更新和两次 fsync 调用，确保了 copy-on-write 树更新的 原子性 和 持久性：
// 原子性：通过原子更新根节点（步骤 3）实现。
// 持久性：通过两次 fsync 确保新页面和根节点的更新都写入磁盘。
// 顺序性：第一次 fsync 确保新页面在根节点更新前持久化。
func updateFile(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}
	if err := updateRoot(db); err != nil {
		return err
	}
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}
	// 为下一次更新准备freelist
	db.free.SetMaxSeq()
	return nil
}

// updateOrRevert 更新或回滚
func updateOrRevert(db *KV, meta []byte) error {
	// 确保 On-Disk Meta 页面与错误后的 In-Memory 页面匹配
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}
		if err := syscall.Fsync(db.fd); err != nil {
			return err
		}
		db.failed = false
	}
	if err := updateFile(db); err != nil {
		db.failed = true
		loadMeta(db, meta)
		db.page.nappend = 0
		db.page.updates = make(map[uint64][]byte)
		return err
	}
	return nil
}

// createFileSync 创建文件并同步目录
func createFileSync(file string) (int, error) {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirFd, err := syscall.Open(path.Dir(file), flags, 0o664)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer func(fd int) {
		_ = syscall.Close(fd)
	}(dirFd)

	flags = os.O_RDWR | os.O_CREATE
	fd, err := util.Openat(dirFd, path.Base(file), flags, 0o664)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	if err = syscall.Fsync(dirFd); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	return fd, nil
}

// writePages 将临时页面写入文件。
func writePages(db *KV) error {
	size := int(db.page.flushed+db.page.nappend) * BTreePageSize
	if err := extendMmap(db, size); err != nil {
		return err
	}

	for ptr, node := range db.page.updates {
		offset := int64(ptr * BTreePageSize)
		if _, err := syscall.Pwrite(db.fd, node, offset); err != nil {
			return err
		}
	}

	db.page.flushed += db.page.nappend
	db.page.nappend = 0
	db.page.updates = make(map[uint64][]byte)
	return nil
}

// extendMmap 通过添加新映射来扩展 mmap
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil
	}
	alloc := max(db.mmap.total, 64<<20)
	for db.mmap.total+alloc < size {
		// 地址空间翻倍
		alloc *= 2
	}

	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

const DbSig = "BuildYourOwnDB06"

// | sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq |
// | 16B |    8B    |    8B     |     8B    |    8B    |     8B    |    8B    |
// saveMeta 保存元数据到内存
func saveMeta(db *KV) []byte {
	var data [64]byte
	copy(data[:16], DbSig)
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	binary.LittleEndian.PutUint64(data[32:], db.free.headPage)
	binary.LittleEndian.PutUint64(data[40:], db.free.headSeq)
	binary.LittleEndian.PutUint64(data[48:], db.free.tailPage)
	binary.LittleEndian.PutUint64(data[56:], db.free.tailSeq)
	return data[:]
}

// loadMeta 从内存加载元数据
func loadMeta(db *KV, data []byte) {
	if string(data[:16]) != DbSig {
		panic("invalid db file")
	}
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
	db.free.headPage = binary.LittleEndian.Uint64(data[32:])
	db.free.headSeq = binary.LittleEndian.Uint64(data[40:])
	db.free.tailPage = binary.LittleEndian.Uint64(data[48:])
	db.free.tailSeq = binary.LittleEndian.Uint64(data[56:])
}

// readRoot 读取根页面
func readRoot(db *KV, fileSize int64) error {
	if fileSize&BTreePageSize != 0 {
		return fmt.Errorf("file size must be a multiple of %d", BTreePageSize)
	}
	if fileSize == 0 {
		db.page.flushed = 2
		db.free.headPage = 1
		db.free.tailPage = 1
		return nil
	}
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	db.free.SetMaxSeq()

	// 验证页面是否有效
	bad := !bytes.Equal([]byte(DbSig), data[:16])
	maxPages := uint64(fileSize / BTreePageSize)
	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxPages)
	bad = bad || !(0 < db.tree.root && db.tree.root < db.page.flushed)
	bad = bad || !(0 < db.free.headPage && db.free.headPage < db.page.flushed)
	bad = bad || !(0 < db.free.tailPage && db.free.tailPage < db.page.flushed)
	if bad {
		return errors.New("bad meta page")
	}
	return nil
}

// updateRoot 更新根页面
func updateRoot(db *KV) error {
	data := saveMeta(db)
	if _, err := syscall.Pwrite(db.fd, data, 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}
