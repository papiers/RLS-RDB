package btree

import (
	"encoding/binary"
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
	// free FreeList
	mmap struct {
		// file   int      // 文件大小，可以大于数据库大小
		total  int      // mmap 大小，可以大于文件大小
		chunks [][]byte // 多个 mmap，可以是非连续的
	}
	page struct {
		flushed uint64   // 数据库大小（页数）
		temp    [][]byte // 新分配的页面
		// nFree   int
		// nAppend int
		// updates map[uint64][]byte
	}
	failed bool
}

func (db *KV) pageRead(ptr uint64) BNode {
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

// pageAppend 分配一个新页面。
func (db *KV) pageAppend(node BNode) uint64 {
	util.Assert(len(node.data) <= BTreePageSize)
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.data)
	return ptr
}

// Open 打开数据库。
func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.new = db.pageAppend
	db.tree.del = func(u uint64) {

	}

	var err error

	// open or create the DB file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}

	// get the file size
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}

	// create the initial mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}

	// read the meta page
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

// Get 获取值。
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// Set 设置值。
func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}

// Del 删除值。
func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, updateFile(db)
}

// updateFile 更新文件
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
	return syscall.Fsync(db.fd)
}

// updateOrRevert 更新或回滚
func updateOrRevert(db *KV, meta []byte) error {
	if db.failed {
		db.failed = false
	}
	if err := updateFile(db); err != nil {
		db.failed = true
		loadMeta(db, meta)
		db.page.temp = db.page.temp[:0]
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
	size := (int(db.page.flushed) + (len(db.page.temp))) * BTreePageSize
	if err := extendMmap(db, size); err != nil {
		return err
	}
	offset := int64(db.page.flushed * BTreePageSize)
	// if _, err := uinx.Pwritev(db.fd, db.page.temp, offset); err != nil {
	// 	return err
	// }
	// 将 db.page.temp 视为多个缓冲区（示例按字节切片处理）
	for _, buf := range db.page.temp {
		n, err := syscall.Pwrite(db.fd, buf, offset)
		if err != nil {
			return fmt.Errorf("write failed: %w", err)
		}
		if n != len(buf) {
			return fmt.Errorf("incomplete write: %d/%d bytes", n, len(buf))
		}
		offset += int64(n)
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

// extendMmap 通过添加新映射来扩展 mmap。
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

// | sig | root_ptr | page_used |
// | 16B |    8B    |    8B     |
// saveMeta 保存元数据到内存
func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], DbSig)
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

// loadMeta 从内存加载元数据
func loadMeta(db *KV, data []byte) {
	if string(data[:16]) != DbSig {
		panic("invalid db file")
	}
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

// readRoot 读取根页面
func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 0
	}
	data := db.mmap.chunks[0]
	loadMeta(db, data)
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
