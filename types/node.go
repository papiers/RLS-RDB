package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"db-practice/util"
)

const (
	Header          = 4
	BTreePageSize   = 4096
	BTreeMaxKeySize = 1000
	BTreeMaxValSize = 3000

	BNodeNode = 1 // 中间结点没有val
	BNodeLeaf = 2 // 叶子结点有val
)

// BNode btree 的一个结点
// 节点结构说明
// 1. 节点头部和指针
// | 字段       | 大小              |
// |------------|-------------------|
// | type       | 2 bytes           |
// | nkeys      | 2 bytes           |
// | pointers   | nkeys * 8 bytes   |（仅用于内部节点）
// | offsets    | nkeys * 2 bytes   |
// 2. 键值对格式
// | 字段   | 大小       |
// |--------|------------|
// | klen   | 2 bytes    |
// | vlen   | 2 bytes    |
// | key    | klen bytes |
// | val    | vlen bytes |
// 叶子节点和内部节点使用相同的格式。
type BNode struct {
	data []byte
}

// bType 返回结点的类型
func (b *BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(b.data)
}

// nKeys 返回结点中键的数量
func (b *BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(b.data[2:])
}

// setHeader 设置结点的类型和键的数量
func (b *BNode) setHeader(btype, nKeys uint16) {
	binary.LittleEndian.PutUint16(b.data[0:2], btype)
	binary.LittleEndian.PutUint16(b.data[2:4], nKeys)
}

// getPtr 返回索引为idx的子节点指针值
func (b *BNode) getPtr(idx uint16) uint64 {
	return binary.LittleEndian.Uint64(b.data[Header+8*idx:])
}

// setPtr 设置索引为idx的子节点指针值
func (b *BNode) setPtr(idx uint16, val uint64) {
	binary.LittleEndian.PutUint64(b.data[Header+8*idx:], val)
}

// offsetPos 返回索引为idx的偏移量
func (b *BNode) offsetPos(idx uint16) uint16 {
	// 第一个kv的offset为0
	return Header + 8*b.nKeys() + 2*(idx-1)
}

// getOffset 返回索引为idx的偏移量
func (b *BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(b.data[b.offsetPos(idx):])
}

// setOffset 设置索引为idx的偏移量
func (b *BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(b.data[b.offsetPos(idx):], offset)
}

// kvPos 返回索引为idx的键值对位置
func (b *BNode) kvPos(idx uint16) uint16 {
	return Header + 8*b.nKeys() + 2*b.nKeys() + b.getOffset(idx)
}

// getKey 返回索引为idx的键值
func (b *BNode) getKey(idx uint16) []byte {
	pos := b.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(b.data[pos:])
	return b.data[pos+4:][:kLen]
}

// getVal 返回索引为idx的值
func (b *BNode) getVal(idx uint16) []byte {
	pos := b.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(b.data[pos:])
	vLen := binary.LittleEndian.Uint16(b.data[pos+2:])
	return b.data[pos+4+kLen:][:vLen]
}

// nBytes 返回结点的大小
func (b *BNode) nBytes() uint16 {
	return b.kvPos(b.nKeys())
}

// String 返回结点信息
func (b *BNode) String() string {
	mp := map[string]string{}
	mp["nkeys"] = fmt.Sprintf("%d", b.nKeys())
	sp := strings.Builder{}
	skv := strings.Builder{}
	for i := range b.nKeys() {
		sp.WriteString(fmt.Sprintf("[%d]:%v  ", i, b.getPtr(i)))
		skv.WriteString(fmt.Sprintf("[%d]{%s:%s}  ", i, b.getKey(i), b.getVal(i)))
	}
	mp["pointers"] = sp.String()
	mp["key-value"] = skv.String()
	if b.bType() == BNodeLeaf {
		mp["type"] = "leaf"
		delete(mp, "pointers")
	} else {
		mp["type"] = "node"
	}
	marshal, _ := json.Marshal(mp)
	return string(marshal)
}

// nodeLookupLE 返回小于等于key的最大键的索引 kid[i] <= key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nKeys := node.nKeys()
	found := uint16(0)
	for i := uint16(1); i < nKeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// nodeAppendRange 复制结点信息到新结点 左闭右开区间[,)
func nodeAppendRange(dstNode, srcNode BNode, dst, src, n uint16) {
	if n == 0 {
		return
	}
	// 复制子节点指针
	for i := uint16(0); i < n; i++ {
		dstNode.setPtr(dst+i, srcNode.getPtr(src+i))
	}
	// 复制偏移量
	dstBegin := dstNode.getOffset(dst)
	srcBegin := srcNode.getOffset(src)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + srcNode.getOffset(src+i) - srcBegin
		dstNode.setOffset(dst+i, offset)
	}
	// 复制键值对
	begin := srcNode.kvPos(src)
	end := srcNode.kvPos(src + n)
	copy(dstNode.data[dstNode.kvPos(dst):], srcNode.data[begin:end])
}

// nodeAppendKV 设置键值对到新结点
func nodeAppendKV(node BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// 设置子结点指针
	node.setPtr(idx, ptr)
	// 设置键值对
	pos := node.kvPos(idx)
	binary.LittleEndian.PutUint16(node.data[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node.data[pos+2:], uint16(len(val)))
	copy(node.data[pos+4:], key)
	copy(node.data[pos+4+uint16(len(key)):], val)
	// 设置offset
	node.setOffset(idx+1, node.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// leafInsert 插入键值对到叶子结点
func leafInsert(newNode, oldNode BNode, idx uint16, key []byte, val []byte) {
	newNode.setHeader(BNodeLeaf, oldNode.nKeys()+1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nKeys()-idx)
}

// leafUpdate 更新键值对到叶子结点
func leafUpdate(newNode, oldNode BNode, idx uint16, key []byte, val []byte) {
	newNode.setHeader(BNodeLeaf, oldNode.nKeys())
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nKeys()-(idx+1))
}

// leafDelete 从叶节点中移除一个键值对
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNodeLeaf, old.nKeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nKeys()-(idx+1))
}

// nodeMerge 将 2 个节点合并为 1 个
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.bType(), left.nKeys()+right.nKeys())
	nodeAppendRange(new, left, 0, 0, left.nKeys())
	nodeAppendRange(new, right, left.nKeys(), 0, right.nKeys())
}

func init() {
	nodeMax := Header + 8 + 2 + 4 + BTreeMaxKeySize + BTreeMaxValSize
	util.Assert(BTreePageSize >= nodeMax)
}
