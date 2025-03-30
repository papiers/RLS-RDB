package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"db-practice/util"
)

const (
	Header          = 4
	BTreePageSize   = 4096
	BTreeMaxKeySize = 1000
	BTreeMaxValSize = 3000

	BNodeNode = 1 // 中间结点没有val
	BNodeLeaf = 2 // 叶子结点有val

	PointerSize = 8
	offsetSize  = 2
	KeyLenSize  = 2
	ValLenSize  = 2
)

// BNode btree 的一个结点
// 节点结构说明
// 1. 节点头部和指针
// | 字段       | 大小              |
// |------------|-------------------|
// | type       | 2 bytes           |
// | nkeys      | 2 bytes           |
// | pointers   | nkeys * 8 bytes   |（仅用于内部节点，叶子节点没有指针）
// | offsets    | nkeys * 2 bytes   |
// 2. 键值对格式
// | 字段   | 大小       |
// |--------|------------|
// | klen   | 2 bytes    |
// | vlen   | 2 bytes    |
// | key    | klen bytes |
// | val    | vlen bytes |
// 叶子节点和内部节点使用相同的格式。
type BNode []byte

// bType 返回结点的类型
func (b BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(b)
}

// nKeys 返回结点中键的数量
func (b BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(b[2:])
}

// setHeader 设置结点的类型和键的数量
func (b BNode) setHeader(btype, nKeys uint16) {
	binary.LittleEndian.PutUint16(b, btype)
	binary.LittleEndian.PutUint16(b[2:], nKeys)
}

// getPtr 返回索引为idx的子节点指针值
func (b BNode) getPtr(idx uint16) uint64 {
	util.Assert(idx < b.nKeys())
	return binary.LittleEndian.Uint64(b[Header+PointerSize*idx:])
}

// setPtr 设置索引为idx的子节点指针值
func (b BNode) setPtr(idx uint16, val uint64) {
	binary.LittleEndian.PutUint64(b[Header+PointerSize*idx:], val)
}

// offsetPos 返回索引为idx的偏移量存储位置
func (b BNode) offsetPos(idx uint16) uint16 {
	// idx == b.nKeys()时，返回最后一个偏移量位置 记录node大小
	util.Assert(idx > 0 && idx <= b.nKeys())
	// 第一个kv的offset为0
	return Header + PointerSize*b.nKeys() + offsetSize*(idx-1)
}

// getOffsetPos 返回索引为idx的偏移量存储位置
func (b BNode) getOffsetPos(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return b.offsetPos(idx)
}

// getOffset 返回索引为idx的偏移量
func (b BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(b[b.offsetPos(idx):])
}

// setOffset 设置索引为idx的偏移量
func (b BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(b[b.offsetPos(idx):], offset)
}

// kvPos 返回索引为idx的键值对位置
func (b BNode) kvPos(idx uint16) uint16 {
	// idx == b.nKeys()时，返回最后一个偏移量位置 记录node大小
	util.Assert(idx <= b.nKeys())
	return Header + PointerSize*b.nKeys() + offsetSize*b.nKeys() + b.getOffset(idx)
}

// getKey 返回索引为idx的键值
func (b BNode) getKey(idx uint16) []byte {
	util.Assert(idx < b.nKeys())
	pos := b.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(b[pos:])
	return b[pos+KeyLenSize+ValLenSize:][:kLen]
}

// getVal 返回索引为idx的值
func (b BNode) getVal(idx uint16) []byte {
	util.Assert(idx < b.nKeys())
	pos := b.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(b[pos:])
	vLen := binary.LittleEndian.Uint16(b[pos+KeyLenSize:])
	start := pos + KeyLenSize + ValLenSize + kLen
	return b[start:][:vLen]
}

// getKeyLen 返回索引为idx的键的长度
func (b BNode) getKeyLen(idx uint16) uint16 {
	util.Assert(idx < b.nKeys())
	pos := b.kvPos(idx)
	return binary.LittleEndian.Uint16(b[pos:])
}

// getValLen 返回索引为idx的值的长度
func (b BNode) getValLen(idx uint16) uint16 {
	util.Assert(idx < b.nKeys())
	pos := b.kvPos(idx)
	return binary.LittleEndian.Uint16(b[pos+KeyLenSize:])
}

// nBytes 返回结点的大小
func (b BNode) nBytes() uint16 {
	return b.kvPos(b.nKeys())
}

// String 返回结点信息
func (b BNode) String() string {
	nodeS := struct {
		Type         string   `json:"type"`
		NKeys        uint16   `json:"nKeys"`
		Pointers     []uint64 `json:"pointers"`
		Offsets      []uint16 `json:"offsets"`
		OffsetsStart []uint16 `json:"offsets_start"`
		KeyVal       []struct {
			KLen uint16 `json:"klen"`
			VLen uint16 `json:"vlen"`
			Key  string `json:"key"`
			Val  string `json:"val"`
		} `json:"key-value"`
	}{}
	nodeS.NKeys = b.nKeys()
	for i := range b.nKeys() {
		nodeS.Pointers = append(nodeS.Pointers, b.getPtr(i))
		nodeS.Offsets = append(nodeS.Offsets, b.getOffset(i))
		nodeS.OffsetsStart = append(nodeS.OffsetsStart, b.getOffsetPos(i))
		nodeS.KeyVal = append(nodeS.KeyVal, struct {
			KLen uint16 `json:"klen"`
			VLen uint16 `json:"vlen"`
			Key  string `json:"key"`
			Val  string `json:"val"`
		}{
			KLen: b.getKeyLen(i),
			VLen: b.getValLen(i),
			Key:  string(b.getKey(i)),
			Val:  string(b.getVal(i)),
		})
	}
	if b.bType() == BNodeLeaf {
		nodeS.Type = "leaf"
	} else {
		nodeS.Type = "node"
	}
	marshal, _ := json.Marshal(nodeS)
	return string(marshal)
}

// nodeLookupLE 返回小于等于key的最大键的索引 kid[i] <= key
func nodeLookupLE(node BNode, key []byte) uint16 {
	// TODO: 二分查找
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

// nodeAppendRange 复制结点信息到新结点
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
	copy(dstNode[dstNode.kvPos(dst):], srcNode[begin:end])
}

// nodeAppendKV 设置键值对到新结点
func nodeAppendKV(node BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// 设置子结点指针
	node.setPtr(idx, ptr)
	// 设置键值对
	pos := node.kvPos(idx)
	binary.LittleEndian.PutUint16(node[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+KeyLenSize:], uint16(len(val)))
	copy(node[pos+KeyLenSize+ValLenSize:], key)
	copy(node[pos+KeyLenSize+ValLenSize+uint16(len(key)):], val)
	// 设置offset
	node.setOffset(idx+1, node.getOffset(idx)+KeyLenSize+ValLenSize+uint16(len(key)+len(val)))
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
