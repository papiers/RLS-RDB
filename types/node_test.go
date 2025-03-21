package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBNode_bType(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:2], uint16(1)) // 设置类型为1
	binary.LittleEndian.PutUint16(data[2:4], uint16(2)) // 设置键的数量为2，但此处不测试这个

	b := &BNode{data: data}
	if bType := b.bType(); bType != 1 {
		t.Errorf("expected bType to be 1, got %d", bType)
	}
}

func TestBNode_nKeys(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:2], uint16(1)) // 设置类型，但此处不测试这个
	binary.LittleEndian.PutUint16(data[2:4], uint16(2)) // 设置键的数量为2

	b := &BNode{data: data}
	if nKeys := b.nKeys(); nKeys != 2 {
		t.Errorf("expected nKeys to be 2, got %d", nKeys)
	}
}

func TestBNode_setHeader(t *testing.T) {
	data := make([]byte, 4)
	b := &BNode{data: data}
	b.setHeader(3, 4) // 设置类型为3，键的数量为4

	if bType := b.bType(); bType != 3 {
		t.Errorf("expected bType to be 3 after setHeader, got %d", bType)
	}

	if nKeys := b.nKeys(); nKeys != 4 {
		t.Errorf("expected nKeys to be 4 after setHeader, got %d", nKeys)
	}
}

func TestBNode_modify(t *testing.T) {
	type BNode struct {
		data []byte
	}
	right := BNode{make([]byte, BTreePageSize)}
	right.data[0] = 1
	right.data = right.data[:10]
	fmt.Println("right.data:", right.data)
	fmt.Println("len(right.data):", len(right.data))
	fmt.Println("cap(right.data):", cap(right.data))
}

// TestLeafInsert 是用于测试 leafInsert
func TestLeafInsert(t *testing.T) {
	// 创建新的BNode和旧的BNode
	newNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode := BNode{data: make([]byte, BTreePageSize)}

	// 设置旧的BNode的键值对数量
	oldNode.setHeader(BNodeLeaf, 1)

	// 测试数据
	key := []byte("testKey")
	val := []byte("testVal")

	// 调用leafInsert函数
	leafInsert(newNode, oldNode, 0, key, val)

	// 验证新的BNode的键值对数量
	if newNode.nKeys() != 2 {
		t.Errorf("Expected nKeys to be 2, got %d", newNode.nKeys())
	}

	// 验证新的BNode的数据
	expectedData := append([]byte{0, 2}, key...)
	expectedData = append(expectedData, val...)
	if !bytes.Equal(newNode.data, expectedData) {
		t.Errorf("Expected data to be %v, got %v", expectedData, newNode.data)
	}
}

func TestLeafInsert_EmptyNode(t *testing.T) {
	oldNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode.setHeader(BNodeLeaf, 0)

	newNode1 := BNode{data: make([]byte, BTreePageSize)}
	key := []byte("testKey")
	val := []byte("testVal")
	leafInsert(newNode1, oldNode, 0, key, val)

	newNode2 := BNode{data: make([]byte, BTreePageSize)}
	key = []byte("tstKey1")
	val = []byte("tstVal1")
	leafInsert(newNode2, newNode1, 0, key, val)

	newNode3 := BNode{data: make([]byte, BTreePageSize)}
	key = []byte("testKey2")
	val = []byte("testVal2")
	leafInsert(newNode3, newNode2, 1, key, val)

	fmt.Println(oldNode.String())
	fmt.Println(newNode1.String())
	fmt.Println(newNode2.String())
	fmt.Println(newNode3.String())
}

func TestLeafInsert_AtEnd(t *testing.T) {
	newNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode.setHeader(BNodeLeaf, 2)

	key1 := []byte("key1")
	val1 := []byte("val1")
	key2 := []byte("key2")
	val2 := []byte("val2")
	key3 := []byte("key3")
	val3 := []byte("val3")

	nodeAppendKV(oldNode, 0, 0, key1, val1)
	nodeAppendKV(oldNode, 1, 0, key2, val2)

	leafInsert(newNode, oldNode, 2, key3, val3)

	if newNode.nKeys() != 3 {
		t.Errorf("Expected nKeys to be 3, got %d", newNode.nKeys())
	}

	expectedData := make([]byte, BTreePageSize)
	newNode.setHeader(BNodeLeaf, 3)
	nodeAppendKV(newNode, 0, 0, key1, val1)
	nodeAppendKV(newNode, 1, 0, key2, val2)
	nodeAppendKV(newNode, 2, 0, key3, val3)

	if !bytes.Equal(newNode.data, expectedData) {
		t.Errorf("Expected data to be %v, got %v", expectedData, newNode.data)
	}
}

func TestLeafInsert1(t *testing.T) {
	// 创建新的BNode和旧的BNode
	newNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode := BNode{data: make([]byte, BTreePageSize)}

	// 设置旧的BNode的键值对数量
	oldNode.setHeader(BNodeLeaf, 1)

	// 测试数据
	key := []byte("testKey")
	val := []byte("testVal")

	// 调用leafInsert函数
	leafInsert(newNode, oldNode, 0, key, val)

	// 验证新的BNode的键值对数量
	if newNode.nKeys() != 2 {
		t.Errorf("Expected nKeys to be 2, got %d", newNode.nKeys())
	}

	// 验证新的BNode的数据
	expectedData := append([]byte{0, 2}, key...)
	expectedData = append(expectedData, val...)
	if !bytes.Equal(newNode.data, expectedData) {
		t.Errorf("Expected data to be %v, got %v", expectedData, newNode.data)
	}
}

func TestLeafInsert_MultipleKeys(t *testing.T) {
	newNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode := BNode{data: make([]byte, BTreePageSize)}
	oldNode.setHeader(BNodeLeaf, 2)

	key1 := []byte("key1")
	val1 := []byte("val1")
	key2 := []byte("key2")
	val2 := []byte("val2")
	key3 := []byte("key3")
	val3 := []byte("val3")

	nodeAppendKV(oldNode, 0, 0, key1, val1)
	nodeAppendKV(oldNode, 1, 0, key2, val2)

	leafInsert(newNode, oldNode, 1, key3, val3)

	if newNode.nKeys() != 3 {
		t.Errorf("Expected nKeys to be 3, got %d", newNode.nKeys())
	}

	expectedData := make([]byte, BTreePageSize)
	newNode.setHeader(BNodeLeaf, 3)
	nodeAppendKV(newNode, 0, 0, key1, val1)
	nodeAppendKV(newNode, 1, 0, key3, val3)
	nodeAppendKV(newNode, 2, 0, key2, val2)

	if !bytes.Equal(newNode.data, expectedData) {
		t.Errorf("Expected data to be %v, got %v", expectedData, newNode.data)
	}
}
