package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBNode_bType(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:2], uint16(1)) // 设置类型为1
	binary.LittleEndian.PutUint16(data[2:4], uint16(2)) // 设置键的数量为2，但此处不测试这个

	b := BNode(data)
	if bType := b.bType(); bType != 1 {
		t.Errorf("expected bType to be 1, got %d", bType)
	}
}

func TestBNode_nKeys(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:2], uint16(1)) // 设置类型，但此处不测试这个
	binary.LittleEndian.PutUint16(data[2:4], uint16(2)) // 设置键的数量为2

	b := BNode(data)
	if nKeys := b.nKeys(); nKeys != 2 {
		t.Errorf("expected nKeys to be 2, got %d", nKeys)
	}
}

func TestBNode_setHeader(t *testing.T) {
	data := make([]byte, 4)
	b := BNode(data)
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
