package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
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

type A struct {
	data []int32
	b    *int32
}

func SetA(a A, val int32) {
	fmt.Printf("SetA Address of the slice: %p\n", unsafe.Pointer(&a.data[0]))
	a.data[0] = val
}
func SetB(a A, val int32) {
	*a.b = val
}
func SetAP(a *A, val int32) {
	a.data[0] = val
}
func TestBNode_setHeader(t *testing.T) {
	a := A{data: make([]int32, 1), b: new(int32)}
	fmt.Printf("Address of the slice: %p\n", unsafe.Pointer(&a.data[0]))
	fmt.Println("init data:", a.data[0], "b:", *a.b)

	// c := a
	// SetAP(&c, 10)

	SetA(a, 100)
	SetB(a, 99)
	fmt.Println("init data:", a.data[0], "b:", *a.b)

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

func TestList(t *testing.T) {
	a := []int{1, 2, 3}
	fmt.Println("原值", a)

	update(a)

	fmt.Println("值传递，但是修改了原值", a)
	c := ap(a)

	fmt.Println("值传递，但是没有append原值", a)
	fmt.Println(c)

	apPtr(&c)
	fmt.Println(c)
}

func update(list []int) {
	list[0] = 10
}

func apPtr(list *[]int) *[]int {
	*list = append(*list, 4)
	return list
}

func ap(list []int) []int {
	list = append(list, 4)
	return list
}

func TestA(t *testing.T) {
	sa := TestStruct{Num: 622047417211854848, Name: "hello"}
	m := StructToMapUseJsonNum(sa)
	fmt.Println("map:", m)
	num, ok := m["num"].(float64)
	fmt.Println("num:", num, "ok:", ok)
}

type TestStruct struct {
	Num  int64  `json:"num"`
	Name string `json:"name"`
}

// StructToMapUseJsonNum 结构体转map，使用jsoniter的UseNumber
func StructToMapUseJsonNum(stInput interface{}) map[string]interface{} {
	if nil == stInput {
		return nil
	}
	ret := make(map[string]interface{})

	jData, err := jsoniter.Marshal(stInput)
	if nil != err {
		return nil
	}
	fmt.Println("jData:", string(jData))
	j := jsoniter.Config{
		UseNumber: true,
	}.Froze()
	err = j.Unmarshal(jData, &ret)
	if nil != err {
		return nil
	}
	return ret
}
