package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"db-practice/util"
)

const (
	TypeBytes = 1
	TypeInt64 = 2
)

type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

type Record struct {
	Cols []string
	Vals []Value
}

// AddStr 添加字符串类型的列
func (r *Record) AddStr(col string, val []byte) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{Type: TypeBytes, Str: val})
	return r
}

// AddInt64 添加int64类型的列
func (r *Record) AddInt64(col string, val int64) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{Type: TypeInt64, I64: val})
	return r
}

// Get 获取列的值
func (r *Record) Get(col string) *Value {
	for i, c := range r.Cols {
		if c == col {
			return &r.Vals[i]
		}
	}
	return nil
}

type TableDef struct {
	Name   string
	Types  []uint32
	Cols   []string
	PKeys  int
	Prefix uint32
}

var TdefTable = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

var TdefMeta = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

type DB struct {
	Path string
	kv   *KV
}

// Get 获取记录
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table %s not found", table)
	}
	return dbGet(db, tdef, rec)
}

// Insert 插入记录
func (db *DB) Insert(table string, rec Record) (bool, error) {
	return false, nil
}

// Update 更新记录
func (db *DB) Update(table string, rec Record) (bool, error) {
	return false, nil
}

// Upsert 插入或更新记录
func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return false, nil
}

// Delete 删除记录
func (db *DB) Delete(table string, rec Record) (bool, error) {
	return false, nil
}

// getTableDef 获取表定义
func getTableDef(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TdefTable, rec)
	util.Assert(err == nil)
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	util.Assert(err == nil)
	return tdef
}

// dbGet 根据主键获取一行记录
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// 根据模式对输入列排序
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	// 编码主键
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}
	// 将值解码为列
	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.PKeys:])
	rec.Cols = tdef.Cols
	rec.Vals = values
	return true, nil
}

// reorderRecord 将记录重新排列到定义的列顺序
func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
	util.Assert(len(rec.Cols) == len(rec.Vals))
	out := make([]Value, len(tdef.Cols))
	for i, c := range tdef.Cols {
		v := rec.Get(c)
		if v == nil {
			continue // 将此列保持未初始化状态
		}
		if v.Type != tdef.Types[i] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		out[i] = *v
	}
	return out, nil
}

// valuesComplete 检查值是否完整
func valuesComplete(tdef *TableDef, vals []Value, n int) error {
	for i, v := range vals {
		if i < n && v.Type == 0 {
			return fmt.Errorf("missing column: %s", tdef.Cols[i])
		} else if i >= n && v.Type != 0 {
			return fmt.Errorf("extra column: %s", tdef.Cols[i])
		}
	}
	return nil
}

// 对记录重新排序并检查是否缺少列。
// n == tdef.PKeys：record 恰好是主键
// n == len（tdef.Cols）：记录包含所有列
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	vals, err := reorderRecord(tdef, rec)
	if err != nil {
		return nil, err
	}
	err = valuesComplete(tdef, vals, n)
	if err != nil {
		return nil, err
	}
	return vals, nil
}

// encodeKey 对于主键
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	// 4 字节表前缀
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	// 顺序保持的编码key
	out = encodeValues(out, vals)
	return out
}

// encodeValues 保序编码，将在下一章中解释
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TypeInt64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)        // flip the sign bit
			binary.BigEndian.PutUint64(buf[:], u) // big endian
			out = append(out, buf[:]...)
		case TypeBytes:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("what?")
		}
	}
	return out
}

// decodeValues 解码保序编码的值
func decodeValues(in []byte, out []Value) {
	for i := range out {
		switch out[i].Type {
		case TypeInt64:
			u := binary.BigEndian.Uint64(in[:8])
			out[i].I64 = int64(u - (1 << 63))
			in = in[8:]
		case TypeBytes:
			idx := bytes.IndexByte(in, 0)
			util.Assert(idx >= 0)
			out[i].Str = unescapeString(in[:idx])
			in = in[idx+1:]
		default:
			panic("what?")
		}
	}
	util.Assert(len(in) == 0)
}

// escapeString 转义 null 字节，以便字符串不包含 null 字节。
func escapeString(in []byte) []byte {
	toEscape := bytes.Count(in, []byte{0}) + bytes.Count(in, []byte{1})
	if toEscape == 0 {
		return in // fast path: no escape
	}

	out := make([]byte, len(in)+toEscape)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			// using 0x01 as the escaping byte:
			// 00 -> 01 01
			// 01 -> 01 02
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

// unescapeString 取消转义 null 字节
func unescapeString(in []byte) []byte {
	if bytes.Count(in, []byte{1}) == 0 {
		return in // fast path: no unescape
	}

	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			// 01 01 -> 00
			// 01 02 -> 01
			i++
			util.Assert(in[i] == 1 || in[i] == 2)
			out = append(out, in[i]-1)
		} else {
			out = append(out, in[i])
		}
	}
	return out
}

// dbUpdate 更新记录
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	return db.kv.Update(&UpdateReq{Key: key, Val: val, Mode: mode})
}
