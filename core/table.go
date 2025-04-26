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

const TablePrefixMin = 100

// Value 存储表中的值
type Value struct {
	Type uint32 //  tagged union 1: bytes, 2: int64
	I64  int64
	Str  []byte
}

// Record 存储表中的记录
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

// TableDef 表定义
type TableDef struct {
	Name   string
	Types  []uint32
	Cols   []string
	PKeys  int    // 主键个数
	Prefix uint32 // 为不同表自动分配的 B 树键前缀
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
var InternalTables = map[string]*TableDef{
	"@meta":  TdefMeta,
	"@table": TdefTable,
}

// DBUpdateReq 更新请求
type DBUpdateReq struct {
	// in
	Record Record
	Mode   int
	// out
	Updated bool
	Added   bool
}

type DB struct {
	Path string
	// internal
	kv     KV
	tables map[string]*TableDef // cached table schemas
}

// Open 打开数据库
func (db *DB) Open() error {
	db.kv.Path = db.Path
	db.tables = map[string]*TableDef{}
	return db.kv.Open()
}

// Close 关闭数据库
func (db *DB) Close() {
	db.kv.Close()
}

// TableNew 创建新表
func (db *DB) TableNew(tdef *TableDef) error {
	// 0. 健全性检查
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// 1. 检查现有表
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TdefTable, table)
	util.Assert(err == nil)
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// 2. 分配新前缀
	util.Assert(tdef.Prefix == 0)
	tdef.Prefix = TablePrefixMin
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TdefMeta, meta)
	util.Assert(err == nil)
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		util.Assert(tdef.Prefix > TablePrefixMin)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	// 3. 更新下一个前缀
	// FIXME: integer overflow.
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, TdefMeta, &DBUpdateReq{Record: *meta})
	if err != nil {
		return err
	}
	// 4. 存储 schema
	val, err := json.Marshal(tdef)
	util.Assert(err == nil)
	table.AddStr("def", val)
	_, err = dbUpdate(db, TdefTable, &DBUpdateReq{Record: *table})
	return err
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
	return db.Set(table, &DBUpdateReq{Record: rec, Mode: ModeInsertOnly})
}

// Set 添加记录
func (db *DB) Set(table string, dbReq *DBUpdateReq) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, dbReq)
}

// Update 更新记录
func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, &DBUpdateReq{Record: rec, Mode: ModeUpdateOnly})
}

// Upsert 插入或更新记录
func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, &DBUpdateReq{Record: rec, Mode: ModeUpsert})
}

// Delete 删除记录
func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

// Scan 扫描记录
func (db *DB) Scan(table string, req *Scanner) error {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return dbScan(db, tdef, req)
}

// dbDelete 按主键删除记录
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Del(key)
}

// getTableDef 获取表定义
func getTableDef(db *DB, name string) *TableDef {
	if tdef, ok := InternalTables[name]; ok {
		return tdef // 暴露内部表
	}
	tdef := db.tables[name]
	if tdef == nil {
		if tdef = getTableDefDB(db, name); tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

// getTableDefDB 获取表定义
func getTableDefDB(db *DB, name string) *TableDef {
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

// reorderRecord 将Record按TableDef的顺序重新排列
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

// encodeKey 编码主键
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	// 4 字节表前缀
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	// 顺序保持的编码key
	out = encodeValues(out, vals)
	return out
}

// decodeKey 解码主键
func decodeKey(in []byte, out []Value) {
	decodeValues(in[4:], out)
}

// encodeValues 保序编码
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TypeInt64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63) // 翻转符号位
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TypeBytes:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // 以 null 结尾
		default:
			panic("unexpected type")
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
			panic("unexpected type")
		}
	}
	util.Assert(len(in) == 0)
}

// escapeString 转义 null 字节，以便字符串不包含 null 字节
func escapeString(in []byte) []byte {
	toEscape := bytes.Count(in, []byte{0}) + bytes.Count(in, []byte{1})
	if toEscape == 0 {
		return in // 快速判断：无转义
	}

	out := make([]byte, len(in)+toEscape)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			// 使用 0x01 作为转义字节:
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
		return in // 快速判断：无转义
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
func dbUpdate(db *DB, tdef *TableDef, dbReq *DBUpdateReq) (bool, error) {
	values, err := checkRecord(tdef, dbReq.Record, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	req := UpdateReq{Key: key, Val: val, Mode: dbReq.Mode}
	if _, err = db.kv.Update(&req); err != nil {
		return false, err
	}
	dbReq.Added, dbReq.Updated = req.Added, req.Updated
	return req.Updated, nil
}

// tableDefCheck 检查表定义
func tableDefCheck(tdef *TableDef) error {
	bad := tdef.Name == "" || len(tdef.Cols) == 0
	bad = bad || len(tdef.Cols) != len(tdef.Types)
	bad = bad || !(1 <= tdef.PKeys && tdef.PKeys <= len(tdef.Cols))
	if bad {
		return fmt.Errorf("bad table schema: %s", tdef.Name)
	}
	return nil
}

// Scanner 范围查询的迭代器
type Scanner struct {
	// 范围，从 Key1 到 Key2
	Cmp1 int
	Cmp2 int
	Key1 Record
	Key2 Record
	// internal
	tdef   *TableDef
	iter   *BIter // 底层 B 树迭代器
	keyEnd []byte // 编码后的 Key2
}

// Valid 是否在范围内
func (sc *Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return cmpOK(key, sc.Cmp2, sc.keyEnd)
}

// Next 移动底层 B 树迭代器
func (sc *Scanner) Next() {
	util.Assert(sc.Valid())
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

// Deref 返回当前行
func (sc *Scanner) Deref(rec *Record) {
	util.Assert(sc.Valid())
	// 从迭代器中获取 KV
	key, val := sc.iter.Deref()
	// 将 KV 解码为列
	rec.Cols = sc.tdef.Cols
	rec.Vals = rec.Vals[:0]
	for _, v := range sc.tdef.Types {
		rec.Vals = append(rec.Vals, Value{Type: v})
	}
	decodeKey(key, rec.Vals[:sc.tdef.PKeys])
	decodeValues(val, rec.Vals[sc.tdef.PKeys:])
}

func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// 0. 健全性检查
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0:
	case req.Cmp2 > 0 && req.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}
	req.tdef = tdef
	// 1. 根据架构对输入列重新排序
	// TODO: allow prefixes
	values1, err := checkRecord(tdef, req.Key1, tdef.PKeys)
	if err != nil {
		return err
	}
	values2, err := checkRecord(tdef, req.Key2, tdef.PKeys)
	if err != nil {
		return err
	}
	// 2. 对主键进行编码
	keyStart := encodeKey(nil, tdef.Prefix, values1[:tdef.PKeys])
	req.keyEnd = encodeKey(nil, tdef.Prefix, values2[:tdef.PKeys])
	// 3. 搜索开始key
	req.iter = db.kv.tree.Seek(keyStart, req.Cmp1)
	return nil
}
