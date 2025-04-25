package core

import (
	"os"
	"reflect"
	"testing"

	is "github.com/stretchr/testify/require"

	"db-practice/util"
)

type R struct {
	db  DB
	ref map[string][]Record
}

func newR() *R {
	err := os.Remove("r.db")
	util.Assert(err == nil || os.IsNotExist(err))

	r := &R{
		db:  DB{Path: "r.db"},
		ref: map[string][]Record{},
	}
	err = r.db.Open()
	util.Assert(err == nil)
	return r
}

func (r *R) dispose() {
	r.db.Close()
	_ = os.Remove("r.db")
}

func (r *R) create(tdef *TableDef) {
	err := r.db.TableNew(tdef)
	util.Assert(err == nil)
}

func (r *R) findRef(table string, rec Record) int {
	pKeys := r.db.tables[table].PKeys
	records := r.ref[table]
	found := -1
	for i, old := range records {
		if reflect.DeepEqual(old.Vals[:pKeys], rec.Vals[:pKeys]) {
			util.Assert(found == -1)
			found = i
		}
	}
	return found
}

func (r *R) add(table string, rec Record) bool {
	dbReq := DBUpdateReq{Record: rec}
	_, err := r.db.Set(table, &dbReq)
	util.Assert(err == nil)

	records := r.ref[table]
	idx := r.findRef(table, rec)
	util.Assert((idx < 0) == dbReq.Added)
	if idx < 0 {
		r.ref[table] = append(records, rec)
	} else {
		records[idx] = rec
	}
	return dbReq.Added
}

func (r *R) del(table string, rec Record) bool {
	deleted, err := r.db.Delete(table, rec)
	util.Assert(err == nil)

	idx := r.findRef(table, rec)
	if deleted {
		util.Assert(idx >= 0)
		records := r.ref[table]
		copy(records[idx:], records[idx+1:])
		r.ref[table] = records[:len(records)-1]
	} else {
		util.Assert(idx == -1)
	}

	return deleted
}

func (r *R) get(table string, rec *Record) bool {
	ok, err := r.db.Get(table, rec)
	util.Assert(err == nil)
	idx := r.findRef(table, *rec)
	if ok {
		util.Assert(idx >= 0)
		records := r.ref[table]
		util.Assert(reflect.DeepEqual(records[idx], *rec))
	} else {
		util.Assert(idx < 0)
	}
	return ok
}

func TestTableCreate(t *testing.T) {
	r := newR()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TypeInt64, TypeBytes, TypeBytes, TypeInt64},
		PKeys: 2,
	}
	r.create(tdef)

	tdef = &TableDef{
		Name:  "tbl_test2",
		Cols:  []string{"ki1", "ks2"},
		Types: []uint32{TypeInt64, TypeBytes},
		PKeys: 2,
	}
	r.create(tdef)

	{
		rec := (&Record{}).AddStr("key", []byte("next_prefix"))
		ok, err := r.db.Get("@meta", rec)
		util.Assert(ok && err == nil)
		is.Equal(t, []byte{102, 0, 0, 0}, rec.Get("val").Str)
	}
	{
		rec := (&Record{}).AddStr("name", []byte("tbl_test"))
		ok, err := r.db.Get("@table", rec)
		util.Assert(ok && err == nil)
		expected := `{"Name":"tbl_test","Types":[2,1,1,2],"Cols":["ki1","ks2","s1","i2"],"PKeys":2,"Prefix":100}`
		is.Equal(t, expected, string(rec.Get("def").Str))
	}

	r.dispose()
}

func TestTableBasic(t *testing.T) {
	r := newR()
	tdef := &TableDef{
		Name:  "tbl_test",
		Cols:  []string{"ki1", "ks2", "s1", "i2"},
		Types: []uint32{TypeInt64, TypeBytes, TypeBytes, TypeInt64},
		PKeys: 2,
	}
	r.create(tdef)

	rec := Record{}
	rec.AddInt64("ki1", 1).AddStr("ks2", []byte("hello"))
	rec.AddStr("s1", []byte("world")).AddInt64("i2", 2)
	added := r.add("tbl_test", rec)
	is.True(t, added)

	{
		got := Record{}
		got.AddInt64("ki1", 1).AddStr("ks2", []byte("hello"))
		ok := r.get("tbl_test", &got)
		is.True(t, ok)
	}
	{
		got := Record{}
		got.AddInt64("ki1", 1).AddStr("ks2", []byte("hello2"))
		ok := r.get("tbl_test", &got)
		is.False(t, ok)
	}

	rec.Get("s1").Str = []byte("www")
	added = r.add("tbl_test", rec)
	is.False(t, added)

	{
		got := Record{}
		got.AddInt64("ki1", 1).AddStr("ks2", []byte("hello"))
		ok := r.get("tbl_test", &got)
		is.True(t, ok)
	}

	{
		key := Record{}
		key.AddInt64("ki1", 1).AddStr("ks2", []byte("hello2"))
		deleted := r.del("tbl_test", key)
		is.False(t, deleted)

		key.Get("ks2").Str = []byte("hello")
		deleted = r.del("tbl_test", key)
		is.True(t, deleted)
	}

	r.dispose()
}
