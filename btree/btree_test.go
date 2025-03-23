package btree

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewC(t *testing.T) {
	c := newC()
	if _, ok := c.get("nonexistent"); ok {
		t.Error("Newly created tree should be empty")
	}
}

func TestAddGetDelete(t *testing.T) {
	c := newC()

	// Test basic add/get
	c.add("key1", "val1")
	if val, ok := c.get("key1"); !ok || val != "val1" {
		t.Errorf("Get failed after add, expected val1 got %s", val)
	}

	// Test delete existing key
	if deleted := c.del("key1"); !deleted {
		t.Error("Delete should return true for existing key")
	}
	if _, ok := c.get("key1"); ok {
		t.Error("Key should be deleted")
	}

	// Test delete non-existing key
	if deleted := c.del("key1"); deleted {
		t.Error("Delete should return false for non-existing key")
	}
}

func TestDuplicateKeys(t *testing.T) {
	c := newC()

	c.add("key", "val1")
	c.add("key", "val2") // Overwrite

	if val, ok := c.get("key"); !ok || val != "val2" {
		t.Errorf("Duplicate key should overwrite value, expected val2 got %s", val)
	}
}

func TestMultipleKeys(t *testing.T) {
	c := newC()

	keys := []string{"b", "a", "c", "d", "e"}
	for i, key := range keys {
		c.add(key, strconv.Itoa(i))
	}

	// Verify all keys exist
	for i, key := range keys {
		if val, ok := c.get(key); !ok || val != strconv.Itoa(i) {
			t.Errorf("Key %s not found or wrong value", key)
		}
	}

	// Delete middle key
	if !c.del("c") {
		t.Error("Failed to delete existing key 'c'")
	}
	if _, ok := c.get("c"); ok {
		t.Error("Deleted key 'c' still exists")
	}

	// Verify remaining keys
	remaining := []string{"a", "b", "d", "e"}
	for _, key := range remaining {
		if _, ok := c.get(key); !ok {
			t.Errorf("Key %s missing after deletion", key)
		}
	}
}

func TestBulkOperations(t *testing.T) {
	c := newC()
	const numKeys = 1000

	// Insert
	for i := 0; i < numKeys; i++ {
		key := strconv.Itoa(i)
		c.add(key, key)
	}

	// Verify
	for i := 0; i < numKeys; i++ {
		key := strconv.Itoa(i)
		if val, ok := c.get(key); !ok || val != key {
			t.Errorf("Key %s missing or wrong value", key)
		}
	}

	// Delete even keys
	for i := 0; i < numKeys; i += 2 {
		key := strconv.Itoa(i)
		if !c.del(key) {
			t.Errorf("Failed to delete key %s", key)
		}
	}

	// Verify after deletion
	for i := 0; i < numKeys; i++ {
		key := strconv.Itoa(i)
		_, ok := c.get(key)
		if i%2 == 0 && ok {
			t.Errorf("Deleted key %s still exists", key)
		}
		if i%2 != 0 && !ok {
			t.Errorf("Existing key %s missing", key)
		}
	}
}

func TestEdgeCases(t *testing.T) {
	c := newC()

	// Empty tree operations
	if _, ok := c.get(""); ok {
		t.Error("Empty key should not exist in new tree")
	}

	assert.Panics(t, func() {
		c.del("")
	}, "assertion failure")

	assert.Panics(t, func() {
		c.add("", "empty")
	}, "assertion failure")
}
