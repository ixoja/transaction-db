package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryDB_Set(t *testing.T) {
	const key1 = "key1"
	const value1 = "value1"
	const value2 = "value2"

	t.Run("commit", func(t *testing.T) {
		db := New()
		db.Set(key1, value1)

		db.StartTransaction()
		db.Set(key1, value2)
		db.Commit()

		res := db.Get(key1)
		assert.Equal(t, value2, res)
	})

	t.Run("rollback", func(t *testing.T) {
		key2 := "key2"
		db := New()
		db.Set(key1, value1)
		res := db.Get(key1)
		assert.Equal(t, value1, res)

		db.StartTransaction()
		db.Set(key1, value2)
		db.Set(key2, value1)
		res = db.Get(key1)
		assert.Equal(t, value2, res)
		db.Rollback()

		res = db.Get(key1)
		assert.Equal(t, value1, res)
		res = db.Get(key2)
		assert.Equal(t, "", res)
	})

	t.Run("nested", func(t *testing.T) {
		db := New()
		db.Set(key1, value1)

		db.StartTransaction()
		db.Set(key1, value2)
		res := db.Get(key1)
		assert.Equal(t, value2, res)

		db.StartTransaction()
		res = db.Get(key1)
		assert.Equal(t, value2, res)
		db.Delete(key1)
		db.Commit()

		res = db.Get(key1)
		assert.Equal(t, "", res)
		db.Commit()

		res = db.Get(key1)
		assert.Equal(t, "", res)
	})

	t.Run("nested rollback", func(t *testing.T) {
		db := New()
		db.Set(key1, value1)

		db.StartTransaction()
		db.Set(key1, value2)
		res := db.Get(key1)
		assert.Equal(t, value2, res)

		db.StartTransaction()
		res = db.Get(key1)
		assert.Equal(t, value2, res)
		db.Delete(key1)
		db.Rollback()

		res = db.Get(key1)
		assert.Equal(t, value2, res)
		db.Commit()

		res = db.Get(key1)
		assert.Equal(t, value2, res)
	})
}
