package dbolt

import (
	"math"
	"testing"

	"github.com/dashjay/dbolt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestCursor(t *testing.T) {
	c := newD()
	defer c.dispose()

	for i := uint64(0); i < math.MaxUint16; i++ {
		err := c.add(utils.GenTestKey(i), utils.GenTestValue(i))
		assert.Nil(t, err)
	}

	tx := c.db.Begin(false)
	defer tx.Commit()

	cursor := tx.Cursor()
	key, value := cursor.SeekToFirst()
	assert.Equal(t, utils.GenTestKey(0), key)
	assert.Equal(t, utils.GenTestValue(0), value)

	for i := uint64(1); i < math.MaxUint16; i++ {
		key, value = cursor.Next()
		assert.Equal(t, utils.GenTestKey(i), key)
		assert.Equal(t, utils.GenTestValue(i), value)
		assert.Equal(t, utils.GenTestKey(i), cursor.Key())
		assert.Equal(t, utils.GenTestValue(i), cursor.Value())
	}

	key, value = cursor.Seek(utils.GenTestKey(math.MaxUint16 / 2))
	assert.Equal(t, key, utils.GenTestKey(math.MaxUint16/2))
	assert.Equal(t, value, utils.GenTestValue(math.MaxUint16/2))

	for i := uint64(math.MaxUint16/2 + 1); i < math.MaxUint16; i++ {
		key, value = cursor.Next()
		assert.Equal(t, utils.GenTestKey(i), key)
		assert.Equal(t, utils.GenTestValue(i), value)
	}
}
