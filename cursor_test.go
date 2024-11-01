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

	for i := uint16(0); i < math.MaxUint16; i++ {
		err := c.add(utils.GenTestKey(i), utils.GenTestValue(i))
		assert.Nil(t, err)
	}

	tx := c.db.Begin(false)
	defer tx.Commit()

	cursor := tx.Cursor()
	cursor.SeekToFirst()
}
