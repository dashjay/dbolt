package dbolt

import (
	"github.com/dashjay/dbolt/pkg/btree"
)

type Cursor struct {
	tx *Tx

	cursor *btree.TreeCursor
}

func (c *Cursor) SeekToFirst() ([]byte, []byte) {
	c.cursor = c.tx.db.tree.NewTreeCursor()
	return c.cursor.SeekToFirst()
}

func (c *Cursor) Key() []byte {
	return c.cursor.Key()
}

func (c *Cursor) Value() []byte {
	return c.cursor.Value()
}

func (c *Cursor) Next() ([]byte, []byte) {
	return c.cursor.Next()
}

func (c *Cursor) Seek(key []byte) ([]byte, []byte) {
	return c.cursor.Seek(key)
}
