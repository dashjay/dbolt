package dbolt

import (
	"errors"
	"fmt"
	"syscall"
)

type Tx struct {
	db *KV

	oldMeta  []byte
	writable bool
}

func NewTx(db *KV, writable bool) *Tx {
	tx := new(Tx)
	tx.init(db, writable)
	return tx
}

func (t *Tx) init(db *KV, writable bool) {
	if t.db != nil {
		panic("dbolt: init transaction more than once")
	}
	t.db = db
	if writable {
		t.db.txMu.Lock()
	} else {
		t.db.txMu.RLock()
	}

	t.oldMeta = saveMeta(t.db)
	t.writable = writable
}

func (t *Tx) commitOrRollback() error {
	defer func() {
		if t.db != nil {
			if t.writable {
				t.db.txMu.Unlock()
			} else {
				t.db.txMu.RUnlock()
			}
			t.db = nil
		}
	}()

	if t.writable && t.db.failed {
		if err := t.rewriteMetaPage(); err != nil {
			return err
		}
	}

	if t.writable {
		return t.commit()
	}

	return nil
}

func (t *Tx) rewriteMetaPage() error {
	t.db.metrics.IncCounterOne(dbCounterPWrite)
	if _, err := syscall.Pwrite(t.db.fd, t.oldMeta, 0); err != nil {
		return fmt.Errorf("rewrite meta page: %w", err)
	}
	t.db.metrics.IncCounterOne(dbCounterFsync)
	if err := t.db.Fsync(t.db.fd); err != nil {
		return err
	}
	t.db.failed = false
	return nil
}

func (t *Tx) commit() error {
	err := updateFile(t.db)
	if err != nil {
		t.rollback()
	}
	return err
}

func (t *Tx) rollback() {
	// the on-disk meta page is in an unknown state.
	// mark it to be rewritten on later recovery.
	t.db.failed = true
	// in-memory states are reverted immediately to allow reads
	loadMeta(t.db, t.oldMeta)
	// discard temporaries
	t.db.page.nappend = 0
	t.db.page.updates = map[uint64][]byte{}
}

func (t *Tx) Commit() error {
	if t.db == nil {
		return errors.New("dbolt: commit on invalid transaction")
	}
	return t.commitOrRollback()
}

func (t *Tx) Get(key []byte) ([]byte, bool) {
	return t.db.tree.Get(key)
}

func (t *Tx) Set(key []byte, val []byte) error {
	if !t.writable {
		return errors.New("dbolt: transaction not writable")
	}
	t.db.tree.Insert(key, val)
	return nil
}

func (t *Tx) Cursor() *Cursor {
	return &Cursor{tx: t}
}

func (t *Tx) Del(key []byte) (bool, error) {
	if !t.writable {
		return false, errors.New("dbolt: transaction not writable")
	}
	deleted := t.db.tree.Delete(key)
	return deleted, nil
}
