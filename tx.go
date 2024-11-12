package dbolt

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/dashjay/dbolt/pkg/utils"
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

// init initializes the transaction.
// lock the database.
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

// _commitOrRollback commits the transaction if writable.
func (t *Tx) _commitOrRollback() error {
	// for writable tx, when operations failed,
	// we call _rewriteMetaPage to recover the metaPage to the original state before the transaction started.
	if t.writable && t.db.failed {
		if err := t._rewriteMetaPage(); err != nil {
			return err
		}
	}
	// for writable tx, we call commit to update the changes to the disk.
	if t.writable {
		return t._commit()
	}
	// for readonly we do nothing
	return nil
}

// _rewriteMetaPage rewrites the meta page to the original state before the transaction started.
// mark failed = false.
func (t *Tx) _rewriteMetaPage() error {
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

// _commit commits the transaction to the disk.
func (t *Tx) _commit() error {
	err := updateFile(t.db)
	if err != nil {
		// when we encounter an error, we rollback the transaction.
		t._rollback( /*failed = */ true)
	}
	return err
}

// _rollback the transaction.
// if failed = true, the transaction is marked as failed.
func (t *Tx) _rollback(failed bool) {
	// the on-disk meta page is in an unknown state.
	// mark it to be rewritten on later recovery.
	t.db.failed = failed
	// in-memory states are reverted immediately to allow reads
	loadMeta(t.db, t.oldMeta)
	// discard temporaries
	t.db.page.nappend = 0
	tmpPages := t.db.page.updates
	t.db.page.updates = map[uint64][]byte{}
	for pgID := range tmpPages {
		utils.PutPage(tmpPages[pgID])
	}
}

// _unlock unlocks the transaction.
// the transaction is invalid after this call.
func (t *Tx) _unlock() {
	if t.db != nil {
		if t.writable {
			t.db.txMu.Unlock()
		} else {
			t.db.txMu.RUnlock()
		}
		t.db = nil
	}
}

// Commit commits the transaction.
// if the transaction is writable, the changes are committed to the disk.
// if the transaction is readonly, same as Rollback.
func (t *Tx) Commit() error {
	if t.db == nil {
		return errors.New("dbolt: _commit on invalid transaction")
	}
	defer t._unlock()
	return t._commitOrRollback()
}

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error {
	if t.db == nil {
		return errors.New("dbolt: _rollback on invalid transaction")
	}
	defer t._unlock()
	t._rollback(false)
	return nil
}

// Get returns the value for the given key.
func (t *Tx) Get(key []byte) ([]byte, bool) {
	return t.db.tree.Get(key)
}

// Set sets the value for the given key.
func (t *Tx) Set(key []byte, val []byte) error {
	if !t.writable {
		return errors.New("dbolt: transaction not writable")
	}
	t.db.tree.Insert(key, val)
	return nil
}

// Cursor returns a new cursor on this transaction.
// invalid after this transaction is committed or rolled back.
func (t *Tx) Cursor() *Cursor {
	return &Cursor{tx: t}
}

// Del deletes the value for the given key.
func (t *Tx) Del(key []byte) (bool, error) {
	if !t.writable {
		return false, errors.New("dbolt: transaction not writable")
	}
	deleted := t.db.tree.Delete(key)
	return deleted, nil
}
