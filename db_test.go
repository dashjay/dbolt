package dbolt

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/dashjay/dbolt/pkg/bnode"
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/freelist"
	"github.com/dashjay/dbolt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type D struct {
	db  *KV
	ref map[string]string
}

func nofsync(int) error {
	return nil
}

func newD() *D {
	os.Remove("test.db")

	d := &D{db: new(KV)}
	d.ref = map[string]string{}
	d.db.Path = "test.db"
	d.db.Fsync = nofsync // faster
	err := d.db.Open()
	utils.Assert(err == nil, "")
	return d
}

func (d *D) reopen() {
	d.db.Close()
	d.db = &KV{Path: d.db.Path, Fsync: d.db.Fsync}
	err := d.db.Open()
	utils.Assertf(err == nil, "reopen: open db failed: %s", err)
}

func (d *D) dispose() {
	d.db.Close()
	os.Remove("test.db")
}

func (d *D) add(key []byte, val []byte) error {
	tx := d.db.Begin(true)
	utils.Assertf(tx.Set(key, val) == nil, "add error")
	d.ref[string(key)] = string(val)
	return tx.Commit()
}

func (d *D) get(key []byte) ([]byte, bool) {
	tx := d.db.Begin(false)
	defer func() {
		err := tx.Commit()
		utils.Assertf(err == nil, "commit on transaction failed: %s", err)
	}()
	return tx.Get(key)
}

func (d *D) del(key []byte) bool {
	delete(d.ref, string(key))
	tx := d.db.Begin(true)
	defer func() {
		err := tx.Commit()
		utils.Assertf(err == nil, "commit on transaction failed: %s", err)

	}()
	deleted, err := tx.Del(key)
	utils.Assert(err == nil, "")
	return deleted
}

func (d *D) dump() ([]string, []string) {
	keys := []string{}
	vals := []string{}

	var nodeDump func(uint64)
	nodeDump = func(ptr uint64) {
		node := bnode.Node(d.db.tree.DebugGetNode(ptr))
		nkeys := node.KeyCounts()
		if node.Type() == bnode.NodeTypeLeaf {
			for i := uint16(0); i < nkeys; i++ {
				keys = append(keys, string(node.GetKey(i)))
				vals = append(vals, string(node.GetVal(i)))
			}
		} else {
			for i := uint16(0); i < nkeys; i++ {
				ptr := node.GetPtr(i)
				nodeDump(ptr)
			}
		}
	}

	nodeDump(d.db.tree.Root())
	utils.Assert(keys[0] == "", "")
	utils.Assert(vals[0] == "", "")
	return keys[1:], vals[1:]
}

type sortIF struct {
	len  int
	less func(i, j int) bool
	swap func(i, j int)
}

func (self sortIF) Len() int {
	return self.len
}
func (self sortIF) Less(i, j int) bool {
	return self.less(i, j)
}
func (self sortIF) Swap(i, j int) {
	self.swap(i, j)
}

func (d *D) verify(t *testing.T) {
	// KV data
	keys, vals := d.dump()
	// reference data
	rkeys, rvals := []string{}, []string{}
	for k, v := range d.ref {
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
	}
	assert.Equal(t, len(rkeys), len(keys))
	sort.Stable(sortIF{
		len:  len(rkeys),
		less: func(i, j int) bool { return rkeys[i] < rkeys[j] },
		swap: func(i, j int) {
			k, v := rkeys[i], rvals[i]
			rkeys[i], rvals[i] = rkeys[j], rvals[j]
			rkeys[j], rvals[j] = k, v
		},
	})
	// compare with the reference
	assert.Equal(t, rkeys, keys)
	assert.Equal(t, rvals, vals)

	// track visited pages
	pages := make([]uint8, d.db.page.flushed)
	pages[0] = 1
	pages[d.db.tree.Root()] = 1
	// verify node structures
	var nodeVerify func(node bnode.Node)
	nodeVerify = func(node bnode.Node) {
		nkeys := node.KeyCounts()
		utils.Assert(nkeys >= 1, "")
		if node.Type() == bnode.NodeTypeLeaf {
			return
		}
		for i := uint16(0); i < nkeys; i++ {
			ptr := node.GetPtr(i)
			assert.Zero(t, pages[ptr])
			pages[ptr] = 1 // tree node
			key := node.GetKey(i)
			kid := bnode.Node(d.db.tree.DebugGetNode(node.GetPtr(i)))
			assert.Equal(t, key, kid.GetKey(0))
			nodeVerify(kid)
		}
	}

	nodeVerify(d.db.tree.DebugGetNode(d.db.tree.Root()))

	// free list
	list, nodes := freelist.DebugFreelistDump(&d.db.free)
	for _, ptr := range nodes {
		assert.Zero(t, pages[ptr])
		pages[ptr] = 2 // free list node
	}
	for _, ptr := range list {
		assert.Zero(t, pages[ptr])
		pages[ptr] = 3 // free list content
	}

	var flagZeroPages []int
	for ptr, flag := range pages {
		if flag == 0 {
			flagZeroPages = append(flagZeroPages, ptr)
			//assert.NotZero(t, flag) // every page assert accounted for
		} else {
			assert.NotZero(t, flag) // every p
		}
	}
	return
}

func funcTestKVBasic(t *testing.T, reopen bool) {
	c := newD()
	defer c.dispose()

	c.add([]byte("k"), []byte("v"))
	c.verify(t)

	// insert
	for i := 0; i < 25000; i++ {
		key := []byte(fmt.Sprintf("key%d", utils.Murmur32(uint32(i))))
		val := []byte(fmt.Sprintf("vvv%d", utils.Murmur32(uint32(-i))))
		c.add(key, val)
		if i < 2000 {
			c.verify(t)
		}
	}
	c.verify(t)
	if reopen {
		c.reopen()
		c.verify(t)
	}
	t.Log("insertion done")

	// del
	for i := 2000; i < 25000; i++ {
		key := []byte(fmt.Sprintf("key%d", utils.Murmur32(uint32(i))))
		assert.True(t, c.del(key))
	}
	c.verify(t)
	if reopen {
		c.reopen()
		c.verify(t)
	}
	t.Log("deletion done")

	// overwrite
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%d", utils.Murmur32(uint32(i))))
		val := []byte(fmt.Sprintf("vvv%d", utils.Murmur32(uint32(+i))))
		c.add(key, val)
		c.verify(t)
	}

	assert.False(t, c.del([]byte("kk")))

	// remove all
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%d", utils.Murmur32(uint32(i))))
		assert.True(t, c.del(key))
		c.verify(t)
	}
	if reopen {
		c.reopen()
		c.verify(t)
	}

	c.add([]byte("k"), []byte("v2"))
	c.verify(t)
	c.del([]byte("k"))
	c.verify(t)
}

func TestKVBasic(t *testing.T) {
	funcTestKVBasic(t, false)
	funcTestKVBasic(t, true)
}

func fsyncErr(errlist ...int) func(int) error {
	return func(int) error {
		fail := errlist[0]
		errlist = errlist[1:]
		if fail != 0 {
			return fmt.Errorf("fsync error!")
		} else {
			return nil
		}
	}
}

func TestKVFsyncErr(t *testing.T) {
	c := newD()
	defer c.dispose()

	set := c.add
	get := c.get

	err := set([]byte("k"), []byte("1"))
	utils.Assert(err == nil, "")
	val, ok := get([]byte("k"))
	utils.Assert(ok && string(val) == "1", "")

	c.db.Fsync = fsyncErr(1)
	err = set([]byte("k"), []byte("2"))
	utils.Assert(err != nil, "")
	val, ok = get([]byte("k"))
	utils.Assert(ok && string(val) == "1", "")

	c.db.Fsync = nofsync
	err = set([]byte("k"), []byte("3"))
	utils.Assert(err == nil, "")
	val, ok = get([]byte("k"))
	utils.Assert(ok && string(val) == "3", "")

	c.db.Fsync = fsyncErr(0, 1)
	err = set([]byte("k"), []byte("4"))
	utils.Assert(err != nil, "")
	val, ok = get([]byte("k"))
	utils.Assert(ok && string(val) == "3", "")

	c.db.Fsync = nofsync
	err = set([]byte("k"), []byte("5"))
	utils.Assert(err == nil, "")
	val, ok = get([]byte("k"))
	utils.Assert(ok && string(val) == "5", "")

	c.db.Fsync = fsyncErr(0, 1)
	err = set([]byte("k"), []byte("6"))
	utils.Assert(err != nil, "")
	val, ok = get([]byte("k"))
	utils.Assert(ok && string(val) == "5", "")
}

func TestKVRandLength(t *testing.T) {
	c := newD()
	defer c.dispose()

	for i := 0; i < 2000; i++ {
		klen := utils.Murmur32(uint32(2*i+0)) % constants.BTREE_MAX_KEY_SIZE
		vlen := utils.Murmur32(uint32(2*i+1)) % constants.BTREE_MAX_VAL_SIZE
		if klen == 0 {
			continue
		}

		key := make([]byte, klen)
		rand.Read(key)
		val := make([]byte, vlen)
		rand.Read(val)
		c.add(key, val)
		c.verify(t)
	}
}

func TestKVIncLength(t *testing.T) {
	for l := 1; l < constants.BTREE_MAX_KEY_SIZE+constants.BTREE_MAX_VAL_SIZE; l++ {
		c := newD()

		klen := l
		if klen > constants.BTREE_MAX_KEY_SIZE {
			klen = constants.BTREE_MAX_KEY_SIZE
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)

		factor := constants.BTREE_PAGE_SIZE / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			rand.Read(key)
			c.add(key, val)
		}
		c.verify(t)

		c.dispose()
	}
}

func BenchmarkSet(b *testing.B) {
	const keySize, valueSize = 64, 256
	d := newD()
	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	keysRef := make([][]byte, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rand.Read(key)
		rand.Read(value)
		keysRef = append(keysRef, bytes.Clone(key))
		b.StartTimer()
		d.add(key, value)
		b.StopTimer()
	}
}

func BenchmarkOnDisk(b *testing.B) {
	var runOnKeySize = []uint64{64 /*512, constants.BTREE_MAX_KEY_SIZE*/}
	var runOnValueSize = []uint64{256 /*1024, constants.BTREE_MAX_VAL_SIZE*/}

	for _, keySize := range runOnKeySize {
		for _, valueSize := range runOnValueSize {
			d := newD()
			key := make([]byte, keySize)
			value := make([]byte, valueSize)
			keysRef := make([][]byte, 0)
			b.Run(fmt.Sprintf(`add-key-%d-value-%d`, keySize, valueSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rand.Read(key)
					rand.Read(value)
					keysRef = append(keysRef, bytes.Clone(key))
					b.StartTimer()
					d.add(key, value)
					b.StopTimer()
				}
			})

			b.Run(fmt.Sprintf(`get-exists-key-%d-value-%d`, keySize, valueSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := keysRef[i%len(keysRef)]
					_, _ = d.get(key)
				}
			})

			b.Run(fmt.Sprintf(`get-non-exists-key-%d-value-%d`, keySize, valueSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rand.Read(key)
					b.StartTimer()
					_, _ = d.get(key)
					b.StopTimer()
				}
			})

			b.Run(fmt.Sprintf(`delete-exists-key-%d-value-%d`, keySize, valueSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := keysRef[i%len(keysRef)]
					_ = d.del(key)
				}
			})

			b.Run(fmt.Sprintf(`delete-non-exists-key-%d-value-%d`, keySize, valueSize), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rand.Read(key)
					b.StartTimer()
					_ = d.del(key)
					b.StopTimer()
				}
			})
		}
	}
}
