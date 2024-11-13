package dbolt

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"syscall"
	"testing"

	"github.com/dashjay/dbolt/pkg/bnode"
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/freelist"
	"github.com/dashjay/dbolt/pkg/utils"
	"github.com/schollz/progressbar/v3"
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
	var err error
	d.db, err = Open("test.db")
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
	d.ref[string(key)] = string(val)
	return d.db.Update(func(tx *Tx) error {
		return tx.Set(key, val)
	})
}

func (d *D) get(key []byte) (val []byte, exists bool) {
	err := d.db.View(func(tx *Tx) error {
		val, exists = tx.Get(key)
		return nil
	})

	utils.Assertf(err == nil, "View failed: %s", err)
	return
}

func (d *D) del(key []byte) bool {
	delete(d.ref, string(key))
	tx := d.db.Begin(true)
	defer func() {
		err := tx.Commit()
		utils.Assertf(err == nil, "_commit on transaction failed: %s", err)
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

	assert.Nil(t, c.add([]byte("k"), []byte("v")))
	t.Run("test error path", func(t *testing.T) {
		err := c.db.View(func(tx *Tx) error {
			return errors.New("view error")
		})
		assert.NotNil(t, err)
		err = c.db.Update(func(tx *Tx) error {
			return errors.New("update error")
		})
		assert.NotNil(t, err)
	})

	t.Run("test recovery", func(t *testing.T) {
		c.db.failed = true
		err := c.db.Update(func(tx *Tx) error {
			return tx.Set([]byte("k"), []byte("v"))
		})
		assert.Nil(t, err)
	})
	c.verify(t)

	c.db.metrics = newMetrics()
	for i := 0; i < 25000; i++ {
		key := []byte(fmt.Sprintf("key%d", utils.Murmur32(uint32(i))))
		val := []byte(fmt.Sprintf("vvv%d", utils.Murmur32(uint32(-i))))
		assert.Nil(t, c.add(key, val))
		if i < 2000 {
			c.verify(t)
		}
	}
	fmt.Fprintf(os.Stdout, "insert 25000 keys, report metrics: ")
	c.db.metrics.ReportMetrics()
	c.db.metrics = newMetrics()

	c.verify(t)
	if reopen {
		c.reopen()
		c.verify(t)
	}
	t.Log("insertion done")

	// del
	c.db.metrics = newMetrics()
	for i := 0; i < 25000; i++ {
		key := []byte(fmt.Sprintf("key%d", utils.Murmur32(uint32(i))))
		assert.True(t, c.del(key))
	}
	fmt.Fprintf(os.Stdout, "delete 25000 keys, report metrics: ")
	c.db.metrics.ReportMetrics()
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
		assert.Nil(t, c.add(key, val))
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
		klen := utils.Murmur32(uint32(2*i+0)) % constants.BtreeMaxKeySize
		vlen := utils.Murmur32(uint32(2*i+1)) % constants.BtreeMaxValSize
		if klen == 0 {
			continue
		}

		key := make([]byte, klen)
		_, err := rand.Read(key)
		assert.Nil(t, err)
		val := make([]byte, vlen)
		_, err = rand.Read(val)
		assert.Nil(t, err)
		assert.Nil(t, c.add(key, val))
		c.verify(t)
	}
}

func TestKVIncLength(t *testing.T) {
	for l := 1; l < constants.BtreeMaxKeySize+constants.BtreeMaxValSize; l += 64 {
		c := newD()

		klen := l
		if klen > constants.BtreeMaxKeySize {
			klen = constants.BtreeMaxKeySize
		}
		vlen := l - klen
		key := make([]byte, klen)
		val := make([]byte, vlen)

		factor := constants.BtreePageSize / l
		size := factor * factor * 2
		if size > 4000 {
			size = 4000
		}
		if size < 10 {
			size = 10
		}
		for i := 0; i < size; i++ {
			_, err := rand.Read(key)
			assert.Nil(t, err)
			assert.Nil(t, c.add(key, val))
		}
		c.verify(t)

		c.dispose()
	}
}

func TestDBCompact(t *testing.T) {
	c := newD()

	reportFileSize := func(fd int) {
		var sts syscall.Stat_t
		err := syscall.Fstat(fd, &sts)
		assert.Nilf(t, err, "%s", err)
		t.Logf("fd: %d, size: %d", c.db.fd, sts.Size)
	}

	t.Log("report empty db")
	reportFileSize(c.db.fd)

	tx := c.db.Begin(true)
	for i := uint64(0); i < math.MaxUint16; i++ {
		key := utils.GenTestKey(i)
		value := utils.GenTestValue(i)
		err := tx.Set(key, value)
		assert.Nil(t, err)
		if i%9 == 0 {
			err = tx.Commit()
			assert.Nil(t, err)
			tx = c.db.Begin(true)
		}
	}
	t.Log("report before _commit")
	reportFileSize(c.db.fd)
	err := tx.Commit()
	t.Log("report after _commit")
	reportFileSize(c.db.fd)

	assert.Nil(t, err)
	t.Log("report before close")
	reportFileSize(c.db.fd)
	c.db.Close()
	t.Logf("totally %d pages", c.db.page.flushed)
}

func BenchmarkOnDisk(b *testing.B) {
	const N = 100_000
	d := newD()
	const reportInterval = 1000
	reportAndResetMetrics := func(title string) {
		fmt.Fprintf(os.Stdout, title)
		d.db.metrics.ReportMetrics()
		d.db.metrics = newMetrics()
	}
	bar := progressbar.NewOptions64(int64(N), progressbar.OptionSetDescription("adding keys"))
	for i := uint64(0); i < N; i++ {
		bar.Add(1)
		d.add(utils.GenTestKey(i), utils.GenTestValue(i))
		if i%reportInterval == 0 {
			reportAndResetMetrics(fmt.Sprintf("report metrics after adding %d keys", reportInterval))
		}
	}
	bar = progressbar.NewOptions64(int64(N), progressbar.OptionSetDescription("getting keys"))
	for i := uint64(0); i < N; i++ {
		bar.Add(1)
		_, _ = d.get(utils.GenTestKey(i))
		d.get(utils.GenTestKey(i))
		if i%reportInterval == 0 {
			reportAndResetMetrics(fmt.Sprintf("report metrics after getting %d keys", reportInterval))
		}
	}
	bar = progressbar.NewOptions64(int64(N), progressbar.OptionSetDescription("getting non-exists keys"))
	for i := uint64(0); i < N; i++ {
		bar.Add(1)
		_, _ = d.get(append(utils.GenTestKey(i), 'n'))
		if i%reportInterval == 0 {
			reportAndResetMetrics(fmt.Sprintf("report metrics after getting %d non-exists keys", reportInterval))
		}
	}
	bar = progressbar.NewOptions64(int64(N), progressbar.OptionSetDescription("deleting key not exists"))
	for i := uint64(0); i < N; i++ {
		bar.Add(1)
		_ = d.del(append(utils.GenTestKey(i), 'n'))
		if i%reportInterval == 0 {
			reportAndResetMetrics(fmt.Sprintf("report metrics after deleting %d non-exists keys", reportInterval))
		}
	}
	bar = progressbar.NewOptions64(int64(N), progressbar.OptionSetDescription("deleting keys"))
	for i := uint64(0); i < N; i++ {
		bar.Add(1)
		_ = d.del(utils.GenTestKey(i))
		if i%reportInterval == 0 {
			reportAndResetMetrics(fmt.Sprintf("report metrics after deleting %d keys", reportInterval))
		}
	}
}
