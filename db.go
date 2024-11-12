package dbolt

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"syscall"

	"github.com/samber/lo"
	"golang.org/x/sys/unix"

	"github.com/dashjay/dbolt/pkg/btree"
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/freelist"
	"github.com/dashjay/dbolt/pkg/utils"
)

type KV struct {
	Path  string
	Fsync func(int) error // overridable; for testing
	// internals
	fd   int
	tree btree.Tree
	free freelist.List
	mmap struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64            // database size in number of pages
		nappend uint64            // number of pages to be appended
		updates map[uint64][]byte // pending updates, including appended pages
	}
	failed bool // Did the last update fail?

	txMu sync.RWMutex

	metrics *metrics
}

// pageRead return the node in cache or from the file.
func (db *KV) pageRead(ptr uint64) []byte {
	utils.Assertf(ptr < db.page.flushed+db.page.nappend,
		"pageRead: ptr(%d) should < db.page.flushed(%d)+db.page.nappend(%d)", ptr, db.page.flushed, db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		db.metrics.IncCounterOne(dbCounterPageReadCache)
		return node // pending update
	}
	return db.pageReadFile(ptr)
}

// pageReadFile read a page from file(mmap).
func (db *KV) pageReadFile(ptr uint64) []byte {
	db.metrics.IncCounterOne(dbCounterPageReadFromFile)
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/constants.BtreePageSize
		if ptr < end {
			offset := constants.BtreePageSize * (ptr - start)
			return chunk[offset : offset+constants.BtreePageSize]
		}
		start = end
	}
	panic("bad ptr")
}

// pageAlloc first try to pop a page ptr from the freelist, if it's empty, append a new page to the end of the file.
func (db *KV) pageAlloc(node []byte) uint64 {
	utils.Assertf(len(node) == constants.BtreePageSize, "pageAlloc: invalid node size: %d", len(node))
	if ptr := db.free.PopHead(); ptr != 0 { // try the free list
		db.metrics.IncCounterOne(dbCounterPageAllocFromFreelist)
		utils.Assertf(db.page.updates[ptr] == nil, "pageAlloc: get invalid ptr %d from freelist", ptr)
		db.page.updates[ptr] = node
		return ptr
	}
	return db.pageAppend(node) // append
}

// pageAppend append a new page to the end of the file.
func (db *KV) pageAppend(node []byte) uint64 {
	utils.Assertf(len(node) == constants.BtreePageSize, "pageAppend: invalid node size: %d", len(node))
	ptr := db.page.flushed + db.page.nappend
	db.page.nappend++
	utils.Assertf(db.page.updates[ptr] == nil,
		"pageAppend: append new ptr %d but it exists in db.pdage.updates", ptr)
	db.page.updates[ptr] = node
	db.metrics.IncCounterOne(dbCounterPageAllocAppend)
	return ptr
}

// pageWrite return the node in cache or from the file.
// the returned node is in memory will be written to the file when the transaction is committed.
func (db *KV) pageWrite(ptr uint64) []byte {
	utils.Assertf(ptr < db.page.flushed+db.page.nappend,
		"pageWrite: ptr(%d) should < db.page.flushed(%d)+db.page.nappend(%d)", ptr, db.page.flushed, db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		db.metrics.IncCounterOne(dbCounterPageUpdateFromCache)
		return node // pending update
	}
	node := utils.GetPage(constants.BtreePageSize)
	copy(node, db.pageReadFile(ptr)) // initialized from the file
	db.page.updates[ptr] = node
	db.metrics.IncCounterOne(dbCounterPageUpdateFromFile)
	return node
}

const dftPerm = 0o644

// createFileSync open or create a file and fsync the directory
func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, dftPerm)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := unix.Openat(dirfd, path.Base(file), flags, dftPerm)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	// fsync the directory
	err = syscall.Fsync(dirfd)
	if err != nil { // may leave an empty file
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	// done
	return fd, nil
}

// open or create a DB file
func (db *KV) Open() error {
	if db.Fsync == nil {
		db.Fsync = syscall.Fsync
	}
	var err error
	db.page.updates = map[uint64][]byte{}
	// free list callbacks
	db.free = freelist.NewFreeList(db.pageRead, db.pageAppend, db.pageWrite)
	// B+tree callbacks
	db.tree = btree.NewTree(db.pageRead, db.pageAlloc, func(u uint64) {
		db.metrics.IncCounterOne(dbCounterFreelistPushTail)
		db.free.PushTail(u)
	})

	// open or create the DB file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}
	// get the file size
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}
	// create the initial mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}
	// read the meta page
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	db.metrics = newMetrics()
	return nil
	// error
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

const dbSig = "dbolt"

/*
the 1st page stores the root pointer and other auxiliary data.
| sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq |
| 16B |    8B    |     8B    |     8B    |    8B    |     8B    |    8B    |
*/
func loadMeta(db *KV, data []byte) {
	db.tree.SetRoot(constants.BinaryAlgorithm.Uint64(data[16:24]))
	db.page.flushed = constants.BinaryAlgorithm.Uint64(data[24:32])

	headPage := constants.BinaryAlgorithm.Uint64(data[32:40])
	headSeq := constants.BinaryAlgorithm.Uint64(data[40:48])
	tailPage := constants.BinaryAlgorithm.Uint64(data[48:56])
	tailSeq := constants.BinaryAlgorithm.Uint64(data[56:64])
	db.free.SetMeta(headPage, headSeq, tailPage, tailSeq)
}

func saveMeta(db *KV) []byte {
	var data [64]byte
	copy(data[:16], dbSig)
	constants.BinaryAlgorithm.PutUint64(data[16:24], db.tree.Root())
	constants.BinaryAlgorithm.PutUint64(data[24:32], db.page.flushed)

	headPage, headSeq, tailPage, tailSeq := db.free.GetMeta()
	constants.BinaryAlgorithm.PutUint64(data[32:40], headPage)
	constants.BinaryAlgorithm.PutUint64(data[40:48], headSeq)
	constants.BinaryAlgorithm.PutUint64(data[48:56], tailPage)
	constants.BinaryAlgorithm.PutUint64(data[56:64], tailSeq)
	return data[:]
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize%constants.BtreePageSize != 0 {
		return errors.New("file is not a multiple of pages")
	}
	if fileSize == 0 { // empty file
		// reserve 2 pages: the meta page and a free list node
		db.page.flushed = 2
		// add an initial node to the free list so it's never empty
		db.free.SetMeta(
			/* default headPage = 1*/ 1,
			/* default headSeq = 1*/ 0,
			/*default tailPage = 1*/ 1,
			/* default tailSeq = 1*/ 0,
		)
		return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// initialize the free list
	db.free.SetMaxSeq()
	// verify the page
	bad := !bytes.Equal([]byte(dbSig), data[:len(dbSig)])
	// pointers are within range?
	maxpages := uint64(fileSize / constants.BtreePageSize)

	headPage, _, tailPage, _ := db.free.GetMeta()
	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxpages)
	bad = bad || !(0 < db.tree.Root() && db.tree.Root() < db.page.flushed)
	bad = bad || !(0 < headPage && headPage < db.page.flushed)
	bad = bad || !(0 < tailPage && tailPage < db.page.flushed)
	if bad {
		return errors.New("bad meta page")
	}
	return nil
}

// updateRoot update the root meta page.
func updateRoot(db *KV) error {
	db.metrics.IncCounterOne(dbCounterPWrite)
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

const maxMmapSize = 64 << 15 // 2 MB

// extendMmap extend the mmap by adding new mappings.
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := lo.Max[int]([]int{db.mmap.total, maxMmapSize}) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := unix.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("extendMmap syscall.Mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// updateFile updates the DB file.
func updateFile(db *KV) error {
	// 1. Write new nodes.
	if err := updatePages(db); err != nil {
		return err
	}
	// 2. `fsync` to enforce the order between 1 and 3.
	db.metrics.IncCounterOne(dbCounterFsync)
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically.
	if err := updateRoot(db); err != nil {
		return err
	}
	// 4. `fsync` to make everything persistent.
	db.metrics.IncCounterOne(dbCounterFsync)
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// prepare the free list for the next update
	db.free.SetMaxSeq()
	return nil
}

// updatePages writes all the dirty pages to the file.
func updatePages(db *KV) error {
	// extend the mmap if needed
	size := (db.page.flushed + db.page.nappend) * constants.BtreePageSize
	if err := extendMmap(db, int(size)); err != nil {
		return err
	}
	// write data pages to the file
	for ptr, node := range db.page.updates {
		offset := int64(ptr * constants.BtreePageSize)
		db.metrics.IncCounterOne(dbCounterPWrite)
		if _, err := unix.Pwrite(db.fd, node, offset); err != nil {
			return err
		}
		utils.PutPage(db.page.updates[ptr])
	}
	// discard in-memory data
	db.page.flushed += db.page.nappend
	db.page.nappend = 0
	db.page.updates = map[uint64][]byte{}
	return nil
}

// Close cleanups all mmap chunks and close the files
func (db *KV) Close() {
	for i := range db.mmap.chunks {
		err := unix.Munmap(db.mmap.chunks[i])
		utils.Assertf(err == nil, "Close: syscall.Munmap error: %s", err)
	}
	err := syscall.Fsync(db.fd)
	utils.Assertf(err == nil, "Close: syscall.Fsync error: %s", err)
	err = syscall.Ftruncate(db.fd, int64(db.page.flushed)*constants.BtreePageSize)
	utils.Assertf(err == nil, "Close: syscall.Ftruncate error: %s", err)
	err = syscall.Close(db.fd)
	utils.Assertf(err == nil, "Close: syscall.Close(db.fd) error: %s", err)
}

func (db *KV) Begin(writable bool) *Tx {
	return NewTx(db, writable)
}

// Open opens the DB.
func Open(fp string) (*KV, error) {
	db := &KV{Path: fp, metrics: newMetrics()}
	err := db.Open()
	if err != nil {
		return nil, fmt.Errorf("open: db.Open() error: %s", err)
	}

	// start an empty write transaction
	tx := db.Begin(true)
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("open: empty _commit transaction error: %s", err)
	}
	return db, err
}

// View is a helper to open a read-only transaction.
func (db *KV) View(fn func(tx *Tx) error) error {
	tx := db.Begin(false)
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return nil
}

// Update is a helper to open a write transaction.
func (db *KV) Update(fn func(tx *Tx) error) error {
	tx := db.Begin(true)
	err := fn(tx)
	if err == nil {
		return tx.Commit()
	}
	err1 := tx.Rollback()
	if err1 != nil {
		return fmt.Errorf("update fn error: %w, rollback error: %s", err, err1)
	}
	return err
}
