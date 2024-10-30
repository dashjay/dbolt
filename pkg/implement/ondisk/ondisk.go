package ondisk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/dashjay/dbolt/pkg/btree"
	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/freelist"
	"github.com/dashjay/dbolt/pkg/utils"
	"golang.org/x/sys/unix"
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
}

// `BTree.get`, read a page.
func (db *KV) pageRead(ptr uint64) []byte {
	utils.Assertf(ptr < db.page.flushed+db.page.nappend,
		"pageRead: ptr(%d) should < db.page.flushed(%d)+db.page.nappend(%d)", ptr, db.page.flushed, db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	return db.pageReadFile(ptr)
}

func (db *KV) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/constants.BTREE_PAGE_SIZE
		if ptr < end {
			offset := constants.BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+constants.BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

// `BTree.new`, allocate a new page.
func (db *KV) pageAlloc(node []byte) uint64 {
	utils.Assertf(len(node) == constants.BTREE_PAGE_SIZE, "pageAlloc: invalid node size: %d", len(node))
	if ptr := db.free.PopHead(); ptr != 0 { // try the free list
		utils.Assertf(db.page.updates[ptr] == nil, "pageAlloc: get invalid ptr %d from freelist", ptr)
		db.page.updates[ptr] = node
		return ptr
	}
	return db.pageAppend(node) // append
}

// `FreeList.new`, append a new page.
func (db *KV) pageAppend(node []byte) uint64 {
	utils.Assertf(len(node) == constants.BTREE_PAGE_SIZE, "pageAppend: invalid node size: %d", len(node))
	ptr := db.page.flushed + db.page.nappend
	db.page.nappend++
	utils.Assertf(db.page.updates[ptr] == nil,
		"pageAppend: append new ptr %d but it exists in db.pdage.updates", ptr)
	db.page.updates[ptr] = node
	return ptr
}

// `FreeList.set`, update an existing page.
func (db *KV) pageWrite(ptr uint64) []byte {
	utils.Assertf(ptr < db.page.flushed+db.page.nappend,
		"pageWrite: ptr(%d) should < db.page.flushed(%d)+db.page.nappend(%d)", ptr, db.page.flushed, db.page.nappend)
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	//node := make([]byte, constants.BTREE_PAGE_SIZE)
	node := utils.GetPage()
	copy(node, db.pageReadFile(ptr)) // initialized from the file
	db.page.updates[ptr] = node
	return node
}

// open or create a file and fsync the directory
func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := unix.Openat(dirfd, path.Base(file), flags, 0o644)
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
	db.tree = btree.NewTree(db.pageRead, db.pageAlloc, db.free.PushTail)

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
	return nil
	// error
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

const DB_SIG = "dbolt"

/*
the 1st page stores the root pointer and other auxiliary data.
| sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq |
| 16B |    8B    |     8B    |     8B    |    8B    |     8B    |    8B    |
*/
func loadMeta(db *KV, data []byte) {
	db.tree.SetRoot(binary.LittleEndian.Uint64(data[16:24]))
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])

	headPage := binary.LittleEndian.Uint64(data[32:40])
	headSeq := binary.LittleEndian.Uint64(data[40:48])
	tailPage := binary.LittleEndian.Uint64(data[48:56])
	tailSeq := binary.LittleEndian.Uint64(data[56:64])
	db.free.SetMeta(headPage, headSeq, tailPage, tailSeq)
}

func saveMeta(db *KV) []byte {
	var data [64]byte
	copy(data[:16], DB_SIG)
	binary.LittleEndian.PutUint64(data[16:24], db.tree.Root())
	binary.LittleEndian.PutUint64(data[24:32], db.page.flushed)

	headPage, headSeq, tailPage, tailSeq := db.free.GetMeta()
	binary.LittleEndian.PutUint64(data[32:40], headPage)
	binary.LittleEndian.PutUint64(data[40:48], headSeq)
	binary.LittleEndian.PutUint64(data[48:56], tailPage)
	binary.LittleEndian.PutUint64(data[56:64], tailSeq)
	return data[:]
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize%constants.BTREE_PAGE_SIZE != 0 {
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
		return nil // the meta page will be written in the 1st update
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// initialize the free list
	db.free.SetMaxSeq()
	// verify the page
	bad := !bytes.Equal([]byte(DB_SIG), data[:len(DB_SIG)])
	// pointers are within range?
	maxpages := uint64(fileSize / constants.BTREE_PAGE_SIZE)

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

// update the meta page. it must be atomic.
func updateRoot(db *KV) error {
	// NOTE: atomic?
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// extend the mmap by adding new mappings.
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<15) // double the current address space
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

func updateFile(db *KV) error {
	// 1. Write new nodes.
	if err := writePages(db); err != nil {
		return err
	}
	// 2. `fsync` to enforce the order between 1 and 3.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically.
	if err := updateRoot(db); err != nil {
		return err
	}
	// 4. `fsync` to make everything persistent.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// prepare the free list for the next update
	db.free.SetMaxSeq()
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}
		if err := db.Fsync(db.fd); err != nil {
			return err
		}
		db.failed = false
	}
	// 2-phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		// the on-disk meta page is in an unknown state.
		// mark it to be rewritten on later recovery.
		db.failed = true
		// in-memory states are reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.nappend = 0
		db.page.updates = map[uint64][]byte{}
	}
	return err
}

func writePages(db *KV) error {
	// extend the mmap if needed
	size := (db.page.flushed + db.page.nappend) * constants.BTREE_PAGE_SIZE
	if err := extendMmap(db, int(size)); err != nil {
		return err
	}
	// write data pages to the file
	for ptr, node := range db.page.updates {
		offset := int64(ptr * constants.BTREE_PAGE_SIZE)
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

// KV interfaces
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}
func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}
func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	if !db.tree.Delete(key) {
		return false, nil
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

// Close cleanups all mmap chunks and close the files
func (db *KV) Close() {
	for i := range db.mmap.chunks {
		err := unix.Munmap(db.mmap.chunks[i])
		utils.Assertf(err == nil, "Close: syscall.Munmap error: %s", err)
	}
	err := syscall.Fsync(db.fd)
	utils.Assertf(err == nil, "Close: syscall.Fsync error: %s", err)
	err = syscall.Ftruncate(db.fd, int64(db.page.flushed)*constants.BTREE_PAGE_SIZE)
	utils.Assertf(err == nil, "Close: syscall.Ftruncate error: %s", err)
	err = syscall.Close(db.fd)
	utils.Assertf(err == nil, "Close: syscall.Close(db.fd) error: %s", err)
}
