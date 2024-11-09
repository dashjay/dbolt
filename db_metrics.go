package dbolt

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
)

const (
	// dbCounterPageReadCache indicate how many times db read page in dirty page
	dbCounterPageReadCache string = "db-counter-page-read-cache"
	// dbCounterPageReadFromFile indicate how many times db read pages from the file
	dbCounterPageReadFromFile string = "db-counter-page-read-from-file"
	// dbCounterPageAllocFromFreelist indicate how many times db reuse pages from the freelist
	dbCounterPageAllocFromFreelist string = "db-counter-page-alloc-from-freelist"
	// dbCounterPageAllocAppend indicate how many times db append new pages
	dbCounterPageAllocAppend string = "db-counter-page-alloc-append"
	// dbCounterPageUpdateFromCache indicate how many times db update pages in dirty page
	dbCounterPageUpdateFromCache string = "db-counter-page-update-from-cache"
	// dbCounterPageUpdateFromFile indicate how many times db writes pages from the file
	dbCounterPageUpdateFromFile string = "db-counter-page-update-from-file"
	// dbCounterFsync indicate how many times db do fsync
	dbCounterFsync string = "db-counter-fsync"
	// dbCounterPWrite indicate how many times db do pwrite
	dbCounterPWrite string = "db-counter-pwrite"
	// dbCounterFreelistPopHead indicate how many times db push to freelist
	dbCounterFreelistPushTail string = "db-couter-freelist-push-tail"
)

var allMetrics = []string{
	dbCounterPageReadCache,
	dbCounterPageReadFromFile,
	dbCounterPageAllocFromFreelist,
	dbCounterPageAllocAppend,
	dbCounterPageUpdateFromCache,
	dbCounterPageUpdateFromFile,
	dbCounterFsync,
	dbCounterPWrite,
	dbCounterFreelistPushTail,
}

type metrics struct {
	Counters map[string]*Counter
}

func newMetrics() *metrics {
	m := &metrics{
		Counters: make(map[string]*Counter, 20),
	}
	for _, metric := range allMetrics {
		m._initCounter(metric)
	}
	return m
}

type Counter struct {
	v uint64
}

func (c *Counter) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Itoa(int(c.v))), nil
}

func (c *Counter) add(v uint64) {
	atomic.AddUint64(&c.v, v)
}

func (c *Counter) reset() {
	atomic.StoreUint64(&c.v, 0)
}

func (m *metrics) _initCounter(metric string) {
	m.Counters[metric] = new(Counter)
}

func (m *metrics) IncCounterOne(metric string) {
	m.Counters[metric].add(1)
}

func (m *metrics) IncCounter(metric string, value uint64) {
	m.Counters[metric].add(value)
}

func (m *metrics) ResetCounter(metric string) {
	m.Counters[metric].reset()
}

func (m *metrics) ReportMetrics() {
	_ = json.NewEncoder(os.Stdout).Encode(m.Counters)
	fmt.Fprint(os.Stdout, "\n")
}
