package dbolt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dashjay/dbolt/pkg/constants"
	"github.com/dashjay/dbolt/pkg/utils"
)

var (
	ErrNoSuchTable       = errors.New("no such table")
	ErrRecordNotFound    = errors.New("record not found")
	ErrInsertDataExists  = errors.New("insert data already exists")
	ErrCreateTableExists = errors.New("create table already exists")
)

// int64ToBytesHelper can be added to int64 before encoding it to bytes
// it help to keep the order of the int64 in bytes
const int64ToBytesHelper = 1 << 63

const (
	MetaTableName        = "@meta"
	MetaTablePrefix      = 1
	TableInfoTableName   = "@table"
	TableInfoTablePrefix = 2
)

const (
	TypeError = 0
	TypeBytes = 1
	TypeInt64 = 2
)

type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

func NewValueStr(in []byte) *Value {
	return &Value{
		Type: TypeBytes,
		Str:  in,
	}
}

func NewValueInt64(in int64) *Value {
	return &Value{
		Type: TypeInt64,
		I64:  in,
	}
}

func (v *Value) EncodedSize() int {
	if v == nil {
		return 0
	}
	if v.Type == TypeInt64 {
		return constants.Uint64Size
	} else if v.Type == TypeBytes {
		return utils.EscapeStringSize(v.Str)
	}
	utils.Assertf(false, "unknown value type %v", v.Type)
	return 0
}

func encodeValue(buf []byte, v *Value) int {
	utils.Assertf(v != nil, "encode nil value")

	encodedSize := v.EncodedSize()
	utils.Assertf(cap(buf) >= encodedSize, "capacity(%d) of buf is not enough to put value(encodedSize=%d) in", cap(buf), encodedSize)
	n := 0
	switch v.Type {
	case TypeBytes:
		n += copy(buf[0:], utils.EscapeString(v.Str))
	case TypeInt64:
		// to keep the order of the values
		u := uint64(v.I64) + int64ToBytesHelper
		constants.BinaryAlgorithm.PutUint64(buf[0:8], u)
		n += 8
	}
	utils.Assertf(n == encodedSize, "encoded size(%d) mismatched the expected encoded size(%d)", n, encodedSize)
	return n
}

func decodeValue(buf []byte, v *Value) (int, error) {
	switch v.Type {
	case TypeInt64:
		v.I64 = int64(constants.BinaryAlgorithm.Uint64(buf[0:8]) - int64ToBytesHelper)
		return constants.Uint64Size, nil
	case TypeBytes:
		idx := bytes.IndexByte(buf, 0)
		v.Str = utils.UnEscapeString(buf[:idx+1])
		return idx + 1, nil
	default:
		return 0, fmt.Errorf("unknown value type %v", v.Type)
	}
}

type Record struct {
	Cols []string
	Vals []*Value
}

func (rec *Record) AddVal(key string, value *Value) {
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, value)
}

func (rec *Record) Get(key string) *Value {
	idx := utils.Index(rec.Cols, key)
	if idx == -1 {
		return nil
	}
	return rec.Vals[idx]
}

// TableDef defines the table definition
type TableDef struct {
	// Name table name
	Name  string
	Types []uint32 // column types
	Cols  []string // column names
	PKeys int      // the first `PKeys` columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
}

type TableLayer struct {
	db     *KV
	tables map[string]*TableDef

	metaLock sync.Mutex
}

func NewTableLayer(db *KV) *TableLayer {
	tLayer := &TableLayer{
		db:     db,
		tables: make(map[string]*TableDef),
	}
	tLayer.initMetadata()
	return tLayer
}

func (t *TableLayer) initMetadata() {
	// tDefMeta meta table definition
	var tDefMeta = &TableDef{
		Prefix: MetaTablePrefix,
		Name:   MetaTableName,
		Types:  []uint32{TypeBytes, TypeBytes},
		Cols:   []string{"key", "value"},
		PKeys:  1,
	}

	// tDefTable table definition table
	var tDefTable = &TableDef{
		Prefix: TableInfoTablePrefix,
		Name:   TableInfoTableName,
		Types:  []uint32{TypeBytes, TypeBytes},
		Cols:   []string{"name", "def"},
		PKeys:  1,
	}
	t.tables[MetaTableName] = tDefMeta
	t.tables[TableInfoTableName] = tDefTable

	var rec = new(Record)
	rec.AddVal("key", NewValueStr([]byte(constants.MetaKeyNextPrefix)))
	err := t.Get(MetaTableName, rec)
	if err != nil {
		if !errors.Is(err, ErrRecordNotFound) {
			panic(err)
		}
	}

	var buf = make([]byte, constants.Uint32Size)
	constants.BinaryAlgorithm.PutUint32(buf, constants.MinTablePrefix)
	rec.AddVal("value", NewValueStr(buf))
	err = t.Insert(MetaTableName, rec)
	if err != nil {
		panic(err)
	}
}

func checkPrimaryKey(tDef *TableDef, rec *Record) error {
	utils.Assertf(len(rec.Cols) == len(rec.Vals), "len(cols)=%d and len(vals)=%d mismatch", len(rec.Cols), len(rec.Vals))

	if len(rec.Cols) < tDef.PKeys {
		return fmt.Errorf("primary need %d col, but has %d col", tDef.PKeys, len(rec.Cols))
	}
	if len(rec.Vals) < tDef.PKeys {
		return fmt.Errorf("primary need %d col, but has %d col", tDef.PKeys, len(rec.Cols))
	}
	for i := 0; i < tDef.PKeys; i++ {
		if rec.Cols[i] != tDef.Cols[i] {
			return fmt.Errorf("column %d name mismatch %s != %s", i, rec.Cols[i], tDef.Cols[i])
		}
		if rec.Vals[i] == nil {
			return fmt.Errorf("column %d value is nil", i)
		}
	}
	return nil
}

// encodePrimaryKey encode a primary key of the record
func encodePrimaryKey(prefix uint32, record *Record, n int) []byte {
	var bufSize = 4
	for _, val := range record.Vals[:n] {
		bufSize += val.EncodedSize()
	}
	var buf = make([]byte, bufSize)
	constants.BinaryAlgorithm.PutUint32(buf[:4], prefix)
	encodeValues(buf[4:], record.Vals[:n])
	return buf
}

func encodeValues(buf []byte, out []*Value) int {
	needSize := 0
	for _, val := range out {
		needSize += val.EncodedSize()
	}
	utils.Assertf(cap(buf) >= needSize, "capacity(%d) of buf is not enough to put all values(size=%d) in", cap(buf), needSize)
	for _, val := range out {
		n := encodeValue(buf, val)
		buf = buf[n:]
	}
	return needSize
}

func decodeValues(buf []byte, out []*Value) error {
	for idx := range out {
		n, err := decodeValue(buf, out[idx])
		if err != nil {
			return fmt.Errorf("decodeValues index(%d) error: %s", idx, err)
		}
		buf = buf[n:]
	}
	return nil
}

// getTableDef get the table definition from the database
func (t *TableLayer) getTableDef(name string) (*TableDef, error) {
	tDef, tblExists := t.tables[name]
	if tblExists {
		return tDef, nil
	}
	rec := new(Record)
	rec.AddVal("name", NewValueStr([]byte(name)))
	rec.AddVal("def", nil)
	err := t.Get(TableInfoTableName, rec)
	if err != nil {
		if !errors.Is(err, ErrRecordNotFound) {
			return nil, ErrNoSuchTable
		}
		return nil, err
	}
	tDef = new(TableDef)
	err = json.Unmarshal(rec.Get("def").Str, tDef)
	t.tables[name] = tDef
	return tDef, err
}

// Get a single row by the primary key
// rec.Cols[:tDef.PKeys] is the primary key, Get needs all primary keys
func (t *TableLayer) Get(table string, rec *Record) error {
	// get table info first
	tDef, err := t.getTableDef(table)
	if err != nil {
		return err
	}
	err = checkPrimaryKey(tDef, rec)
	if err != nil {
		return err
	}

	key := encodePrimaryKey(tDef.Prefix, rec, tDef.PKeys)
	values := make([]*Value, len(tDef.Cols)-tDef.PKeys)
	for i, typ := range tDef.Types[tDef.PKeys:] {
		values[i] = new(Value)
		values[i].Type = typ
	}
	err = t.db.View(func(tx *Tx) error {
		val, exists := tx.Get(key)
		if !exists {
			return ErrRecordNotFound
		}
		return decodeValues(val, values)
	})
	if err != nil {
		return err
	}

	rec.Cols = append(rec.Cols[:tDef.PKeys], tDef.Cols[tDef.PKeys:]...)
	rec.Vals = append(rec.Vals[:tDef.PKeys], values...)
	return nil
}

func (t *TableLayer) Update(table string, rec *Record) error {
	// get table info first
	tDef, err := t.getTableDef(table)
	if err != nil {
		return err
	}
	err = checkPrimaryKey(tDef, rec)
	if err != nil {
		return err
	}
	key := encodePrimaryKey(tDef.Prefix, rec, tDef.PKeys)
	err = t.db.View(func(tx *Tx) error {
		_, exists := tx.Get(key)
		if !exists {
			return ErrRecordNotFound
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = t.db.Update(func(tx *Tx) error {
		_, exists := tx.Get(key)
		if !exists {
			return ErrRecordNotFound
		}
		buf := utils.GetPage(constants.BtreePageSize)
		defer utils.PutPage(buf)
		n := encodeValues(buf, rec.Vals[tDef.PKeys:])
		return tx.Set(key, buf[:n])
	})
	if err != nil {
		return err
	}
	return nil
}

func (t *TableLayer) Insert(table string, rec *Record) error {
	tDef, err := t.getTableDef(table)
	if err != nil {
		return err
	}
	err = checkPrimaryKey(tDef, rec)
	if err != nil {
		return err
	}
	key := encodePrimaryKey(tDef.Prefix, rec, tDef.PKeys)
	err = t.db.View(func(tx *Tx) error {
		_, exists := tx.Get(key)
		if exists {
			return ErrInsertDataExists
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("check before insert error: %s, table: %s, primary key: %s", err, table, key)
	}
	return t.db.Update(func(tx *Tx) error {
		_, exists := tx.Get(key)
		if exists {
			return ErrInsertDataExists
		}
		buf := utils.GetPage(constants.BtreePageSize)
		defer utils.PutPage(buf)
		n := encodeValues(buf, rec.Vals[tDef.PKeys:])
		return tx.Set(key, buf[:n])
	})
}

func (t *TableLayer) Upsert(table string, rec *Record) (insert bool, err error) {
	err = t.Insert(table, rec)
	if err == nil {
		return true, nil
	} else {
		if errors.Is(err, ErrInsertDataExists) {
			return false, t.Update(table, rec)
		}
		return false, err
	}
}

func (t *TableLayer) Delete(table string, rec *Record) error {
	// get table info first
	tDef, err := t.getTableDef(table)
	if err != nil {
		return err
	}
	err = checkPrimaryKey(tDef, rec)
	if err != nil {
		return err
	}
	key := encodePrimaryKey(tDef.Prefix, rec, tDef.PKeys)
	var deleted bool
	err = t.db.Update(func(tx *Tx) error {
		deleted, err = tx.Del(key)
		return err
	})
	if !deleted {
		return ErrRecordNotFound
	}
	return err
}

func tableDefCheck(def *TableDef) error {
	if def == nil {
		return fmt.Errorf("table definition is nil")
	}
	if def.Name == "" || strings.HasPrefix(def.Name, "@") {
		return fmt.Errorf("table name %s is invalid", def.Name)
	}
	if len(def.Cols) != len(def.Types) {
		return fmt.Errorf("table definition cols and types mismatch, cols(%d) != types(%d)", len(def.Cols), len(def.Types))
	}
	if def.PKeys <= 0 || def.PKeys > len(def.Cols) {
		return fmt.Errorf("table definition primary key(%d) is invalid", def.PKeys)
	}

	if def.Prefix != 0 {
		return fmt.Errorf("table definition prefix(%d) can not be specified", def.PKeys)
	}
	return nil
}

func (t *TableLayer) nextPrefix() (uint32, error) {
	t.metaLock.Lock()
	defer t.metaLock.Unlock()
	var prefix uint32
	var rec = new(Record)
	rec.AddVal("key", NewValueStr([]byte(constants.MetaKeyNextPrefix)))
	err := t.Get(MetaTableName, rec)
	if err != nil {
		if !errors.Is(err, ErrRecordNotFound) {
			return 0, err
		} else {
			return 0, errors.New("init meta error next_prefix not exists")
		}
	}
	prefix = constants.BinaryAlgorithm.Uint32(rec.Get("value").Str)
	var buf = make([]byte, constants.Uint32Size)
	constants.BinaryAlgorithm.PutUint32(buf, prefix+1)
	var updatedRec = new(Record)
	updatedRec.AddVal("key", NewValueStr([]byte(constants.MetaKeyNextPrefix)))
	updatedRec.AddVal("value", NewValueStr(buf))
	err = t.Update(MetaTableName, updatedRec)
	if err != nil {
		return 0, err
	}
	return prefix, nil
}

func (t *TableLayer) CreateTable(tDef *TableDef) error {
	if err := tableDefCheck(tDef); err != nil {
		return err
	}
	tbl := new(Record)
	tbl.AddVal("name", NewValueStr([]byte(tDef.Name)))
	err := t.Get(TableInfoTableName, tbl)
	if err != nil {
		if !errors.Is(err, ErrRecordNotFound) {
			return err
		}
		// table dose not exists
	} else {
		return ErrCreateTableExists
	}
	prefix, err := t.nextPrefix()
	if err != nil {
		return err
	}
	tDef.Prefix = prefix
	val, err := json.Marshal(tDef)
	if err != nil {
		return err
	}
	tbl.AddVal("def", NewValueStr(val))
	return t.Insert(TableInfoTableName, tbl)
}
