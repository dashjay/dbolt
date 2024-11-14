package dbolt_test

import (
	"path/filepath"
	"testing"

	"github.com/dashjay/dbolt"
	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	tmpDir := t.TempDir()
	fp := filepath.Join(tmpDir, "test.db")
	kv, err := dbolt.Open(fp)
	assert.NoError(t, err)
	defer kv.Close()

	// insert
	val1 := []byte("value1")
	tableLayer := dbolt.NewTableLayer(kv)
	var r = new(dbolt.Record)
	r.AddVal("key", dbolt.NewValueStr([]byte("key1")))
	r.AddVal("value", dbolt.NewValueStr(val1))
	err = tableLayer.Insert(dbolt.MetaTableName, r)
	assert.NoError(t, err)

	// get
	r = new(dbolt.Record)
	r.AddVal("key", dbolt.NewValueStr([]byte("key1")))
	err = tableLayer.Get(dbolt.MetaTableName, r)
	assert.NoError(t, err)
	val := r.Get("value")
	assert.Equal(t, val1, val.Str)

	// update
	val2 := []byte("value2")
	r = new(dbolt.Record)
	r.AddVal("key", dbolt.NewValueStr([]byte("key1")))
	r.AddVal("value", dbolt.NewValueStr(val2))
	err = tableLayer.Update(dbolt.MetaTableName, r)
	assert.NoError(t, err)

	// get again
	r = new(dbolt.Record)
	r.AddVal("key", dbolt.NewValueStr([]byte("key1")))
	err = tableLayer.Get(dbolt.MetaTableName, r)
	assert.NoError(t, err)
	val = r.Get("value")
	assert.Equal(t, val2, val.Str)

	// create new table
	const myTableName = "my_table"
	tbl := dbolt.TableDef{
		Name:  myTableName,
		Types: []uint32{dbolt.TypeInt64, dbolt.TypeBytes, dbolt.TypeBytes, dbolt.TypeBytes},
		Cols:  []string{"id", "username", "password", "email"},
		PKeys: 1,
	}
	err = tableLayer.CreateTable(&tbl)
	assert.Nil(t, err)

	// insert data to new table
	// INSERT INTO my_table (id, username, password, email) VALUES (1, 'john', 'password1', 'abc@123.com')
	r = new(dbolt.Record)
	r.AddVal("id", dbolt.NewValueInt64(100))
	r.AddVal("username", dbolt.NewValueStr([]byte("john")))
	r.AddVal("password", dbolt.NewValueStr([]byte("password1")))
	r.AddVal("email", dbolt.NewValueStr([]byte("abc@123.com")))
	err = tableLayer.Insert(myTableName, r)
	assert.Nil(t, err)

	// get data from new table
	// SELECT * FROM my_table WHERE id = 100
	r = new(dbolt.Record)
	r.AddVal("id", dbolt.NewValueInt64(100))
	err = tableLayer.Get(myTableName, r)
	assert.NoError(t, err)
	assert.Equal(t, []byte("john"), r.Get("username").Str)
	assert.Equal(t, []byte("password1"), r.Get("password").Str)
	assert.Equal(t, []byte("abc@123.com"), r.Get("email").Str)

	// DELETE FROM my_table WHERE id = 100
	r = new(dbolt.Record)
	r.AddVal("id", dbolt.NewValueInt64(100))
	err = tableLayer.Delete(myTableName, r)
	assert.Nil(t, err)

	// get data from table again (not exists)
	r = new(dbolt.Record)
	r.AddVal("id", dbolt.NewValueInt64(100))
	err = tableLayer.Get(myTableName, r)
	assert.Equal(t, dbolt.ErrRecordNotFound, err)
}
