package main

import (
	"fmt"
	"math"
	"os"

	"github.com/dashjay/dbolt"
	"github.com/spf13/cobra"
)

var (
	DBPath string
)

func main() {
	cmd := NewDBCommand()
	err := cmd.Execute()
	if err != nil {
		panic(err)
	}
}

func NewDBCommand() *cobra.Command {
	cmd := new(cobra.Command)
	cmd.Use = "dbolt"
	cmd.AddCommand(
		NewScanDBCommand(),
		NewCreateDBCommand(),
		NewAppendDBCommand(),
		NewDeleteCommand(),
	)
	cmd.PersistentFlags().StringVar(&DBPath, "db-path", "", "path to database")
	return cmd
}

func NewAppendDBCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = "append"
	keyTpl := cmd.Flags().String("key-tpl", "append-key-%010d", "template of key")
	valueTpl := cmd.Flags().String("value-tpl", "append-value-%010d", "template of value")
	entryCount := cmd.Flags().Int64("entry-count", math.MaxUint16, "number of entries to create")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		_, err := os.Stat(DBPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		kv, err := dbolt.Open(DBPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		tx := kv.Begin(true)
		for i := int64(0); i < *entryCount; i++ {
			key := []byte(fmt.Sprintf(*keyTpl, i))
			value := []byte(fmt.Sprintf(*valueTpl, i))
			fmt.Printf("append key/value pair: %s/%s\n", key, value)
			err = tx.Set(key, value)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%10 == 0 {
				err = tx.Commit()
				if err != nil {
					return fmt.Errorf("commit tx error: %s", err)
				}
				tx = kv.Begin(true)
			}
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("commit tx error: %s", err)
		}

		kv.Close()
		return nil
	}
	return cmd
}

func NewDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = "delete"
	keyTpl := cmd.Flags().String("key-tpl", "test-key-%010d", "template of key")
	entryCount := cmd.Flags().Int64("entry-count", math.MaxUint16, "number of entries to create")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		_, err := os.Stat(DBPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		kv, err := dbolt.Open(DBPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		tx := kv.Begin(true)
		for i := int64(0); i < *entryCount; i++ {
			key := []byte(fmt.Sprintf(*keyTpl, i))
			fmt.Printf("delete key: %s\n", key)
			_, err = tx.Del(key)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%10 == 0 {
				err = tx.Commit()
				if err != nil {
					return fmt.Errorf("commit tx error: %s", err)
				}
				tx = kv.Begin(true)
			}
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("commit tx error: %s", err)
		}

		kv.Close()
		return nil
	}
	return cmd
}

func NewCreateDBCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = "create"
	keyTpl := cmd.Flags().String("key-tpl", "test-key-%010d", "template of key")
	valueTpl := cmd.Flags().String("value-tpl", "test-value-%010d", "template of value")
	entryCount := cmd.Flags().Int64("entry-count", math.MaxUint16, "number of entries to create")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		_, err := os.Stat(DBPath)
		if err == nil {
			return fmt.Errorf("create db error, db %s already exists", DBPath)
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("create db error: %s", err)
		}
		kv, err := dbolt.Open(DBPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		tx := kv.Begin(true)
		for i := int64(0); i < *entryCount; i++ {
			key := []byte(fmt.Sprintf(*keyTpl, i))
			value := []byte(fmt.Sprintf(*valueTpl, i))
			fmt.Printf("insert key/value pair: %s/%s\n", key, value)
			err = tx.Set(key, value)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%10 == 0 {
				err = tx.Commit()
				if err != nil {
					return fmt.Errorf("commit tx error: %s", err)
				}
				tx = kv.Begin(true)
			}
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("commit tx error: %s", err)
		}

		kv.Close()
		return nil
	}
	return cmd
}

func NewScanDBCommand() *cobra.Command {
	cmd := new(cobra.Command)
	cmd.Use = "scan"

	handleOne := func(key, value []byte) {
		fmt.Fprintf(os.Stdout, "key: %s, value: %s\n", key, value)
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		kv, err := dbolt.Open(DBPath)
		if err != nil {
			return err
		}
		tx := kv.Begin(false)
		defer tx.Commit()
		cursor := tx.Cursor()
		for key, value := cursor.SeekToFirst(); key != nil; key, value = cursor.Next() {
			handleOne(key, value)
		}
		return nil
	}
	return cmd
}
