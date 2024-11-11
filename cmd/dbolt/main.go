package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/dashjay/dbolt"
	"github.com/pkg/profile"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var (
	DBPath     string
	BatchCount int64
)

func main() {
	p := profile.Start(profile.CPUProfile,
		profile.ProfilePath("db-pprof"),
		profile.NoShutdownHook,
	)
	defer p.Stop()
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
	cmd.PersistentFlags().Int64Var(&BatchCount, "batch-count", 10, "count for every transaction")
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
		bar := progressbar.Default(*entryCount, "appending to db")
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
			bar.Add(1)
			key := []byte(fmt.Sprintf(*keyTpl, i))
			value := []byte(fmt.Sprintf(*valueTpl, i))
			bar.Describe(fmt.Sprintf("append key/value pair: %s/%s", key, value))
			err = tx.Set(key, value)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%BatchCount == 0 {
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
		bar := progressbar.Default(*entryCount, "deleting db")
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
			bar.Add(1)
			bar.Describe(fmt.Sprintf("delete key: %s", key))
			_, err = tx.Del(key)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%BatchCount == 0 {
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
		bar := progressbar.Default(*entryCount, "creating db")
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
			bar.Describe(fmt.Sprintf("insert key/value pair: %s/%s", key, value))
			bar.Add(1)
			err = tx.Set(key, value)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%BatchCount == 0 {
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

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		i := 0
		handleOne := func(key, value []byte) {
			i++
			fmt.Printf("key: %s, value: %s, %d scanned\n", key, value, i)
		}
		start := time.Now()

		defer func() {
			fmt.Printf("%d key scanned in %.2f sec, %.2f per sec", i, time.Since(start).Seconds(), float64(i)/time.Since(start).Seconds())
		}()
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
