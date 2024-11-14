package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/pkg/profile"
	"github.com/spf13/cobra"

	"github.com/dashjay/dbolt"
)

var (
	//nolint:gochecknoglobals // global variables in main
	dbPath string
	//nolint:gochecknoglobals // global variables in main
	batchCount int64
)

const (
	defaultBatchCount = 5
	defaultWidth      = 120
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
		NewGetDBCommand(),
	)
	cmd.PersistentFlags().Int64Var(&batchCount, "batch-count", defaultBatchCount, "count for every transaction")
	cmd.PersistentFlags().StringVar(&dbPath, "db-path", "", "path to database")
	return cmd
}

func NewAppendDBCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = "append"
	keyTpl := cmd.Flags().String("key-tpl", "append-key-%010d", "template of key")
	valueTpl := cmd.Flags().String("value-tpl", "append-value-%010d", "template of value")
	entryCount := cmd.Flags().Int64("entry-count", math.MaxUint16, "number of entries to create")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		p := pb.New64(*entryCount)
		p.Start()
		p.SetWidth(defaultWidth)
		defer p.Finish()

		_, err := os.Stat(dbPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		kv, err := dbolt.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		tx := kv.Begin(true)
		for i := int64(0); i < *entryCount; i++ {
			key := []byte(fmt.Sprintf(*keyTpl, i))
			value := []byte(fmt.Sprintf(*valueTpl, i))
			p.SetTemplateString(fmt.Sprintf(`{{"append key/value pair: %s/%s"}} {{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{ speed . }}`, key, value))
			_ = p.Add(1)
			err = tx.Set(key, value)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%batchCount == 0 {
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
		p := pb.New64(*entryCount)
		p.Start()
		p.SetWidth(defaultWidth)
		defer p.Finish()
		_, err := os.Stat(dbPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		kv, err := dbolt.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		tx := kv.Begin(true)
		for i := int64(0); i < *entryCount; i++ {
			key := []byte(fmt.Sprintf(*keyTpl, i))
			_ = p.Add(1)
			p.SetTemplateString(fmt.Sprintf(`{{"delete key: %s"}} {{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{ speed . }}`, key))
			_, err = tx.Del(key)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%batchCount == 0 {
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
		p := pb.New64(*entryCount)
		p.Start()
		p.SetWidth(defaultWidth)
		defer p.Finish()
		_, err := os.Stat(dbPath)
		if err == nil {
			return fmt.Errorf("create db error, db %s already exists", dbPath)
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("create db error: %s", err)
		}
		kv, err := dbolt.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open db error: %s", err)
		}
		tx := kv.Begin(true)
		for i := int64(0); i < *entryCount; i++ {
			key := []byte(fmt.Sprintf(*keyTpl, i))
			value := []byte(fmt.Sprintf(*valueTpl, i))
			p.SetTemplateString(fmt.Sprintf(`{{"insert key/value pair: %s/%s"}} {{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{ speed . }}`, key, value))
			_ = p.Add(1)
			err = tx.Set(key, value)
			if err != nil {
				return fmt.Errorf("write key/value pair error: %s", err)
			}
			if i%batchCount == 0 {
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
	startKey := cmd.Flags().String("start-key", "", "start key")
	endKey := cmd.Flags().String("end-key", "", "end key")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		p := pb.New64(1)
		p.Start()
		p.SetWidth(defaultWidth)
		defer p.Finish()
		i := 0
		handleOne := func(key, value []byte) {
			i++
			p.AddTotal(1)
			p.Add(1)
			p.SetTemplateString(fmt.Sprintf(`{{"scanning key: %s, value: %s, %d scanned"}} {{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{ speed . }}`, key, value, i))
		}
		start := time.Now()
		defer func() {
			fmt.Printf("\n%d key scanned in %.2f sec, %.2f per sec\n", i, time.Since(start).Seconds(), float64(i)/time.Since(start).Seconds())
		}()
		kv, err := dbolt.Open(dbPath)
		if err != nil {
			return err
		}
		tx := kv.Begin(false)
		defer func() {
			err = tx.Commit()
			if err != nil {
				panic(err)
			}
		}()

		var endKeyPtr []byte = nil
		if *endKey != "" {
			endKeyPtr = []byte(*endKey)
		}

		cursor := tx.Cursor()
		for key, value := cursor.Seek([]byte(*startKey)); key != nil; key, value = cursor.Next() {
			handleOne(key, value)
			if endKeyPtr != nil && bytes.Compare(key, endKeyPtr) >= 0 {
				break
			}
		}
		return nil
	}
	return cmd
}

func NewGetDBCommand() *cobra.Command {
	cmd := new(cobra.Command)
	cmd.Use = "get"
	keyTpl := cmd.Flags().String("key-tpl", "test-key-%010d", "template of key")
	entryCount := cmd.Flags().Int64("entry-count", math.MaxUint16, "number of entries to create")
	concurrency := cmd.Flags().Int64("concurrency", 1, "number of concurrent goroutines")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		kv, err := dbolt.Open(dbPath)
		if err != nil {
			return err
		}
		defer kv.Close()
		ch := make(chan struct{}, *concurrency)
		var wg sync.WaitGroup
		p := pb.New64(*entryCount)
		p.Start()
		p.SetTemplateString(`{{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{ speed . }}`)
		p.SetWidth(defaultWidth)
		defer p.Finish()
		starts := make([]int, 0, *concurrency)
		end := make([]int, 0, *concurrency)
		for i := int64(0); i < *concurrency; i++ {
			share := *entryCount / *concurrency
			starts = append(starts, int(i*share))
			end = append(end, int((i+1)*share))
		}
		for i := int64(0); i < *concurrency; i++ {
			ch <- struct{}{}
			wg.Add(1)
			go func(idx int) {
				defer func() {
					<-ch
					wg.Done()
				}()
				for j := starts[idx]; j < end[idx]; j++ {
					err := kv.View(func(tx *dbolt.Tx) error {
						p.Add(1)
						key := fmt.Sprintf(*keyTpl, j)
						_, _ = tx.Get([]byte(key))
						return nil
					})
					if err != nil {
						panic(err)
					}
				}
			}(int(i))
		}
		wg.Wait()
		return nil
	}
	return cmd
}
