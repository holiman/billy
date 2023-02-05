package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/holiman/billy"
	"github.com/urfave/cli/v2"
)

var (
	pathFlag = &cli.StringFlag{
		Name:  "path",
		Usage: "path to db",
		Value: "./",
	}
	minFlag = &cli.IntFlag{
		Name:  "min",
		Usage: "Min element size",
		Value: 1024,
	}
	maxFlag = &cli.IntFlag{
		Name:  "max",
		Usage: "Max element size",
		Value: 1024 * 1024,
	}
	timeFlag = &cli.DurationFlag{
		Name:  "timeout",
		Usage: "Amount of time to to fuzzing",
		Value: time.Hour * 24 * 265,
	}
)

func main() {
	app := cli.NewApp()
	app.Usage = "A command-line fuzzer"
	app.Copyright = "Copyright 2023 The Billy Authors"
	app.Action = doFuzz
	app.Flags = []cli.Flag{
		pathFlag,
		minFlag,
		maxFlag,
		timeFlag,
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doOpenDb(ctx *cli.Context) (billy.Database, map[uint64][]byte, error) {
	var kvdata = make(map[uint64][]byte)
	db, err := billy.Open(billy.Options{Path: ctx.String("path")},
		billy.SlotSizePowerOfTwo(uint32(ctx.Int("min")), uint32(ctx.Int("max"))),
		func(key uint64, data []byte) {
			cpy := make([]byte, len(data))
			copy(cpy, data)
			kvdata[key] = cpy
		})
	if err == nil {
		fmt.Fprintf(os.Stderr, "Opened %v\n", ctx.String("path"))
	}
	return db, kvdata, err
}

func doFuzz(ctx *cli.Context) error {
	db, kvdata, err := doOpenDb(ctx)
	if err != nil {
		return err
	}
	var (
		min, max  = db.Limits()
		ops       int
		abortChan = make(chan os.Signal, 1)
		timeout   = time.NewTimer(ctx.Duration("timeout"))
		facts     = make(map[uint64]string)
		hasher    = sha256.New()
		lastLog   time.Time
		stopper   = time.NewTicker(4 * time.Second)
	)
	for key, data := range kvdata {
		hasher.Reset()
		facts[key] = hex.EncodeToString(hasher.Sum(data))
	}
	signal.Notify(abortChan, os.Interrupt)
	for {
		op := rand.Intn(3)
		if len(facts) < 1000 && op == 2 {
			// avoid delete if we're too small
			continue
		}
		if len(facts) > 5000 && op == 0 {
			// avoid put if we're too large
			continue
		}
		ops++
		switch op {
		case 0: // PUT
			// Randomize size of data
			l := int(min) + rand.Intn(int(max-min))
			data := make([]byte, l)
			rand.Read(data)
			hasher.Reset()
			sum := hex.EncodeToString(hasher.Sum(data))
			key, err := db.Put(data)
			if err != nil {
				panic(err)
			}
			//fmt.Printf("Wrote %d bytes data to key %d\n", len(data), key)
			facts[key] = sum
		case 1: // GET
			var key uint64
			var want string
			if len(facts) == 0 {
				continue
			}
			for key, want = range facts {
				break
			}
			//fmt.Printf("Checking %d bytes data at key %d\n", len(want), key)
			data, err := db.Get(key)
			if err != nil {
				panic(err)
			}
			// check the data
			hasher.Reset()
			have := hex.EncodeToString(hasher.Sum(data))
			if have != want {
				panic(fmt.Sprintf("key %v\nhave %v\n, want %v\n", key, have, want))
			}
		case 2: // DELETE
			var key uint64
			for key = range facts {
				break
			}
			//fmt.Printf("Deleting data at key %d\n", key)
			if err := db.Delete(key); err != nil {
				panic(err)
			}
			delete(facts, key)
		}
		if time.Since(lastLog) > 8*time.Second {
			fmt.Fprintf(os.Stderr, "%d ops, %d keys active\n", ops, len(facts))
			lastLog = time.Now()
		}
		select {
		case <-abortChan:
			fmt.Fprintf(os.Stderr, "Shutting down\n")
			db.Close()
			return nil
		case <-timeout.C:
			fmt.Fprintf(os.Stderr, "Shutting down\n")
			db.Close()
			return nil
		case <-stopper.C:
			fmt.Fprintf(os.Stderr, "Reopening db, ops %d, keys %d\n", ops, len(facts))
			db.Close()
			db, kvdata, err = doOpenDb(ctx)
			if err != nil {
				return err
			}
			facts = make(map[uint64]string)
			for key, data := range kvdata {
				hasher.Reset()
				facts[key] = hex.EncodeToString(hasher.Sum(data))
			}
		default:
		}
	}
}
