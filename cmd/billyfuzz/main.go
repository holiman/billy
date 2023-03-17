package main

import (
	crand "crypto/rand"
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

const verbose = false

func doOpenDb(ctx *cli.Context, onData billy.OnDataFn) (billy.Database, error) {
	db, err := billy.Open(billy.Options{Path: ctx.String("path")},
		billy.SlotSizePowerOfTwo(uint32(ctx.Int("min")), uint32(ctx.Int("max"))),
		onData)
	if err == nil {
		fmt.Fprintf(os.Stderr, "Opened %v\n", ctx.String("path"))
	}
	return db, err
}

func doFuzz(ctx *cli.Context) error {
	var (
		hasher = sha256.New()
		hashes = make(map[uint64]string)
		onData = func(key uint64, size uint32, data []byte) {
			if verbose {
				fmt.Printf("init key %x val %x\n", key, data[:20])
			}
			hasher.Reset()
			hashes[key] = hex.EncodeToString(hasher.Sum(data))
		}
		db, err = doOpenDb(ctx, onData)
	)
	if err != nil {
		return err
	}
	var (
		ops       int
		lastLog   time.Time
		min, max  = db.Limits()
		abortChan = make(chan os.Signal, 1)
		timeout   = time.NewTimer(ctx.Duration("timeout"))
		stopper   = time.NewTicker(4 * time.Second) // Close every 4 seconds
	)
	max = max - 4 // Adjust for item header size.
	signal.Notify(abortChan, os.Interrupt)
	for {
		op := rand.Intn(3)
		if len(hashes) < 1000 && op == 2 {
			// avoid delete if we're too small
			continue
		}
		if len(hashes) > 5000 && op == 0 {
			// avoid put if we're too large
			continue
		}
		ops++
		switch op {
		case 0: // PUT
			// Randomize size of data
			l := int(min) + rand.Intn(int(max-min))
			data := make([]byte, l)
			_, _ = crand.Read(data)
			hasher.Reset()
			sum := hex.EncodeToString(hasher.Sum(data))
			key, err := db.Put(data)
			if err != nil {
				panic(err)
			}
			//fmt.Printf("Wrote %d bytes data to key %d\n", len(data), key)
			hashes[key] = sum
		case 1: // GET
			var key uint64
			var want string
			if len(hashes) == 0 {
				continue
			}
			for key, want = range hashes {
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
			for key = range hashes {
				break
			}
			//fmt.Printf("Deleting data at key %d\n", key)
			if err := db.Delete(key); err != nil {
				panic(err)
			}
			delete(hashes, key)
		}
		if time.Since(lastLog) > 8*time.Second {
			fmt.Fprintf(os.Stderr, "%d ops, %d keys active\n", ops, len(hashes))
			lastLog = time.Now()
		}
		select {
		case <-abortChan:
			fmt.Fprintf(os.Stderr, "Aborted, shutting down\n")
			db.Close()
			return nil
		case <-timeout.C:
			fmt.Fprintf(os.Stderr, "Timeout, shutting down\n")
			db.Close()
			return nil
		case <-stopper.C:
			fmt.Fprintf(os.Stderr, "Reopening db, ops %d, keys %d\n", ops, len(hashes))
			db.Close()
			for k := range hashes {
				delete(hashes, k)
			}
			db, err = doOpenDb(ctx, onData)
			if err != nil {
				return err
			}
		default:
		}
	}
}
