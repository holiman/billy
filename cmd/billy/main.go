package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/holiman/billy"
	"github.com/urfave/cli/v2"
	"math/big"
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
	putCommand = &cli.Command{
		Action:      put,
		Name:        "put",
		Usage:       "Store a blob (raw input)",
		ArgsUsage:   "<data>",
		Description: `Store a blob`,
	}
	put64Command = &cli.Command{
		Action:    put64,
		Name:      "put64",
		Usage:     "Store a blob (b64 input)",
		ArgsUsage: "<base64-encoded data>",
		Flags:     []cli.Flag{},
	}
	getCommand = &cli.Command{
		Action:    get,
		Name:      "get",
		Usage:     "Load a blob (raw output)",
		ArgsUsage: "<key>",
	}
	get64Command = &cli.Command{
		Action:    get64,
		Name:      "get64",
		Usage:     "Load a blob (b64 output)",
		ArgsUsage: "<key>",
	}
	delCommand = &cli.Command{
		Action:    del,
		Name:      "del",
		Usage:     "Delete a blob",
		ArgsUsage: "<key>",
	}
)

func main() {
	app := cli.NewApp()
	app.Usage = "A command-line utility to interact with a billy database"
	app.Copyright = "Copyright 2023 The Billy Authors"
	app.Commands = []*cli.Command{
		// See chaincmd.go:
		putCommand,
		put64Command,
		getCommand,
		get64Command,
		delCommand,
	}
	app.Flags = []cli.Flag{
		pathFlag,
		minFlag,
		maxFlag,
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}

func openDb(ctx *cli.Context) (billy.Database, error) {
	p := ctx.String("path")
	db, err := billy.Open(p, ctx.Int("min"), ctx.Int("max"), func(key uint64, data []byte) {
		var d string
		if len(data) > 100 {
			d = fmt.Sprintf("%q...", data[:100])
		} else {
			d = fmt.Sprintf("%q", data)
		}
		fmt.Fprintf(os.Stderr, "%#08x %s\n", key, d)
	})
	if err == nil {
		fmt.Fprintf(os.Stderr, "Opened %v\n", p)
	}
	return db, err
}

func put(ctx *cli.Context) error {
	data := ctx.Args().First()
	return doPut(ctx, []byte(data))
}

func put64(ctx *cli.Context) error {
	in := strings.NewReader(ctx.Args().First())
	dec := base64.NewDecoder(base64.StdEncoding, in)
	data, err := io.ReadAll(dec)
	if err != nil {
		return err
	}
	return doPut(ctx, data)
}

func doPut(ctx *cli.Context, data []byte) error {
	db, err := openDb(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	_, max := db.Limits()
	if len(data) == 0 {
		return fmt.Errorf("data missing")
	}
	if len(data) > int(max) {
		return fmt.Errorf("data too large, max %d, was %d", max, len(data))
	}
	id := db.Put([]byte(data))
	fmt.Printf("%#08x %d\n", id, id)
	return nil
}

func get(ctx *cli.Context) error {
	return doGet(ctx, func(data []byte) error {
		fmt.Printf("%s\n", data)
		return nil
	})
}

func get64(ctx *cli.Context) error {
	return doGet(ctx, func(data []byte) error {
		out := new(strings.Builder)
		enc := base64.NewEncoder(base64.StdEncoding, out)
		enc.Write(data)
		err := enc.Close()
		fmt.Println(out.String())
		return err
	})
}

func doGet(ctx *cli.Context, outputFn func([]byte) error) error {
	db, err := openDb(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	key := ctx.Args().First()
	k, ok := big.NewInt(0).SetString(key, 0)
	if !ok {
		return fmt.Errorf("failed to parse key from '%s'", key)
	}
	if !k.IsUint64() {
		return fmt.Errorf("failed to parse key from '%s'", key)

	}
	data, err := db.Get(k.Uint64())
	if err != nil {
		return err
	}
	return outputFn(data)
}

func del(ctx *cli.Context) error {
	db, err := openDb(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	key := ctx.Args().First()
	k, ok := big.NewInt(0).SetString(key, 0)
	if !ok {
		return fmt.Errorf("failed to parse key from '%s'", key)
	}
	if !k.IsUint64() {
		return fmt.Errorf("failed to parse key from '%s'", key)
	}
	return db.Delete(k.Uint64())
}
