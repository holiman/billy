package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

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
	openCommand = &cli.Command{
		Action: open,
		Name:   "open",
		Usage:  "Open a database, and keep open until ctrl-c.",
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
		openCommand,
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

type dbParams struct {
	path string
	min  uint32
	max  uint32
}

func openDb(ctx *cli.Context) (billy.Database, *dbParams, error) {
	opts := &dbParams{
		path: ctx.String("path"),
		min:  uint32(ctx.Int("min")),
		max:  uint32(ctx.Int("max")),
	}
	db, err := doOpenDb(opts)
	return db, opts, err
}

func doOpenDb(opts *dbParams) (billy.Database, error) {
	db, err := billy.Open(billy.Options{Path: opts.path}, billy.SlotSizePowerOfTwo(opts.min, opts.max), func(key uint64, size uint32, data []byte) {
		var d string
		if len(data) > 100 {
			d = fmt.Sprintf("%q...", data[:100])
		} else {
			d = fmt.Sprintf("%q", data)
		}
		fmt.Fprintf(os.Stderr, "%#08x %s\n", key, d)
	})
	if err == nil {
		fmt.Fprintf(os.Stderr, "Opened %v\n", opts.path)
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
	db, _, err := openDb(ctx)
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
	id, _ := db.Put([]byte(data))
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
		if _, err := enc.Write(data); err != nil {
			return err
		}
		err := enc.Close()
		fmt.Println(out.String())
		return err
	})
}

func doGet(ctx *cli.Context, outputFn func([]byte) error) error {
	db, _, err := openDb(ctx)
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
	db, _, err := openDb(ctx)
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

func startIpc(endpoint string) (net.Listener, error) {
	// Ensure the IPC path exists and remove any previous leftover
	if err := os.MkdirAll(filepath.Dir(endpoint), 0750); err != nil {
		return nil, err
	}
	os.Remove(endpoint)
	l, err := net.Listen("unix", endpoint)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(endpoint, 0600); err != nil {
		return nil, err
	}
	return l, nil
}

// serveCodec handles the request from the socket
// Format:
// 1. PUT string(base64 data) -> string(id)
// 2. GET string(id) -> string(base64 data)
// 3. DEL string(id) -> -
// 4. RST -> - (closes and reopens the database)
func serveCodec(conn net.Conn, db billy.Database, opts *dbParams) {
	in := bufio.NewScanner(conn)
	for in.Scan() {
		var verb string
		line := in.Bytes()
		if len(line) >= 4 {
			verb = string(line[:4])
		}
		switch verb {
		case "PUT ":
			in := bytes.NewReader(line[4:])
			dec := base64.NewDecoder(base64.StdEncoding, in)
			data, err := io.ReadAll(dec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			id, _ := db.Put(data)
			_, _ = conn.Write([]byte(fmt.Sprintf("%#08x\n", id)))
		case "GET ":
			k, ok := big.NewInt(0).SetString(string(line[4:]), 0)
			if !ok {
				fmt.Fprintf(os.Stderr, "failed to parse key")
				continue
			}
			if !k.IsUint64() {
				fmt.Fprintf(os.Stderr, "failed to parse key (oob)")
				continue
			}
			data, err := db.Get(k.Uint64())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			enc := base64.NewEncoder(base64.StdEncoding, conn)
			_, _ = enc.Write(data)
			_ = enc.Close()
			_, _ = conn.Write([]byte("\n"))
		case "DEL ":
			k, ok := big.NewInt(0).SetString(string(line[4:]), 0)
			if !ok {
				fmt.Fprintf(os.Stderr, "failed to parse key")
				continue
			}
			if !k.IsUint64() {
				fmt.Fprintf(os.Stderr, "failed to parse key (oob)")
				continue
			}
			_ = db.Delete(k.Uint64())
		case "RST ":
			// Restart it
			db.Close()
			var err error
			db, err = doOpenDb(opts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
		default:
			fmt.Fprintf(os.Stderr, "bad verb\n")
		}
	}
}

func open(ctx *cli.Context) error {
	db, opts, err := openDb(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	endpoint := filepath.Join(ctx.String("path"), "billy.ipc")
	l, err := startIpc(endpoint)
	if err != nil {
		return err
	}
	defer func() {
		os.Remove(endpoint)
	}()
	go func(l net.Listener, db billy.Database, opts *dbParams) {
		// ServeListener accepts connections on l, serving JSON-RPC on them.
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Err: %v\n", err)
				return
			}
			go serveCodec(conn, db, opts)
		}
	}(l, db, opts)
	abortChan := make(chan os.Signal, 1)
	signal.Notify(abortChan, os.Interrupt)
	<-abortChan
	fmt.Fprintf(os.Stderr, "Shutting down\n")
	return nil
}
