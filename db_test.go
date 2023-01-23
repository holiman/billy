// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGrowFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "testgrow.temp")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(filename)
	defer f.Close()

	f.Seek(55, 0)
	if _, err := f.Write([]byte("test")); err != nil {
		t.Fatal(err)
	}
	if finfo, err := os.Stat(filename); err != nil {
		t.Fatal(err)
	} else if have, want := finfo.Size(), int64(59); have != want {
		t.Fatalf("have %d want %d", have, want)
	}
}

func TestGrowFile2(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "testgrow2.temp")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(filename)
	defer f.Close()

	f.WriteAt([]byte("test"), 55)
	if _, err := f.Write([]byte("test")); err != nil {
		t.Fatal(err)
	}
	if finfo, err := os.Stat(filename); err != nil {
		t.Fatal(err)
	} else if have, want := finfo.Size(), int64(59); have != want {
		t.Fatalf("have %d want %d", have, want)
	}
}
