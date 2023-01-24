// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"bytes"
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

func fill(data byte, size int) []byte {
	a := make([]byte, size)
	for i := range a {
		a[i] = data
	}
	return a
}

func TestDBBasics(t *testing.T) {
	db, err := Open(t.TempDir(), 128, 500, nil)
	if err != nil {
		t.Fatal(err)
	}
	k0 := db.Put(fill(0, 140))
	k1 := db.Put(fill(1, 140))
	k2 := db.Put(fill(2, 140))
	k3 := db.Put(fill(3, 140))

	if have, err := db.Get(k0); err != nil {
		t.Fatal(err)
	} else if want := fill(0, 140); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have, err := db.Get(k1); err != nil {
		t.Fatal(err)
	} else if want := fill(1, 140); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have, err := db.Get(k2); err != nil {
		t.Fatal(err)
	} else if want := fill(2, 140); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have, err := db.Get(k3); err != nil {
		t.Fatal(err)
	} else if want := fill(3, 140); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
}

func TestCustomSlotSizes(t *testing.T) {
	a := 500
	b := 0
	c := 0
	for i, tt := range []func() (int, bool){
		// Not increasing orders
		func() (int, bool) {
			a--
			return a, false
		},
		// Too many buckets
		func() (int, bool) {
			b++
			return 1024 + b, b > 10000
		},
		// Too small buckets
		func() (int, bool) {
			c++
			return c, c > 5
		},
	} {
		_, err := OpenCustom(t.TempDir(), tt, nil)
		if err == nil {
			t.Errorf("test %d: expected error but got none", i)
		}
		t.Logf("error: %v", err)
	}
}
