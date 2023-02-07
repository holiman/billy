// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"bytes"
	"math/rand"
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

	if _, err := f.Seek(55, 0); err != nil {
		t.Fatal(err)
	}
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

	if _, err := f.WriteAt([]byte("test"), 55); err != nil {
		t.Fatal(err)
	}
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
	db, err := Open(Options{Path: t.TempDir()}, SlotSizePowerOfTwo(128, 500), nil)
	if err != nil {
		t.Fatal(err)
	}
	k0, _ := db.Put(fill(0, 140))
	k1, _ := db.Put(fill(1, 140))
	k2, _ := db.Put(fill(2, 140))
	k3, _ := db.Put(fill(3, 140))

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

func TestCustomSlotSizesFailures(t *testing.T) {
	a := uint32(500)
	var b uint32
	var c uint32
	for i, tt := range []SlotSizeFn{
		// Not increasing orders
		func() (uint32, bool) {
			a--
			return a, false
		},
		// Too many buckets
		func() (uint32, bool) {
			b++
			return 1024 + b, b > 10000
		},
		// Too small buckets
		func() (uint32, bool) {
			c++
			return c, c > 5
		},
	} {
		_, err := Open(Options{Path: t.TempDir()}, tt, nil)
		if err == nil {
			t.Errorf("test %d: expected error but got none", i)
		}
		t.Logf("error: %v", err)
	}
}

func TestCustomSlotSizesOk(t *testing.T) {
	a := 0
	db, err := Open(Options{Path: t.TempDir()}, func() (uint32, bool) {
		ret := 10 * (1 + a)
		a++
		return uint32(ret), ret >= 30
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if have, want := len(db.(*database).shelves), 3; have != want {
		t.Fatalf("have %d buckets, want %d", have, want)
	}
}

func TestSizes(t *testing.T) {
	a := 0
	db, err := Open(Options{Path: t.TempDir()}, func() (uint32, bool) {
		// Return 10, 20, 30
		ret := 10 * (1 + a)
		a++
		return uint32(ret), ret >= 30
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 35; i++ {
		var (
			want = make([]byte, i)
			have []byte
		)
		rand.Read(want)
		key, err := db.Put(want)
		if i >= 27 && err != nil {
			// It should reject size 27  and onwards
			continue
		}
		if err != nil {
			t.Errorf("test %d: %v", i, err)
			continue
		}
		have, err = db.Get(key)
		if err != nil {
			t.Errorf("test %d: %v", i, err)
			continue
		}
		if !bytes.Equal(have, want) {
			t.Errorf("test %d\nhave: %x\nwant: %x\n", i, have, want)
		}
	}
}
