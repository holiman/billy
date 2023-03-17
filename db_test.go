// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"bytes"
	"crypto/rand"
	"errors"
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
	k2, _ := db.Put(fill(2, 280))
	k3, _ := db.Put(fill(3, 280))

	if have, err := db.Get(k0); err != nil {
		t.Fatal(err)
	} else if want := fill(0, 140); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have := db.Size(k0); have != 256 {
		t.Fatalf(" have\n%d\n want\n%d", have, 256)
	}
	if have, err := db.Get(k1); err != nil {
		t.Fatal(err)
	} else if want := fill(1, 140); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have := db.Size(k1); have != 256 {
		t.Fatalf(" have\n%d\n want\n%d", have, 256)
	}
	if have, err := db.Get(k2); err != nil {
		t.Fatal(err)
	} else if want := fill(2, 280); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have := db.Size(k2); have != 512 {
		t.Fatalf(" have\n%d\n want\n%d", have, 512)
	}
	if have, err := db.Get(k3); err != nil {
		t.Fatal(err)
	} else if want := fill(3, 280); !bytes.Equal(have, want) {
		t.Fatalf(" have\n%x\n want\n%x", have, want)
	}
	if have := db.Size(k3); have != 512 {
		t.Fatalf(" have\n%d\n want\n%d", have, 512)
	}
}

func TestDbErrors(t *testing.T) {
	// Create a db
	p := t.TempDir()
	db, err := Open(Options{Path: p}, SlotSizePowerOfTwo(128, 500), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Put(fill(0, 140))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Iterate(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("want %v,  have %v", ErrClosed, err)
	}
	// Open readonly
	if db, err = Open(Options{Path: p, Readonly: true}, SlotSizePowerOfTwo(128, 500), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Put([]byte{}); !errors.Is(err, ErrReadonly) {
		t.Fatalf("want %v,  have %v", ErrReadonly, err)
	}
	// Open regular again
	db, err = Open(Options{Path: p}, SlotSizePowerOfTwo(128, 500), nil)
	if err != nil {
		t.Fatal(err)
	}
	// We reach in and close one of the files to trigger an error on Close
	_ = db.(*database).shelves[0].f.Close()
	if err := db.Close(); err == nil {
		t.Fatal("expected error due to double-close")
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

func TestSlotSizeLinear(t *testing.T) {
	linearFn := SlotSizeLinear(10, 100)
	for want := 10; want < 1000; want += 10 {
		have, last := linearFn()
		if int(have) != want {
			t.Fatalf("have %d want %d", have, want)
		}
		if last {
			t.Fatalf("ended too soon, val %d", have)
		}
	}
	have, last := linearFn()
	want := 1000
	if int(have) != want {
		t.Fatalf("have %d want %d", have, want)
	}
	if !last {
		t.Fatalf("ended too late")
	}
}

func TestSlotSizePowerOfTwo(t *testing.T) {
	linearFn := SlotSizePowerOfTwo(2, 100)
	for want := 2; want < 100; want *= 2 {
		have, last := linearFn()
		if int(have) != want {
			t.Fatalf("have %d want %d", have, want)
		}
		if last {
			t.Fatalf("ended too soon, val %d", have)
		}
	}
	have, last := linearFn()
	want := 128
	if int(have) != want {
		t.Fatalf("have %d want %d", have, want)
	}
	if !last {
		t.Fatalf("ended too late")
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
	min, max := db.Limits()
	if min != 10 {
		t.Fatalf("exp 10, have %d", min)
	}
	if max != 30 {
		t.Fatalf("exp 30, have %d", max)
	}
	var kvdata = make(map[uint64]string)
	for i := 1; i < 35; i++ {
		var (
			want = make([]byte, i)
			have []byte
		)
		_, _ = rand.Read(want)
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
		kvdata[key] = string(have)
	}
	err = db.Iterate(func(key uint64, size uint32, data []byte) {
		wantVal := kvdata[key]
		wantSize := uint32(((len(wantVal) + itemHeaderSize + 9) / 10) * 10)
		if string(data) != wantVal || size != wantSize {
			t.Fatalf("iteration fail, key %d\nhave data: %x\nwant data: %x\nhave size: %d\nwant size: %d\n",
				key, data, wantVal, size, wantSize)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for key := range kvdata {
		err = db.Delete(key)
		if err != nil {
			t.Errorf("delete key %d: %v", key, err)
			continue
		}
	}
	// Expect nothing to remaing
	err = db.Iterate(func(key uint64, size uint32, data []byte) {
		t.Fatalf("Expected empty db, key %d", key)
	})
	if err != nil {
		t.Fatal(err)
	}
}
