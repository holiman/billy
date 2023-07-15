// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// wipe deletes the files.
func wipe(t *testing.T, cf *cappedFile) {
	t.Helper()
	var err error
	for _, f := range cf.files {
		if e := os.RemoveAll(f.Name()); e != nil && err == nil {
			err = e
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

func diskSize(t *testing.T, cf *cappedFile) int {
	t.Helper()
	var size int
	for _, f := range cf.files {
		finfo, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		size += int(finfo.Size())
		//t.Logf("file %d size %d total %d", j, finfo.Size(), have)
	}
	return size
}

func TestWriteAt(t *testing.T) {
	// Max filesize: 50 bytes.
	// 10 individual files.
	// Mox storage capacity: 500 bytes.
	f, err := newCappedFile("multifile-test", 10, 50, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wipe(t, f) })

	{
		// Writing data at offset 55 means that it should write into
		// the second file, five bytes in.
		// [0,0,0,0,0,m,o,o,h]
		want := []byte("mooh")
		if _, err := f.WriteAt(want, 55); err != nil {
			t.Fatal(err)
		}
		have := make([]byte, 4)
		if _, err := f.ReadAt(have, 55); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(have, want) {
			t.Fatalf("have %x want %x", have, want)
		}
	}

	{
		// Test data that starts at one file, fully saturates a second, and
		// ends a bit into the third.
		want := []byte("miao01234567890123456789012345678901234567890123456789woof")
		if _, err := f.WriteAt(want, 96); err != nil {
			t.Fatal(err)
		}
		have := make([]byte, 58)
		if _, err := f.ReadAt(have, 96); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(have, want) {
			t.Fatalf("have %x want %x", have, want)
		}
	}
}

func TestTruncate(t *testing.T) {
	// Max filesize: 50 bytes.
	// 10 individual files.
	// Mox storage capacity: 500 bytes.
	f, err := newCappedFile("multifile-test", 10, 50, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wipe(t, f) })
	// Fill with data
	if _, err := f.WriteAt(make([]byte, 470), 20); err != nil {
		t.Fatal(err)
	}
	// The total size of all files should == 490
	if have, want := diskSize(t, f), 490; have != want {
		t.Fatalf("have %d want %d", have, want)
	}
	for i := 480; i > 0; i -= 10 {
		if err := f.Truncate(int64(i)); err != nil {
			t.Fatal(err)
		}
		// The total size of all files should == i
		if have, want := diskSize(t, f), i; have != want {
			t.Fatalf("have %d want %d", have, want)
		}
	}
}

func TestReadonly(t *testing.T) {
	// Create in readonly -- should fail
	f, err := newCappedFile("multifile-ro-test", 10, 50, true)
	if err == nil {
		t.Fatal("want error trying to create files in readonly, got none")
	}
	// Create in RW, should be ok
	f, err = newCappedFile("multifile-ro-test", 10, 50, false)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	// The files now exist, so should be ok to open
	f, err = newCappedFile("multifile-ro-test", 10, 50, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wipe(t, f) })
	if _, err := f.WriteAt([]byte("mooh"), 55); err == nil {
		t.Fatalf("want error trying to write files in readonly, got none")
	}
}

func TestOutOfBounds(t *testing.T) {
	f, err := newCappedFile("multifile-ro-test", 10, 50, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.Close()
		wipe(t, f)
	})
	// Read starting OOB
	if _, err := f.ReadAt(make([]byte, 10), 501); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("want %v have %v", ErrBadIndex, err)
	}
	// Read reaching into OOB
	if _, err := f.ReadAt(make([]byte, 10), 495); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("want %v have %v", ErrBadIndex, err)
	}
	// Write starting OOB
	if _, err := f.WriteAt(make([]byte, 10), 501); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("want %v have %v", ErrBadIndex, err)
	}
	// Write reaching into OOB
	if _, err := f.WriteAt(make([]byte, 10), 495); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("want %v have %v", ErrBadIndex, err)
	}

}
