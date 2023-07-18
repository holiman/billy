// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import "testing"

func TestMemoryStore(t *testing.T) {
	ms := new(memoryStore)
	for i, want := range []int64{100, 150, 149, 99, 0} {
		if err := ms.Truncate(want); err != nil {
			t.Fatal(err)
		}
		if have := int64(len(ms.buffer)); have != want {
			t.Fatalf("test %d: have %d want %d", i, have, want)
		}
	}
}

func TestOutOfBands(t *testing.T) {
	ms := new(memoryStore)
	if _, err := ms.WriteAt(make([]byte, 0), -1); err == nil {
		t.Fatal("want error, got none")
	}
	if _, err := ms.ReadAt(make([]byte, 0), -1); err == nil {
		t.Fatal("want error, got none")
	}
	// Make it a bit larger
	if err := ms.Truncate(100); err != nil {
		t.Fatal(err)
	}
	n, err := ms.ReadAt(make([]byte, 20), 90)
	if err == nil {
		t.Fatal("want error, got none")
	}
	if n != 10 {
		t.Fatalf("expected to read %d bytes, read %d", 10, n)
	}
}
