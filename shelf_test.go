// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// getBlob returns a byte-slice filled with the given fill-byte
func getBlob(fill byte, size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = fill
	}
	return buf
}

func checkBlob(fill byte, blob []byte, size int) error {
	if len(blob) != size {
		return fmt.Errorf("wrong size: got %d, want %d", len(blob), size)
	}
	for i := range blob {
		if blob[i] != fill {
			return fmt.Errorf("wrong data, byte %d: got %x want %x", i, blob[i], fill)
		}
	}
	return nil
}

func TestBasicsInMemory(t *testing.T) { testBasics(t, "") }
func TestBasicsOnDisk(t *testing.T)   { testBasics(t, t.TempDir()) }

func testBasics(t *testing.T, path string) {
	{ // Pre-instance failures
		// can't open non-existing directory
		if _, err := openShelf("/baz/bonk/foobar/gazonk", 10, nil, false, false); err == nil {
			t.Fatal("expected error")
		}
		// Can't point path to a file
		if _, err := openShelf("./README.md", 10, nil, false, false); err == nil {
			t.Fatal("expected error")
		}
	}
	b, cleanup := setup(t, path)
	defer cleanup()
	{ // Tests on Put
		// Should reject empty data
		if _, err := b.Put([]byte{}); !errors.Is(err, ErrEmptyData) {
			t.Fatal("expected error")
		}
		// Should reject nil data
		if _, err := b.Put(nil); !errors.Is(err, ErrEmptyData) {
			t.Fatal("expected error")
		}
		// Should reject oversized data (max size 200)
		if _, err := b.Put(make([]byte, 201)); !errors.Is(err, ErrOversized) {
			t.Fatal("expected error")
		}
	}
	aa, _ := b.Put(getBlob(0x0a, 150))
	{ // Tests on Update
		// Should reject empty data
		if err := b.Update([]byte{}, aa); !errors.Is(err, ErrEmptyData) {
			t.Fatal("expected error")
		}
		// Should reject nil data
		if err := b.Update(nil, aa); !errors.Is(err, ErrEmptyData) {
			t.Fatal("expected error")
		}
		// max size 200, check oversized data
		if err := b.Update(make([]byte, 201), aa); !errors.Is(err, ErrOversized) {
			t.Fatal("expected error")
		}
	}

	bb, _ := b.Put(getBlob(0x0b, 151))
	cc, _ := b.Put(getBlob(0x0c, 152))
	dd, err := b.Put(getBlob(0x0d, 153))

	if err != nil {
		t.Fatal(err)
	}
	get := func(slot uint64) []byte {
		t.Helper()
		data, err := b.Get(slot)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if err := checkBlob(0x0a, get(aa), 150); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0b, get(bb), 151); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0c, get(cc), 152); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0d, get(dd), 153); err != nil {
		t.Fatal(err)
	}
	// Same checks, but during iteration
	if err = b.Iterate(func(slot uint64, data []byte) {
		if have, want := byte(slot)+0x0a, data[0]; have != want {
			t.Fatalf("wrong content: have %x want %x", have, want)
		}
		if have, want := len(data), int(150+slot); have != want {
			t.Fatalf("wrong size: have %x want %x", have, want)
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Delete item and place a new one there
	if err := b.Delete(bb); err != nil {
		t.Fatal(err)
	}
	// Iteration should skip over deleted items
	if err = b.Iterate(func(slot uint64, data []byte) {
		if have, want := byte(slot)+0x0a, data[0]; have != want {
			t.Fatalf("wrong content: have %x want %x", have, want)
		}
		if have, want := len(data), int(150+slot); have != want {
			t.Fatalf("wrong size: have %x want %x", have, want)
		}
		if slot == bb {
			t.Fatalf("Expected not to iterate %d", bb)
		}
	}); err != nil {
		t.Fatal(err)
	}

	ee, _ := b.Put(getBlob(0x0e, 154))
	if err := checkBlob(0x0e, get(ee), 154); err != nil {
		t.Fatal(err)
	}
	// Update in place
	if err := b.Update(getBlob(0x0f, 35), ee); err != nil {
		t.Fatal(err)
	}
	if err := checkBlob(0x0f, get(ee), 35); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(aa); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ee); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(cc); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(dd); err != nil {
		t.Fatal(err)
	}
	// Iteration should be a no-op
	if err := b.Iterate(func(slot uint64, data []byte) {
		t.Fatalf("Expected no iteration")
	}); err != nil {
		t.Fatal(err)
	}
}

func writeShelfFile(name string, slotSize int, slotData []byte) error {

	f, err := os.Create(name)
	if err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, &shelfHeader{
		Magic:    Magic,
		Version:  curVersion,
		Slotsize: uint32(slotSize),
	}); err != nil {
		return err
	}

	// Fill all the items
	for i, byt := range slotData {
		if byt == 0 {
			continue
		}
		data := getBlob(byt, slotSize-itemHeaderSize)
		slotdata := make([]byte, slotSize)
		// write header
		binary.BigEndian.PutUint32(slotdata, uint32(slotSize-itemHeaderSize))
		copy(slotdata[itemHeaderSize:], data)
		// write data
		_, err = f.WriteAt(slotdata, int64(ShelfHeaderSize)+int64(i)*int64(slotSize))
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIdentical(fileA, fileB string) error {
	var (
		dataA []byte
		dataB []byte
		err   error
	)
	dataA, err = os.ReadFile(fileA)
	if err != nil {
		return fmt.Errorf("failed to open %v: %v", fileA, err)
	}
	dataB, err = os.ReadFile(fileB)
	if err != nil {
		return fmt.Errorf("failed to open %v: %v", fileB, err)
	}
	if !bytes.Equal(dataA, dataB) {
		return fmt.Errorf("data differs: \n%x\n%x", dataA, dataB)
	}
	return nil
}

func setup(t *testing.T, path string) (*shelf, func()) {
	t.Helper()
	a, err := openShelf(path, 200, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	return a, func() {
		a.Close()
	}
}

// TestOversized
// - Test writing oversized data into a shelf
// - Test writing exactly-sized data into a shelf

func TestOversizedInMemory(t *testing.T) { testOversized(t, "") }
func TestOversizedOnDisk(t *testing.T)   { testOversized(t, t.TempDir()) }

func testOversized(t *testing.T, path string) {
	a, cleanup := setup(t, path)
	defer cleanup()

	for s := 190; s < 205; s++ {
		data := getBlob('x', s)
		slot, err := a.Put(data)
		if err != nil {
			if slot != 0 {
				t.Fatalf("Exp slot 0 on error, got %d", slot)
			}
			if have := s + itemHeaderSize; have <= int(a.slotSize) {
				t.Fatalf("expected to store %d bytes of data, got error", have)
			}
		} else {
			if have := s + itemHeaderSize; have > int(a.slotSize) {
				t.Fatalf("expected error storing %d bytes of data", have)
			}
		}
	}
}

// TestErrOnClose
// - Tests reading, writing, deleting from a closed shelf

func TestErrOnCloseInMemory(t *testing.T) { testErrOnClose(t, "") }
func TestErrOnCloseOnDisk(t *testing.T)   { testErrOnClose(t, t.TempDir()) }

func testErrOnClose(t *testing.T, path string) {
	a, cleanup := setup(t, path)
	defer cleanup()
	// Write something and delete it again, to have a gap
	if have, want := a.count, uint64(0); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	_, _ = a.Put(make([]byte, 3))
	if have, want := a.count, uint64(1); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	_, _ = a.Put(make([]byte, 3))
	if have, want := a.count, uint64(2); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	if err := a.Delete(0); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// Double-close should be a no-op
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(make([]byte, 3)); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected error for Put on closed shelf, got %v", err)
	}
	if err := a.Iterate(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected error for Iterate on closed shelf, got %v", err)
	}
	if _, err := a.Get(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected error for Get on closed shelf, got %v", err)
	}
	// Only expectation here is not to panic, basically
	if err := a.Delete(0); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(0); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(1); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(100); !errors.Is(err, ErrBadIndex) {
		t.Fatal("exp error")
	}
}

func TestBadInputInMemory(t *testing.T) { testBadInput(t, "") }
func TestBadInputOnDisk(t *testing.T)   { testBadInput(t, t.TempDir()) }

func testBadInput(t *testing.T, path string) {
	a, cleanup := setup(t, path)
	defer cleanup()

	if _, err := a.Put(make([]byte, 25)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(make([]byte, 25)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(make([]byte, 25)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(make([]byte, 25)); err != nil {
		t.Fatal(err)
	}

	if _, err := a.Get(uint64(0x000000FFFFFFFFFF)); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected %v, got %v", ErrBadIndex, err)
	}
	if _, err := a.Get(uint64(0xFFFFFFFFFFFFFFFF)); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected %v, got %v", ErrBadIndex, err)
	}
	if err := a.Delete(0x000FFFF); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected %v, got %v", ErrBadIndex, err)
	}
	if _, err := a.Put(nil); !errors.Is(err, ErrEmptyData) {
		t.Fatalf("expected %v", ErrEmptyData)
	}
	if _, err := a.Put(make([]byte, 0)); !errors.Is(err, ErrEmptyData) {
		t.Fatalf("expected %v", ErrEmptyData)
	}
}

func TestCompaction(t *testing.T) {
	var (
		a   *shelf
		b   *shelf
		err error
		pA  = t.TempDir()
		pB  = t.TempDir()
	)
	if err = writeShelfFile(filepath.Join(pA, "bkt_00000010.bag"),
		10, []byte{1, 0, 3, 0, 5, 0, 6, 0, 4, 0, 2, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err = writeShelfFile(filepath.Join(pB, "bkt_00000010.bag"),
		10, []byte{1, 2, 3, 4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	// The order and content that we expect for the onData
	expOnData := []byte{1, 2, 3, 4, 5, 6}
	var haveOnData []byte
	onData := func(slot uint64, data []byte) {
		haveOnData = append(haveOnData, data[0])
	}
	/// Now open them as shelves
	a, err = openShelf(pA, 10, onData, false, false)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	b, err = openShelf(pB, 10, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := checkIdentical(
		filepath.Join(pA, "bkt_00000010.bag"),
		filepath.Join(pB, "bkt_00000010.bag")); err != nil {
		t.Fatal(err)
	}
	// And the content of the onData callback
	if !bytes.Equal(expOnData, haveOnData) {
		t.Fatalf("onData wrong, expected \n%x\ngot\n%x\n", expOnData, haveOnData)
	}
}

func TestGapHeap(t *testing.T) {
	fill := func(gaps *sortedUniqueInts) {
		gaps.Append(uint64(1))
		gaps.Append(uint64(10))
		gaps.Append(uint64(2))
		gaps.Append(uint64(9))
		gaps.Append(uint64(3))
		gaps.Append(uint64(8))
		gaps.Append(uint64(4))
		gaps.Append(uint64(7))
		gaps.Append(uint64(5))
		gaps.Append(uint64(6))

	}
	gaps := make(sortedUniqueInts, 0)
	fill(&gaps)
	for i := uint64(10); len(gaps) > 0; i-- {
		if have, want := gaps[len(gaps)-1], i; have != want {
			t.Fatalf("have %d want %d", have, want)
		}
		gaps = gaps[:len(gaps)-1]
	}
	// Check uniqueness filter
	fill(&gaps)
	fill(&gaps)
	fill(&gaps)
	for i := uint64(10); len(gaps) > 0; i-- {
		if have, want := gaps[len(gaps)-1], i; have != want {
			t.Fatalf("have %d want %d", have, want)
		}
		gaps = gaps[:len(gaps)-1]
	}
}

func TestCompaction2(t *testing.T) {
	p := t.TempDir()
	/// Now open them as shelves
	openAndStore := func(data string) {
		a, err := openShelf(p, 10, nil, false, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Put([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
	}
	openAndIterate := func() string {
		var data []byte
		_, err := openShelf(p, 10, func(slot uint64, x []byte) {
			data = append(data, x...)
		}, false, false)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	openAndDel := func(deletes ...int) {
		a, err := openShelf(p, 10, nil, false, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range deletes {
			if err := a.Delete(uint64(id)); err != nil {
				t.Fatal(err)
			}
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
	}
	openAndStore("000000")
	openAndStore("111111")
	openAndStore("222222")
	openAndStore("333333")
	openAndStore("444444")
	if have, want := openAndIterate(), "000000111111222222333333444444"; have != want {
		t.Fatalf("have %v\nwant %v", have, want)
	}
	openAndDel(1)
	// If we delete 1, then the last item should be moved into the gap
	if have, want := openAndIterate(), "000000444444222222333333"; have != want {
		t.Fatalf("have %v want %v", have, want)
	}
	openAndDel(1, 2)
	// If we delete 1, then the last item should be moved into the gap
	if have, want := openAndIterate(), "000000333333"; have != want {
		t.Fatalf("have %v want %v", have, want)
	}
}

func TestShelfRO(t *testing.T) {
	p := t.TempDir()

	a, err := openShelf(p, 20, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Put(make([]byte, 17))
	if !errors.Is(err, ErrOversized) {
		t.Fatalf("expected error")
	}
	// Put some items there, then delete the second to create a gap
	// When we later open it as readonly, there should be mo compaction
	// (so a gap at index 2), but all items should be iterated
	_, err = a.Put(make([]byte, 5)) // id 0, size 5
	if err != nil {
		t.Fatal(err)
	}
	_, _ = a.Put(make([]byte, 6))   // id 1, size 6
	id, _ := a.Put(make([]byte, 7)) // id 2, size 7
	_, _ = a.Put(make([]byte, 8))   // id 3, size 8
	_, _ = a.Put(make([]byte, 9))   // id 4, size 9
	if err := a.Delete(id); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	// READONLY
	out := new(strings.Builder)
	a, err = openShelf(p, 20, func(slot uint64, data []byte) {
		fmt.Fprintf(out, "%d:%d, ", slot, len(data))
	}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	want := "0:5, 1:6, 3:8, 4:9, "
	have := out.String()
	if have != want {
		t.Fatalf("have '%v'\nwant: '%v'\n", have, want)
	}
	if _, err := a.Put(make([]byte, 10)); !errors.Is(err, ErrReadonly) {
		t.Fatal("Expected error")
	}
	if err := a.Delete(0); !errors.Is(err, ErrReadonly) {
		t.Fatal("Expected error")
	}
	if err := a.Update(nil, 0); !errors.Is(err, ErrReadonly) {
		t.Fatal("Expected error")
	}

	if err = a.Close(); err != nil {
		t.Fatal(err)
	}

	// READ/WRITE
	// We now expect the last data (4:9) to be moved to slot 2
	out = new(strings.Builder)
	a, err = openShelf(p, 20, func(slot uint64, data []byte) {
		fmt.Fprintf(out, "%d:%d, ", slot, len(data))
	}, false, false)
	if err != nil {
		t.Fatal(err)
	}
	want = "0:5, 1:6, 2:9, 3:8, "
	have = out.String()
	if have != want {
		t.Fatalf("have '%v'\nwant: '%v'\n", have, want)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	p := t.TempDir()
	a, err := openShelf(p, 20, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	checkSize := func(want int) {
		finfo, _ := a.f.Stat()
		if have := finfo.Size(); int(have) != want {
			t.Fatalf("want size %d, have %d", want, have)
		}
	}
	checkSize(ShelfHeaderSize)
	// Write 100 items
	for i := 0; i < 100; i++ {
		_, _ = a.Put(make([]byte, 15)) // id 1, size 6
	}
	checkSize(2000 + ShelfHeaderSize)
	// Delete half of them, last item first
	for i := 99; i >= 50; i-- {
		_ = a.Delete(uint64(i))
	}
	checkSize(1000 + ShelfHeaderSize)
	// Delete every second remaining item
	for i := 50; i >= 0; i -= 2 {
		_ = a.Delete(uint64(i))
	}
	checkSize(1000 + ShelfHeaderSize)
	var (
		have string
		want = "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,"
	)
	err = a.Iterate(func(slot uint64, data []byte) {
		have = have + fmt.Sprintf("%d,", slot)
	})
	if err != nil {
		t.Fatal(err)
	}
	if have != want {
		t.Fatalf("have: %v\nwant: %v\n", have, want)
	}
}

func TestVersion(t *testing.T) {
	// header for 255-sized shelf
	var (
		size  = uint32(100)
		fname = fmt.Sprintf("bkt_%08d.bag", 100)
		p     = t.TempDir()
	)
	for i, tc := range []struct {
		hdr  []byte
		want string
	}{
		{ // Wrong magic
			hdr:  []byte{'b', 'o', 'l', 'l', 'y', 0x00, 0x00, 0x00, 0x00, 0x00, 100},
			want: "missing magic",
		},
		{ // Wrong size
			hdr:  []byte{'b', 'i', 'l', 'l', 'y', 0x00, 0x00, 0x00, 0x00, 0x00, 0xff},
			want: "wrong slotsize, file:255, need:100",
		},
		{ // Future version
			hdr:  []byte{'b', 'i', 'l', 'l', 'y', 0x05, 0x39, 0x00, 0x00, 0x00, 100},
			want: "wrong version: 1337",
		},
		{ // Too short
			hdr:  []byte{'b'},
			want: "EOF",
		},
		{ // Correct magic, empty shelf
			hdr:  []byte{'b', 'i', 'l', 'l', 'y', 0x00, 0x00, 0x00, 0x00, 0x00, 100},
			want: "",
		},
	} {
		if err := os.WriteFile(filepath.Join(p, fname), tc.hdr, 0o777); err != nil {
			t.Fatal(err)
		}
		_, err := openShelf(p, size, nil, false, false)
		if err == nil {
			if tc.want != "" {
				t.Fatal("expected error")
			}
		} else if have := err.Error(); have != tc.want {
			t.Fatalf("test %d: wrong error, have '%v' want '%v'", i, have, tc.want)
		}
	}
}

func fuzzShelf(t *testing.T, content []byte) {
	// Create a valid shelf, we only fuzz the contents, not the magic
	path := t.TempDir()
	file := filepath.Join(path, fmt.Sprintf("bkt_%08d.bag", 100))
	head := []byte{'b', 'i', 'l', 'l', 'y', 0x00, 0x00, 0x00, 0x00, 0x00, 100}

	// Append the fuzz content to the shelf
	blob := append(head, content...)
	if err := os.WriteFile(file, blob, 0o777); err != nil {
		t.Fatal(err)
	}
	// Try to open the shelf and verify the errors
	shelf, err := openShelf(path, 100, nil, false, false)
	if err == nil {
		shelf.Close()
		return
	}
	shelf, err = openShelf(path, 100, nil, false, true)
	if err != nil {
		t.Fatalf("failed to recover shelf: %v", err)
	}
	shelf.Close()
}

// This test contains a fuzzer-finding which triggered an overflow-to-zero when
// itemHeaderSize and an items-reported-size were added.
func TestShelfContents(t *testing.T) {
	data := []byte("\xff\xff\xff\xfc000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
	fuzzShelf(t, data)
}

func FuzzShelfContents(f *testing.F) {
	f.Fuzz(fuzzShelf)
}
