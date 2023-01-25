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

func TestBasics(t *testing.T) {
	b, cleanup := setup(t)
	defer cleanup()
	aa, _ := b.Put(getBlob(0x0a, 150))
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
	b.Iterate(func(slot uint64, data []byte) {
		if have, want := byte(slot)+0x0a, data[0]; have != want {
			t.Fatalf("wrong content: have %x want %x", have, want)
		}
		if have, want := len(data), int(150+slot); have != want {
			t.Fatalf("wrong size: have %x want %x", have, want)
		}
	})
	// Delete item and place a new one there
	if err := b.Delete(bb); err != nil {
		t.Fatal(err)
	}
	// Iteration should skip over deleted items
	b.Iterate(func(slot uint64, data []byte) {
		if have, want := byte(slot)+0x0a, data[0]; have != want {
			t.Fatalf("wrong content: have %x want %x", have, want)
		}
		if have, want := len(data), int(150+slot); have != want {
			t.Fatalf("wrong size: have %x want %x", have, want)
		}
		if slot == bb {
			t.Fatalf("Expected not to iterate %d", bb)
		}
	})
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
	b.Iterate(func(slot uint64, data []byte) {
		t.Fatalf("Expected no iteration")
	})
}

func writeBucketFile(name string, size int, slotData []byte) error {
	var bucketData = make([]byte, len(slotData)*size)
	// Fill all the items
	for i, byt := range slotData {
		if byt == 0 {
			continue
		}
		data := getBlob(byt, size-itemHeaderSize)
		// write header
		binary.BigEndian.PutUint32(bucketData[i*size:], uint32(size-itemHeaderSize))
		// write data
		copy(bucketData[i*size+itemHeaderSize:], data)
	}
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	_, err = f.Write(bucketData)
	return err
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

func setup(t *testing.T) (*Bucket, func()) {
	t.Helper()
	a, err := openBucket(t.TempDir(), 200, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	return a, func() {
		a.Close()
	}
}

// TestOversized
// - Test writing oversized data into a bucket
// - Test writing exactly-sized data into a bucket
func TestOversized(t *testing.T) {
	a, cleanup := setup(t)
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
// - Tests reading, writing, deleting from a closed bucket
func TestErrOnClose(t *testing.T) {
	a, cleanup := setup(t)
	defer cleanup()
	// Write something and delete it again, to have a gap
	if have, want := a.tail, uint64(0); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	_, _ = a.Put(make([]byte, 3))
	if have, want := a.tail, uint64(1); have != want {
		t.Fatalf("tail error have %v want %v", have, want)
	}
	_, _ = a.Put(make([]byte, 3))
	if have, want := a.tail, uint64(2); have != want {
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
		t.Fatalf("expected error for Put on closed bucket, got %v", err)
	}
	if _, err := a.Get(0); !errors.Is(err, ErrBadIndex) {
		t.Fatalf("expected error for Get on closed bucket, got %v", err)
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

func TestBadInput(t *testing.T) {
	a, cleanup := setup(t)
	defer cleanup()

	a.Put(make([]byte, 25))
	a.Put(make([]byte, 25))
	a.Put(make([]byte, 25))
	a.Put(make([]byte, 25))

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
		a   *Bucket
		b   *Bucket
		err error
		pA  = t.TempDir()
		pB  = t.TempDir()
	)
	if err = writeBucketFile(filepath.Join(pA, "bkt_00000010.bag"),
		10, []byte{1, 0, 3, 0, 5, 0, 6, 0, 4, 0, 2, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err = writeBucketFile(filepath.Join(pB, "bkt_00000010.bag"),
		10, []byte{1, 2, 3, 4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	// The order and content that we expect for the onData
	expOnData := []byte{1, 2, 3, 4, 5, 6}
	var haveOnData []byte
	onData := func(slot uint64, data []byte) {
		haveOnData = append(haveOnData, data[0])
	}
	/// Now open them as buckets
	a, err = openBucket(pA, 10, onData, false)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	b, err = openBucket(pB, 10, nil, false)
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
	for i := uint64(10); gaps.Len() > 0; i-- {
		if have, want := gaps.Last(), i; have != want {
			t.Fatalf("have %d want %d", have, want)
		}
		gaps = gaps[:len(gaps)-1]
	}
	// Check uniqueness filter
	fill(&gaps)
	fill(&gaps)
	fill(&gaps)
	for i := uint64(10); gaps.Len() > 0; i-- {
		if have, want := gaps.Last(), i; have != want {
			t.Fatalf("have %d want %d", have, want)
		}
		gaps = gaps[:len(gaps)-1]
	}
}

func TestCompaction2(t *testing.T) {
	p := t.TempDir()
	/// Now open them as buckets
	openAndStore := func(data string) {
		a, err := openBucket(p, 10, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		a.Put([]byte(data))
		a.Close()
	}
	openAndIterate := func() string {
		var data []byte
		_, err := openBucket(p, 10, func(slot uint64, x []byte) {
			data = append(data, x...)
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	openAndDel := func(deletes ...int) {
		a, err := openBucket(p, 10, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range deletes {
			a.Delete(uint64(id))
		}
		a.Close()
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

func TestBucketRO(t *testing.T) {
	p := t.TempDir()

	a, err := openBucket(p, 20, nil, false)
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
	a.Put(make([]byte, 6))          // id 1, size 6
	id, _ := a.Put(make([]byte, 7)) // id 2, size 7
	a.Put(make([]byte, 8))          // id 3, size 8
	a.Put(make([]byte, 9))          // id 4, size 9
	a.Delete(id)
	a.Close()

	// READONLY
	out := new(strings.Builder)
	a, err = openBucket(p, 20, func(slot uint64, data []byte) {
		fmt.Fprintf(out, "%d:%d, ", slot, len(data))
	}, true)
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

	if err = a.Close(); err != nil {
		t.Fatal(err)
	}

	// READ/WRITE
	// We now expect the last data (4:9) to be moved to slot 2
	out = new(strings.Builder)
	a, err = openBucket(p, 20, func(slot uint64, data []byte) {
		fmt.Fprintf(out, "%d:%d, ", slot, len(data))
	}, false)
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

// TODO tests
// - Test Put / Delete in parallel
// - Test that simultaneous filewrites to different parts of the file don't cause problems
