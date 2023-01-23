// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// itemHeaderSize is 4 bytes: each piece of data is stored as
// [ uint32: size |  <data> ]
const (
	itemHeaderSize = 4
	maxSlotSize    = 0xffffffff
)

var (
	ErrClosed      = errors.New("bucket closed")
	ErrOversized   = errors.New("data too large for bucket")
	ErrBadIndex    = errors.New("bad index")
	ErrEmptyData   = errors.New("empty data")
	ErrCorruptData = errors.New("corrupt data")
)

// A Bucket represents a collection of similarly-sized items. The bucket uses
// a number of slots, where each slot is of the exact same size.
type Bucket struct {
	id       string
	slotSize uint32 // Size of the slots, up to 4GB

	gapsMu sync.Mutex // Mutex for operating on 'gaps' and 'tail'
	// A slice of indices to slots that are free to use. The
	// gaps are always sorted lowest numbers first.
	gaps sortedUniqueInts
	tail uint64 // First free slot

	fileMu sync.RWMutex // Mutex for file operations on 'f' (rw versus Close) and closed
	f      *os.File     // The file backing the data
	closed bool
}

// openBucketAs is mainly exposed for testing purposes
func openBucketAs(path string, id string, slotSize uint32, onData onBucketDataFn) (*Bucket, error) {
	// Check if the path exists
	finfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !finfo.IsDir() {
		return nil, fmt.Errorf("not a directory: '%v'", path)
	}
	f, err := os.OpenFile(filepath.Join(path, fmt.Sprintf("%v", id)), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	var nSlots = uint64(0)
	if stat, err := f.Stat(); err != nil {
		return nil, err
	} else {
		size := stat.Size()
		nSlots = uint64((size + int64(slotSize) - 1) / int64(slotSize))
	}
	bucket := &Bucket{
		id:       id,
		slotSize: slotSize,
		tail:     nSlots,
		f:        f,
	}
	// Compact + iterate
	bucket.compact(onData)
	return bucket, nil
}

// openBucket opens a (new or existing) bucket with the given slot size.
// If the bucket already exists, it's opened and read, which populates the
// internal gap-list.
// The onData callback is optional, and can be nil.
func openBucket(path string, slotSize uint32, onData onBucketDataFn) (*Bucket, error) {
	id := fmt.Sprintf("bkt_%08d.bag", slotSize)
	return openBucketAs(path, id, slotSize, onData)
}

func (bucket *Bucket) Close() error {
	// We don't need the gapsMu until later, but order matters: all places
	// which require both mutexes first obtain gapsMu, and _then_ fileMu.
	// If one place uses a different order, then a deadlock is possible
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	bucket.fileMu.Lock()
	defer bucket.fileMu.Unlock()
	if bucket.closed {
		return nil
	}
	bucket.closed = true
	// Before closing the file, we overwrite all gaps with
	// blank space in the headers. Later on, when opening, we can reconstruct the
	// gaps by skimming through the slots and checking the headers.
	hdr := make([]byte, 4)
	var err error
	for _, gap := range bucket.gaps {
		if _, e := bucket.f.WriteAt(hdr, int64(gap)*int64(bucket.slotSize)); e != nil {
			err = e
		}
	}
	bucket.gaps = bucket.gaps[:0]
	bucket.f.Close()
	return err
}

// Update overwrites the existing data at the given slot. This operation is more
// efficient than Delete + Put, since it does not require managing slot availability
// but instead just overwrites in-place.
func (bucket *Bucket) Update(data []byte, slot uint64) error {
	// Write data: header + blob
	hdr := make([]byte, itemHeaderSize)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if err := bucket.writeFile(hdr, data, slot); err != nil {
		return err
	}
	return nil
}

// Put writes the given data and returns a slot identifier. The caller may
// modify the data after this method returns.
func (bucket *Bucket) Put(data []byte) (uint64, error) {
	// Validations
	if len(data) == 0 {
		return 0, ErrEmptyData
	}
	if have, max := uint32(len(data)+itemHeaderSize), bucket.slotSize; have > max {
		return 0, ErrOversized
	}
	// Find a free slot
	slot := bucket.getSlot()
	// Write data: header + blob
	hdr := make([]byte, itemHeaderSize)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if err := bucket.writeFile(hdr, data, slot); err != nil {
		return 0, err
	}
	return slot, nil
}

// Delete marks the data at the given slot of deletion.
// Delete does not touch the disk. When the bucket is Close():d, any remaining
// gaps will be marked as such in the backing file.
func (bucket *Bucket) Delete(slot uint64) error {
	// Mark gap
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	// Can't delete outside of the file
	if slot >= bucket.tail {
		return fmt.Errorf("%w: bucket %d, slot %d, tail %d", ErrBadIndex, bucket.slotSize, slot, bucket.tail)
	}
	// We try to keep writes going to the early parts of the file, to have the
	// possibility of trimming the file when/if the tail becomes unused.
	bucket.gaps.Append(slot)
	if bucket.tail == bucket.gaps.Last() {
		// we can delete a portion of the file
		bucket.fileMu.Lock()
		defer bucket.fileMu.Unlock()
		if bucket.closed {
			// Undo (not really important, but correct) and back out again
			bucket.gaps = bucket.gaps[:0]
			return ErrClosed
		}
		for len(bucket.gaps) > 0 && bucket.tail == bucket.gaps.Last() {
			bucket.gaps = bucket.gaps[:len(bucket.gaps)-1]
			bucket.tail--
		}
		bucket.f.Truncate(int64(bucket.tail * uint64(bucket.slotSize)))
	}
	return nil
}

// Get returns the data at the given slot. If the slot has been deleted, the returndata
// this method is undefined: it may return the original data, or some newer data
// which has been written into the slot after Delete was called.
func (bucket *Bucket) Get(slot uint64) ([]byte, error) {
	data, err := bucket.readFile(slot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadIndex, err)
	}
	return data, nil
}

func (bucket *Bucket) readFile(slot uint64) ([]byte, error) {
	// We're read-locking this to prevent the file from being closed while we're
	// reading from it
	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()
	if bucket.closed {
		return nil, ErrClosed
	}
	offset := int64(slot) * int64(bucket.slotSize)
	// Read header
	hdr := make([]byte, itemHeaderSize)
	_, err := bucket.f.ReadAt(hdr, offset)
	if err != nil {
		return nil, err
	}
	// Check data size
	blobLen := binary.BigEndian.Uint32(hdr)
	if blobLen+uint32(itemHeaderSize) > uint32(bucket.slotSize) {
		return nil, ErrCorruptData
	}
	// Read data
	buf := make([]byte, blobLen)
	//fmt.Printf("readAt(%d, %d)\n", len(buf), int64(slot)*int64(bucket.slotSize))
	_, err = bucket.f.ReadAt(buf, offset+itemHeaderSize)
	return buf, err
}

func (bucket *Bucket) writeFile(hdr, data []byte, slot uint64) error {
	// We're read-locking this to prevent the file from being closed while we're
	// writing to it
	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()
	if bucket.closed {
		return ErrClosed
	}
	if _, err := bucket.f.WriteAt(hdr, int64(slot)*int64(bucket.slotSize)); err != nil {
		return err
	}
	if _, err := bucket.f.WriteAt(data, int64(slot)*int64(bucket.slotSize)+int64(len(hdr))); err != nil {
		return err
	}
	return nil
}

func (bucket *Bucket) getSlot() uint64 {
	var slot uint64
	// Locate the first free slot
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	if nGaps := bucket.gaps.Len(); nGaps > 0 {
		slot = bucket.gaps[0]
		bucket.gaps = bucket.gaps[1:]
		return slot
	}
	// No gaps available: Expand the tail
	slot = bucket.tail
	bucket.tail++
	return slot
}

// onBucketDataFn is used to iterate the entire dataset in the bucket.
// After the method returns, the content of 'data' will be modified by
// the iterator, so it needs to be copied if it is to be used later.
type onBucketDataFn func(slot uint64, data []byte)

func (bucket *Bucket) Iterate(onData onBucketDataFn) {

	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()

	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()
	if bucket.closed {
		return
	}

	buf := make([]byte, bucket.slotSize)
	var (
		nextGap = uint64(0xffffffffffffffff)
		gapIdx  = 0
	)

	if bucket.gaps.Len() > 0 {
		nextGap = bucket.gaps[0]
	}
	var newGaps []uint64
	for slot := uint64(0); slot < bucket.tail; slot++ {
		if slot == nextGap {
			// We've reached a gap. Skip it
			gapIdx++
			if gapIdx < len(bucket.gaps) {
				nextGap = bucket.gaps[gapIdx]
			} else {
				nextGap = 0xffffffffffffffff
			}
			continue
		}
		n, _ := bucket.f.ReadAt(buf, int64(slot)*int64(bucket.slotSize))
		if n < itemHeaderSize {
			panic(fmt.Sprintf("too short, need %d bytes, got %d", itemHeaderSize, n))
		}
		blobLen := binary.BigEndian.Uint32(buf)
		if blobLen == 0 {
			// Here's an item which has been deleted, but not marked as a gap.
			// Mark it now
			newGaps = append(newGaps, slot)
			continue
		}
		if onData == nil {
			// onData can be nil, it's used on 'Open' to reconstruct the gaps
			continue
		}
		if blobLen+uint32(itemHeaderSize) > uint32(n) {
			panic(fmt.Sprintf("too short, need %d bytes, got %d", blobLen+itemHeaderSize, n))
		}
		onData(slot, buf[itemHeaderSize:itemHeaderSize+blobLen])
	}
	for _, g := range newGaps {
		bucket.gaps.Append(g)
	}
}

// compactBucket moves data 'up' to fill gaps, and truncates the file afterwards.
// This operation must only be performed during the opening of the bucket.
func (bucket *Bucket) compact(onData onBucketDataFn) {
	bucket.gapsMu.Lock()
	defer bucket.gapsMu.Unlock()
	bucket.fileMu.RLock()
	defer bucket.fileMu.RUnlock()

	buf := make([]byte, bucket.slotSize)

	// readSlot reads data from the given slot and returns the declared size.
	// The data is placed into 'buf'
	readSlot := func(slot uint64) uint32 {
		n, _ := bucket.f.ReadAt(buf, int64(slot)*int64(bucket.slotSize))
		if n < itemHeaderSize {
			panic(fmt.Sprintf("failed reading slot %d, need %d bytes, got %d", slot, itemHeaderSize, n))
		}
		return binary.BigEndian.Uint32(buf)
	}
	writeBuf := func(slot uint64) {
		n, _ := bucket.f.WriteAt(buf, int64(slot)*int64(bucket.slotSize))
		if n < len(buf) {
			panic(fmt.Sprintf("write too short, wrote %d bytes, wanted to write %d", n, len(buf)))
		}
	}

	nextGap := func(slot uint64) uint64 {
		for ; slot < bucket.tail; slot++ {
			if size := readSlot(slot); size == 0 {
				// We've found a gap
				return slot
			} else if onData != nil {
				onData(slot, buf[itemHeaderSize:itemHeaderSize+size])
			}
		}
		return slot
	}
	prevData := func(slot, gap uint64) uint64 {
		for ; slot > 0; slot-- {
			if size := readSlot(slot); size != 0 {
				// We've found a slot of data. Copy it to the gap
				writeBuf(gap)
				if onData != nil {
					onData(gap, buf[itemHeaderSize:itemHeaderSize+size])
				}
				return slot
			}
		}
		return 0
	}
	var (
		gapSlot  = uint64(0)
		dataSlot = bucket.tail
		empty    = bucket.tail == 0
	)
	// The compaction / iteration goes through the file two directions:
	// - forwards: search for gaps,
	// - backwards: searh for data to move into the gaps
	// The two searches happen in turns, and if both find a match, the
	// data is moved from the slot to the gap. Once the two searches cross eachother,
	// the algorithm is finished.
	// This algorithm reads minimal number of items and performs minimal
	// number of writes.
	bucket.gaps = make([]uint64, 0)
	if empty {
		return
	}
	dataSlot--
	for gapSlot <= dataSlot {
		gapSlot = nextGap(gapSlot)
		if gapSlot >= bucket.tail {
			break // done here
		}
		dataSlot = prevData(dataSlot, gapSlot)
		gapSlot++
		dataSlot--
	}
	if gapSlot >= dataSlot {
		// Some gc was performed. gapSlot is the first empty slot now
		bucket.tail = gapSlot
		bucket.f.Truncate(int64(bucket.tail * uint64(bucket.slotSize)))
	}
}

// sortedUniqueInts is a helper structure to maintain an ordered slice
// of gaps. We keep them ordered to make writes prefer early slots, to increase
// the chance of trimming the end of files upon deletion.
type sortedUniqueInts []uint64

func (u sortedUniqueInts) Len() int           { return len(u) }
func (u sortedUniqueInts) Less(i, j int) bool { return u[i] < u[j] }
func (u sortedUniqueInts) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u sortedUniqueInts) Last() uint64       { return u[len(u)-1] }

func (u *sortedUniqueInts) Append(elem uint64) {
	s := *u
	size := len(s)
	idx := sort.Search(size, func(i int) bool {
		return elem <= s[i]
	})
	if idx < size && s[idx] == elem {
		return // Elem already there
	}
	*u = append(s[:idx], append([]uint64{elem}, s[idx:]...)...)
}
