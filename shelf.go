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
	maxSlotSize    = uint64(0xffffffff)
	// minSlotSize is the minimum size of a slot. It needs to fit the header,
	// and then some actual data too.
	minSlotSize = itemHeaderSize * 2
)

var (
	ErrClosed      = errors.New("shelf closed")
	ErrOversized   = errors.New("data too large for shelf")
	ErrBadIndex    = errors.New("bad index")
	ErrEmptyData   = errors.New("empty data")
	ErrReadonly    = errors.New("read-only mode")
	ErrCorruptData = errors.New("corrupt data")
)

// A shelf represents a collection of similarly-sized items. The shelf uses
// a number of slots, where each slot is of the exact same size.
type shelf struct {
	id       string
	slotSize uint32 // Size of the slots, up to 4GB

	gapsMu sync.Mutex // Mutex for operating on 'gaps' and 'tail'
	// A slice of indices to slots that are free to use. The
	// gaps are always sorted lowest numbers first.
	gaps sortedUniqueInts
	tail uint64 // First free slot

	fileMu   sync.RWMutex // Mutex for file operations on 'f' (rw versus Close) and closed
	f        *os.File     // The file backing the data
	closed   bool
	readonly bool
}

// openShelf opens a (new or existing) shelf with the given slot size.
// If the shelf already exists, it's opened and read, which populates the
// internal gap-list.
// The onData callback is optional, and can be nil.
func openShelf(path string, slotSize uint32, onData onShelfDataFn, readonly bool) (*shelf, error) {
	if slotSize < minSlotSize {
		return nil, fmt.Errorf("slot size %d smaller than minimum (%d)", slotSize, minSlotSize)
	}
	if finfo, err := os.Stat(path); err != nil {
		return nil, err
	} else if !finfo.IsDir() {
		return nil, fmt.Errorf("not a directory: '%v'", path)
	}
	var (
		id     = fmt.Sprintf("bkt_%08d.bag", slotSize)
		f      *os.File
		err    error
		nSlots uint64
	)
	if readonly {
		f, err = os.OpenFile(filepath.Join(path, fmt.Sprintf("%v", id)), os.O_RDONLY, 0666)
	} else {
		f, err = os.OpenFile(filepath.Join(path, fmt.Sprintf("%v", id)), os.O_RDWR|os.O_CREATE, 0666)
	}
	if err != nil {
		return nil, err
	}
	if stat, err := f.Stat(); err != nil {
		return nil, err
	} else {
		size := stat.Size()
		nSlots = uint64((size + int64(slotSize) - 1) / int64(slotSize))
	}
	sh := &shelf{
		id:       id,
		slotSize: slotSize,
		tail:     nSlots,
		f:        f,
		readonly: readonly,
	}
	// Compact + iterate
	sh.compact(onData)
	return sh, nil
}

func (s *shelf) Close() error {
	// We don't need the gapsMu until later, but order matters: all places
	// which require both mutexes first obtain gapsMu, and _then_ fileMu.
	// If one place uses a different order, then a deadlock is possible
	s.gapsMu.Lock()
	defer s.gapsMu.Unlock()
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.readonly {
		return nil
	}
	var err error
	setErr := func(e error) {
		if err == nil && e != nil {
			err = e
		}
	}
	// Before closing the file, we overwrite all gaps with
	// blank space in the headers. Later on, when opening, we can reconstruct the
	// gaps by skimming through the slots and checking the headers.
	hdr := make([]byte, 4)
	for _, gap := range s.gaps {
		_, e := s.f.WriteAt(hdr, int64(gap)*int64(s.slotSize))
		setErr(e)
	}
	s.gaps = s.gaps[:0]
	setErr(s.f.Sync())
	setErr(s.f.Close())
	return err
}

// Update overwrites the existing data at the given slot. This operation is more
// efficient than Delete + Put, since it does not require managing slot availability
// but instead just overwrites in-place.
func (s *shelf) Update(data []byte, slot uint64) error {
	// Write data: header + blob
	hdr := make([]byte, itemHeaderSize)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	return s.writeFile(hdr, data, slot)
}

// Put writes the given data and returns a slot identifier. The caller may
// modify the data after this method returns.
func (s *shelf) Put(data []byte) (uint64, error) {
	if s.readonly {
		return 0, ErrReadonly
	}
	// Validations
	if len(data) == 0 {
		return 0, ErrEmptyData
	}
	if have, max := uint32(len(data)+itemHeaderSize), s.slotSize; have > max {
		return 0, ErrOversized
	}
	// Find a free slot
	slot := s.getSlot()
	// Write data: header + blob
	hdr := make([]byte, itemHeaderSize)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if err := s.writeFile(hdr, data, slot); err != nil {
		return 0, err
	}
	return slot, nil
}

// Delete marks the data at the given slot of deletion.
// Delete does not touch the disk. When the shelf is Close():d, any remaining
// gaps will be marked as such in the backing file.
func (s *shelf) Delete(slot uint64) error {
	if s.readonly {
		return ErrReadonly
	}
	// Mark gap
	s.gapsMu.Lock()
	defer s.gapsMu.Unlock()
	// Can't delete outside of the file
	if slot >= s.tail {
		return fmt.Errorf("%w: shelf %d, slot %d, tail %d", ErrBadIndex, s.slotSize, slot, s.tail)
	}
	// We try to keep writes going to the early parts of the file, to have the
	// possibility of trimming the file when/if the tail becomes unused.
	s.gaps.Append(slot)
	if s.tail == s.gaps.Last() {
		// we can delete a portion of the file
		s.fileMu.Lock()
		defer s.fileMu.Unlock()
		if s.closed {
			// Undo (not really important, but correct) and back out again
			s.gaps = s.gaps[:0]
			return ErrClosed
		}
		for len(s.gaps) > 0 && s.tail == s.gaps.Last() {
			s.gaps = s.gaps[:len(s.gaps)-1]
			s.tail--
		}
		if err := s.f.Truncate(int64(s.tail * uint64(s.slotSize))); err != nil {
			return err
		}
	}
	return nil
}

// Get returns the data at the given slot. If the slot has been deleted, the returndata
// this method is undefined: it may return the original data, or some newer data
// which has been written into the slot after Delete was called.
func (s *shelf) Get(slot uint64) ([]byte, error) {
	data, err := s.readFile(slot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadIndex, err)
	}
	return data, nil
}

func (s *shelf) readFile(slot uint64) ([]byte, error) {
	// We're read-locking this to prevent the file from being closed while we're
	// reading from it
	s.fileMu.RLock()
	defer s.fileMu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	offset := int64(slot) * int64(s.slotSize)
	// Read the entire slot at once -- this might mean we read a bit more
	// than strictly necessary, but it saves us one syscall.
	slotData := make([]byte, s.slotSize)
	_, err := s.f.ReadAt(slotData, offset)
	if err != nil {
		return nil, err
	}
	// Check data size
	itemSize := binary.BigEndian.Uint32(slotData)
	if itemHeaderSize+itemSize >= uint32(s.slotSize) {
		return nil, ErrCorruptData
	}
	return slotData[itemHeaderSize : itemHeaderSize+itemSize], nil
}

func (s *shelf) writeFile(hdr, data []byte, slot uint64) error {
	// We're read-locking this to prevent the file from being closed while we're
	// writing to it
	s.fileMu.RLock()
	defer s.fileMu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	buf := make([]byte, s.slotSize)
	copy(buf, hdr[:])
	copy(buf[itemHeaderSize:], data)
	if _, err := s.f.WriteAt(buf, int64(slot)*int64(s.slotSize)); err != nil {
		return err
	}
	return nil
}

func (s *shelf) getSlot() uint64 {
	var slot uint64
	// Locate the first free slot
	s.gapsMu.Lock()
	defer s.gapsMu.Unlock()
	if nGaps := s.gaps.Len(); nGaps > 0 {
		slot = s.gaps[0]
		s.gaps = s.gaps[1:]
		return slot
	}
	// No gaps available: Expand the tail
	slot = s.tail
	s.tail++
	return slot
}

// onShelfDataFn is used to iterate the entire dataset in the shelf.
// After the method returns, the content of 'data' will be modified by
// the iterator, so it needs to be copied if it is to be used later.
type onShelfDataFn func(slot uint64, data []byte)

func (s *shelf) Iterate(onData onShelfDataFn) {

	s.gapsMu.Lock()
	defer s.gapsMu.Unlock()

	s.fileMu.RLock()
	defer s.fileMu.RUnlock()
	if s.closed {
		return
	}

	buf := make([]byte, s.slotSize)
	var (
		nextGap = uint64(0xffffffffffffffff)
		gapIdx  = 0
	)

	if s.gaps.Len() > 0 {
		nextGap = s.gaps[0]
	}
	var newGaps []uint64
	for slot := uint64(0); slot < s.tail; slot++ {
		if slot == nextGap {
			// We've reached a gap. Skip it
			gapIdx++
			if gapIdx < len(s.gaps) {
				nextGap = s.gaps[gapIdx]
			} else {
				nextGap = 0xffffffffffffffff
			}
			continue
		}
		n, _ := s.f.ReadAt(buf, int64(slot)*int64(s.slotSize))
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
		s.gaps.Append(g)
	}
}

// compact moves data 'up' to fill gaps, and truncates the file afterwards.
// This operation must only be performed during the opening of the shelf.
func (s *shelf) compact(onData onShelfDataFn) {
	s.gapsMu.Lock()
	defer s.gapsMu.Unlock()
	s.fileMu.RLock()
	defer s.fileMu.RUnlock()

	buf := make([]byte, s.slotSize)

	// readSlot reads data from the given slot and returns the declared size.
	// The data is placed into 'buf'
	readSlot := func(slot uint64) uint32 {
		n, _ := s.f.ReadAt(buf, int64(slot)*int64(s.slotSize))
		if n < itemHeaderSize {
			panic(fmt.Sprintf("failed reading slot %d, need %d bytes, got %d", slot, itemHeaderSize, n))
		}
		return binary.BigEndian.Uint32(buf)
	}
	writeBuf := func(slot uint64) {
		n, _ := s.f.WriteAt(buf, int64(slot)*int64(s.slotSize))
		if n < len(buf) {
			panic(fmt.Sprintf("write too short, wrote %d bytes, wanted to write %d", n, len(buf)))
		}
	}

	nextGap := func(slot uint64) uint64 {
		for ; slot < s.tail; slot++ {
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
		for ; slot > gap && slot > 0; slot-- {
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
		dataSlot = s.tail
		empty    = s.tail == 0
	)
	// The compaction / iteration goes through the file two directions:
	// - forwards: search for gaps,
	// - backwards: searh for data to move into the gaps
	// The two searches happen in turns, and if both find a match, the
	// data is moved from the slot to the gap. Once the two searches cross eachother,
	// the algorithm is finished.
	// This algorithm reads minimal number of items and performs minimal
	// number of writes.
	s.gaps = make([]uint64, 0)
	if empty {
		return
	}
	if s.readonly {
		// Don't (try to) mutate the file in readonly mode, but still
		// iterate for the ondata callbacks.
		for gapSlot <= s.tail {
			gapSlot = nextGap(gapSlot)
			gapSlot++
		}
		return
	}
	dataSlot--
	firstTail := s.tail
	for gapSlot <= dataSlot {
		gapSlot = nextGap(gapSlot)
		if gapSlot >= s.tail {
			break // done here
		}
		dataSlot = prevData(dataSlot, gapSlot)
		// dataSlot is now the empty area
		s.tail = dataSlot
		gapSlot++
		dataSlot--
	}
	if firstTail != s.tail {
		// Some gc was performed. gapSlot is the first empty slot now
		if err := s.f.Truncate(int64(s.tail * uint64(s.slotSize))); err != nil {
			// TODO handle better?
			fmt.Fprintf(os.Stderr, "Warning: truncation failed: err %v", err)
		}
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
