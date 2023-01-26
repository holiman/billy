// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"fmt"
	"io"
	"sort"
)

type Database interface {
	io.Closer

	// Put stores the data to the underlying database, and returns the key needed
	// for later accessing the data.
	// The data is copied by the database, and is safe to modify after the method returns
	Put(data []byte) uint64

	// Get retrieves the data stored at the given key.
	Get(key uint64) ([]byte, error)

	// Delete marks the data for deletion, which means it will (eventually) be
	// overwritten by other data. After calling Delete with a given key, the results
	// from doing Get(key) is undefined -- it may return the same data, or some other
	// data, or fail with an error.
	Delete(key uint64) error

	// Limits returns the smallest and largest slot size.
	Limits() (uint32, uint32)
}

// SlotSizeFn is a method that acts as a "generator": a closure which, at each
// invocation, should spit out the next slot-size. In order to create a DB with three
// shelves (buckets), invocation of the method should return e.g.
// 10, false
// 20, false
// 30, true
type SlotSizeFn func() (size uint32, done bool)

// SlotSizePowerOfTwo is a SlotSizeFn which arranges the slots in buckets which
// double in size for each level.
func SlotSizePowerOfTwo(min, max uint32) SlotSizeFn {
	if min >= max { // programming error
		panic(fmt.Sprintf("Bad options, min (%d) >= max (%d)", min, max))
	}
	v := min
	return func() (uint32, bool) {
		ret := v
		v += v
		return ret, ret >= max
	}
}

// SlotSizeLinear is a SlotSizeFn which arranges the slots in buckets which
// increase linearly.
func SlotSizeLinear(size, count int) SlotSizeFn {
	i := 1
	return func() (uint32, bool) {
		ret := size * i
		i++
		return uint32(ret), i >= count
	}
}

type DB struct {
	buckets []*Bucket
}

type Options struct {
	Path     string
	Readonly bool
	Snappy   bool // unused for now
}

// OpenCustom opens a (new or eixsting) database, with configurable limits. The
// given slotSizeFn will be used to determine both the bucket sizes and the number
// of buckets.
// The function must yield values in increasing order.
// If bucket already exists, they are opened and read, in order to populate the
// internal gap-list.
// While doing so, it's a good opportunity for the caller to read the data out,
// (which is probably desirable), which can be done using the optional onData callback.
func Open(opts Options, slotSizeFn SlotSizeFn, onData OnDataFn) (Database, error) {
	var (
		db           = &DB{}
		prevSlotSize uint32
		prevId       int
		slotSize     uint32
		done         bool
	)
	for !done {
		slotSize, done = slotSizeFn()
		if slotSize <= prevSlotSize {
			return nil, fmt.Errorf("slot sizes must be in increasing order")
		}
		prevSlotSize = slotSize
		bucket, err := openBucket(opts.Path, slotSize, wrapBucketDataFn(len(db.buckets), onData), opts.Readonly)
		if err != nil {
			db.Close() // Close buckets
			return nil, err
		}
		db.buckets = append(db.buckets, bucket)

		if id := len(db.buckets) & 0xfff; id < prevId {
			return nil, fmt.Errorf("too many buckets (%d)", len(db.buckets))
		} else {
			prevId = id
		}
	}
	return db, nil
}

// Put stores the data to the underlying database, and returns the key needed
// for later accessing the data.
// The data is copied by the database, and is safe to modify after the method returns
func (db *DB) Put(data []byte) uint64 {
	// Search uses binary search to find and return the smallest index i
	// in [0, n) at which f(i) is true,
	index := sort.Search(len(db.buckets), func(i int) bool {
		return int(db.buckets[i].slotSize) > len(data)
	})
	if index == len(db.buckets) {
		panic(fmt.Sprintf("No bucket found for size %d", len(data)))
	}
	slot, err := db.buckets[index].Put(data)
	if err != nil {
		panic(fmt.Sprintf("Error in Put: %v\n", err))
	}
	slot |= uint64(index) << 28
	return slot
}

// Get retrieves the data stored at the given key.
func (db *DB) Get(key uint64) ([]byte, error) {
	id := int(key>>28) & 0xfff
	return db.buckets[id].Get(key & 0x0FFFFFFF)
}

// Delete marks the data for deletion, which means it will (eventually) be
// overwritten by other data. After calling Delete with a given key, the results
// from doing Get(key) is undefined -- it may return the same data, or some other
// data, or fail with an error.
func (db *DB) Delete(key uint64) error {
	id := int(key>>28) & 0xfff
	return db.buckets[id].Delete(key & 0x00FFFFFF)
}

// OnDataFn is used to iterate the entire dataset in the DB.
// After the method returns, the content of 'data' will be modified by
// the iterator, so it needs to be copied if it is to be used later.
type OnDataFn func(key uint64, data []byte)

func wrapBucketDataFn(bucketId int, onData OnDataFn) onBucketDataFn {
	if onData == nil {
		return nil
	}
	return func(slot uint64, data []byte) {
		key := slot | uint64(bucketId)<<28
		onData(key, data)
	}
}

// Iterate iterates through all the data in the DB, and invokes the
// given onData method for every element
func (db *DB) Iterate(onData OnDataFn) {
	for i, b := range db.buckets {
		b.Iterate(wrapBucketDataFn(i, onData))
	}
}

func (db *DB) Limits() (uint32, uint32) {
	smallest := db.buckets[0].slotSize
	largest := db.buckets[len(db.buckets)-1].slotSize
	return smallest, largest
}

// Close implements io.Closer
func (db *DB) Close() error {
	var err error
	for _, bucket := range db.buckets {
		if e := bucket.Close(); e != nil {
			err = e
		}
	}
	return err
}
