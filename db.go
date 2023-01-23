// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"fmt"
	"io"
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

	//
	Limits() (uint32, uint32)
}

type DB struct {
	buckets []*Bucket
	dbErr   error
}

// Open opens a (new or existing) database with the given limits.
// If bucket already exists, they are opened and read, in order to populate the
// internal gap-list.
// While doing so, it's a good opportunity for the caller to read the data out,
// (which is probably desirable), which can be done using the optional onData callback.
func Open(path string, smallest, max int, onData OnDataFn) (Database, error) {
	if smallest < 128 {
		return nil, fmt.Errorf("Too small slot size: %d, need at least %d", smallest, 128)
	}
	if smallest >= maxSlotSize {
		return nil, fmt.Errorf("Too large slot size: %d, max is %d", smallest, maxSlotSize)
	}
	if smallest >= max {
		return nil, fmt.Errorf("Bad options, min (%d) >= max (%d)", smallest, max)
	}
	db := &DB{}
	for v := smallest; v < max; v += v {
		bucket, err := openBucket(path, uint32(v), wrapBucketDataFn(len(db.buckets), onData))
		if err != nil {
			db.Close() // Close buckets
			return nil, err
		}
		db.buckets = append(db.buckets, bucket)
	}
	return db, nil
}

// Put stores the data to the underlying database, and returns the key needed
// for later accessing the data.
// The data is copied by the database, and is safe to modify after the method returns
func (db *DB) Put(data []byte) uint64 {
	for i, b := range db.buckets {
		if int(b.slotSize) > len(data) {
			slot, err := b.Put(data)
			if err != nil {
				panic(fmt.Sprintf("Error in Put: %v\n", err))
			}
			slot |= uint64(i) << 24
			return slot
		}
	}
	panic(fmt.Sprintf("No bucket found for size %d", len(data)))
}

// Get retrieves the data stored at the given key.
func (db *DB) Get(key uint64) ([]byte, error) {
	id := int(key >> 24)
	return db.buckets[id].Get(key & 0x00FFFFFF)
}

// Delete marks the data for deletion, which means it will (eventually) be
// overwritten by other data. After calling Delete with a given key, the results
// from doing Get(key) is undefined -- it may return the same data, or some other
// data, or fail with an error.
func (db *DB) Delete(key uint64) error {
	id := int(key >> 24)
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
		key := slot | uint64(bucketId)<<24
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
