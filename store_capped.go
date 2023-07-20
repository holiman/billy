// bagdb: Simple datastorage
// Copyright 2021 billy authors
// SPDX-License-Identifier: BSD-3-Clause

package billy

import (
	"fmt"
	"os"
)

// cappedFile has an API-surface as if it were one file, but maps
// to a set of files on disk. These files are all capped in size
// to maxFileSize.
type cappedFile struct {
	cap   uint64
	files []*os.File
}

// newCappedFile creates a cappedFile
func newCappedFile(basename string, nFiles int, cap uint64, readonly bool) (*cappedFile, error) {
	flags := os.O_RDWR | os.O_CREATE
	if readonly {
		flags = os.O_RDONLY
	}
	if cap == 0 {
		nFiles = 1
	}
	var files []*os.File
	for i := 0; i < nFiles; i++ {
		var (
			f   *os.File
			err error
		)
		if i == 0 {
			f, err = os.OpenFile(basename, flags, 0666)
		} else {
			f, err = os.OpenFile(fmt.Sprintf("%v.cap.%d", basename, i), flags, 0666)
		}
		if err != nil {
			// Clean-up: close opened files
			for _, f := range files {
				f.Close()
			}
			return nil, err
		}
		files = append(files, f)
	}
	return &cappedFile{
		cap:   cap,
		files: files,
	}, nil
}

// WriteAt writes len(b) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(b).
//
// Internally, a write will only touch one file, and may cause the cap to be exceeeded.
func (cf *cappedFile) WriteAt(data []byte, off int64) (n int, err error) {
	var (
		fNum    = uint64(0)
		fOffset = off
	)
	if cf.cap > 0 {
		fNum = uint64(off) / cf.cap
		fOffset = off % int64(cf.cap)
	}
	// Check if the write is out of bounds
	if fNum >= uint64(len(cf.files)) {
		return 0, ErrBadIndex
	}
	//fmt.Printf("file-%d, write %d bytes @ %d (total offset %d)\n", fNum, len(data), fOffset, off)
	return cf.files[fNum].WriteAt(data, fOffset)
}

// ReadAt reads len(b) bytes from the file(s) starting at byte offset off.
// It returns the number of bytes read and the error, if any.
func (cf *cappedFile) ReadAt(b []byte, off int64) (n int, err error) {
	var (
		fNum    = uint64(0)
		fOffset = off
	)
	if cf.cap > 0 {
		fNum = uint64(off) / cf.cap
		fOffset = off % int64(cf.cap)
	}
	// Check if the read is out of bounds
	if fNum >= uint64(len(cf.files)) {
		return 0, ErrBadIndex
	}
	return cf.files[fNum].ReadAt(b, fOffset)
}

// Sync calls *os.File Sync on the backing-files.
func (cf *cappedFile) Sync() error {
	var err error
	for _, f := range cf.files {
		if e := f.Sync(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Close closes all files.
func (cf *cappedFile) Close() error {
	var err error
	for _, f := range cf.files {
		if e := f.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Truncate changes the size of the file.
func (cf *cappedFile) Truncate(size int64) error {
	if cf.cap == 0 {
		return cf.files[0].Truncate(size)
	}
	var err error
	for i, f := range cf.files {
		// Files below the truncation limit are expanded to the limit.
		if uint64(i+1)*cf.cap <= uint64(size) {
			//if e := f.Truncate(int64(cf.cap)); e != nil && err == nil {
			//	err = e
			//}
			continue
		}
		// Files fully above the truncation limit are truncated to zero.
		if uint64(i)*cf.cap > uint64(size) {
			if e := f.Truncate(0); e != nil && err == nil {
				err = e
			}
			continue
		}
		fSize := size % int64(cf.cap)
		//fmt.Printf("Truncate file-%d to %d (total size %d)\n", i, fSize, size)
		if e := f.Truncate(fSize); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (cf *cappedFile) Stat() (os.FileInfo, error) {
	var size int64
	var err error
	for _, f := range cf.files {
		finfo, e := f.Stat()
		if e != nil && err != nil {
			err = e
		}
		if finfo != nil {
			// If a file exceeds the cap, then the next file will contain a
			// corresponding empty-data section in the beginning. Therefore,
			// we must not count that twice. Easiest to just count to the cap.
			// This is a bit hacky, and would be more correct if we also
			// ensure that the 'next' file is non-empty.
			a := finfo.Size()
			if a > int64(cf.cap) {
				a = int64(cf.cap)
			}
			size += a
		}
	}
	return &fileinfoMock{size: size}, nil
}
