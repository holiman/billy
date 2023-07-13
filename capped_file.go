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
func (cf *cappedFile) WriteAt(data []byte, off int64) (n int, err error) {
	var (
		offset      = uint64(off)
		written     int // no of bytes read
		totalLength = len(data)
		fileNum     = offset / cf.cap
	)
	if fileNum >= uint64(len(cf.files)) {
		return 0, ErrBadIndex
	}
	for ; written < totalLength; fileNum++ {
		offset = offset % cf.cap
		length := uint64(len(data))
		if offset+length > cf.cap {
			// Write continuing in the next
			length = cf.cap - offset
		}
		//fmt.Printf("WriteAt file-%d at %d <- b[:%d]\n", fileNum, offset, length)
		if n, err := cf.files[fileNum].WriteAt(data[:length], int64(offset)); err != nil {
			return written + n, err
		}
		data = data[length:]
		written += int(length)
		offset += length
	}
	return written, nil
}

// ReadAt reads len(b) bytes from the file(s) starting at byte offset off.
// It returns the number of bytes read and the error, if any.
func (cf *cappedFile) ReadAt(b []byte, off int64) (n int, err error) {
	var (
		offset  = uint64(off)
		read    int // no of bytes read
		fileNum = offset / cf.cap
	)
	if fileNum >= uint64(len(cf.files)) {
		return 0, ErrBadIndex
	}
	for ; read < len(b); fileNum++ {
		offset = offset % cf.cap
		length := uint64(len(b) - read) // no of bytes to read this iteration
		if offset+length > cf.cap {
			length = cf.cap - offset
		}
		//fmt.Printf("ReadAt  file-%d at %d -> b[%d:%d]\n", fileNum, offset, read, length)
		if n, err := cf.files[fileNum].ReadAt(b[read:uint64(read)+length], int64(offset)); err != nil {
			return read + n, err
		}
		read += int(length)
		offset += length
	}
	return read, nil
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
	var err error
	for i, f := range cf.files {
		// Files below the truncation limit are expanded to the limit.
		if uint64(i+1)*cf.cap <= uint64(size) {
			if e := f.Truncate(int64(cf.cap)); e != nil && err == nil {
				err = e
			}
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
			size += finfo.Size()
		}
	}
	return &fileinfoMock{size: size}, nil
}
