package log

import (
	"github.com/tysonmote/gommap"
	"os"
)

// The *Width constants define the number of bytes that make up each index entry.
var (
	// record offset
	offWidth uint64 = 4
	// record's position in the store file
	posWidth uint64 = 8
	// given offset of a record, the position in the store file is
	// offWidth x offset + posWidth x offset = offset x entWidth
	entWidth = offWidth + posWidth
)

// index defines our index file, which comprises a persisted file and a memory- mapped file.
//
// The size tells us the size of the index and where to write the next entry appended to the index.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates an index for the given file. We create the index and save the current size
// of the file, so we can track the amount of data in the index file as we add index entries.
//
// We grow the file to the max index size before memory-mapping the file and then return the
// created index to the caller.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close makes sure the memory-mapped file has synced its data to the persisted file and that
// the persisted file has flushed its contents to stable storage.
//
// Then it truncates the persisted file to the amount of data that’s actually in it and closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
