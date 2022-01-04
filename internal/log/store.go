package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// enc defines the encoding that we persist record sizes and index entries in
	enc = binary.BigEndian
)

const (
	// lenWidth defines the number of bytes used to store the record’s length
	lenWidth = 8
)

// store is a simple wrapper around a file with two APIs to append and
// read bytes to and from the file
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a store for the given file
func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		File: f,
		mu:   sync.Mutex{},
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

// Append persists the given bytes to the store.
//
// It returns the number of bytes written, the position where the store holds the
// record in its file and an error if any occurred.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// writing the length of the log record first
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// writing the log record
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, err
}

// Read returns the record stored at the given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush any outstanding content in the buffered writer
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// reading the size of the record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// reading the record
	record := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

// ReadAt reads len(p) bytes into p starting at offset off in the store’s file.
//
// It implements io.ReaderAt on the store type.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close flushes any buffered data and close the underlying file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
