package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// enc defines the encoding that we persist record sizes and index entries
	enc = binary.BigEndian
)

const (
	// lenWidth defines the number of bytes used to store the record’s length
	lenWidth = 8
)

// store struct is a simple wrapper around a file with two APIs to append
// and read bytes to and from the file
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a store for the given file
func newStore(f *os.File) (*store, error) {
	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(info.Size())

	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

// Append persists the given bytes to the store
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// writing the log length
	err = binary.Write(s.buf, enc, uint64(len(p)))
	if err != nil {
		return 0, 0, err
	}

	// writing the actual log content
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// increment the bytes written in with the log size information
	w += lenWidth

	// increment store offset
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read returns the record stored at the given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush out any remaining data in the buffer to the underlying writer
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read the bytes which has length information encoded
	sizeByteSlice := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(sizeByteSlice, int64(pos)); err != nil {
		return nil, err
	}

	// decode the byte slice to get the log sizeByteSlice
	logSize := enc.Uint64(sizeByteSlice)

	// read the bytes which has the actual log content encoded
	b := make([]byte, logSize)
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt reads len(p) bytes into p beginning at the off offset in the store’s file
//
// It implements io.ReaderAt on the store type
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
