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
	// lenWidth defines the number of bytes used to store the record's length
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append appends a record to the store
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// writing the size of the content
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// writing the actual content
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// number of bytes written plus the number of bytes needed to specify the size
	w += lenWidth

	// increment tbe store size
	s.size += uint64(w)

	// returns numBytesWritten, starting position of the record and error
	return uint64(w), pos, nil
}

// Read reads a record from the store
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush any outstanding content to write
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read the record size first
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// read the actual content
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt a given offset
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close closes the underlying file writer
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
