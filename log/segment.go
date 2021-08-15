package log

import (
	"fmt"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	"os"
	"path"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64 // offset to append new records under
	nextOffset uint64 // relative offsets for the index entries
	config     Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	// setting up the store file
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// setting up the index file
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)), os.O_RDWR|os.O_CREATE, 0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// update the nextOffset
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) {

}
