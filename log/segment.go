package log

import (
	"fmt"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	"google.golang.org/protobuf/proto"
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

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	// marshal the record and get raw bytes
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// append to the store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// append to the index (index offsets are relative to base offset)
	err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}
