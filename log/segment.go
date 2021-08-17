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

// IsMaxed returns whether the segment has reached its max size, either by
// writing too much to the store or the index.
//
// If you wrote a small number of
// long logs, then you’d hit the segment bytes limit; if you wrote a lot of small
// logs, then you’d hit the index bytes limit
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j,
//
// for example nearestMultiple(9, 4) == 8. We take the lesser multiple to make sure
// we stay under the user’s disk capacity
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
