package server

import (
	"fmt"
	"sync"
)

// Record includes the data and the offset of the log.
// It's the lowest level of abstraction.
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// Log includes a collection of Record values.
type Log struct {
	mu      sync.Mutex
	records []Record
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")

// NewLog creates a new log
func NewLog() *Log {
	return &Log{}
}

// Append appends the record to the log collection and returns the offset
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read reads a log using the given offset and returns the corresponding Record
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
