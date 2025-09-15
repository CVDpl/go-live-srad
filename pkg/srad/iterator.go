package srad

import (
	"context"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/pkg/srad/memtable"
)

// simpleIterator is a simple implementation of Iterator for memtable-only queries.
type simpleIterator struct {
	memIter     *memtable.Iterator
	frozenIters []*memtable.Iterator
	mode        QueryMode
	limit       int
	count       int
	current     struct {
		id  uint64
		key []byte
		op  uint8
	}
	err error
}

// Next advances to the next result.
func (it *simpleIterator) Next(ctx context.Context) bool {
	// Check limit
	if it.limit > 0 && it.count >= it.limit {
		return false
	}

	// Advance memtable iterator first
	if it.memIter != nil {
		if it.memIter.Next(ctx) {
			// Get current data
			it.current.key = it.memIter.Key()
			value := it.memIter.Value()
			if value != nil {
				// Use sequence number as temporary ID for memtable entries
				it.current.id = it.memIter.SeqNum()
				if it.memIter.IsTombstone() {
					it.current.op = common.OpDelete
				} else {
					it.current.op = common.OpInsert
				}
			}
			it.count++
			return true
		}
		it.err = it.memIter.Err()
		it.memIter = nil
	}

	// Then advance frozen snapshot iterators in order
	for len(it.frozenIters) > 0 {
		cur := it.frozenIters[0]
		if cur == nil {
			it.frozenIters = it.frozenIters[1:]
			continue
		}
		if cur.Next(ctx) {
			it.current.key = cur.Key()
			if v := cur.Value(); v != nil {
				it.current.id = cur.SeqNum()
				if cur.IsTombstone() {
					it.current.op = common.OpDelete
				} else {
					it.current.op = common.OpInsert
				}
			}
			it.count++
			return true
		}
		it.frozenIters = it.frozenIters[1:]
	}

	return false
}

// Err returns any error encountered during iteration.
func (it *simpleIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.memIter != nil {
		return it.memIter.Err()
	}
	return nil
}

// ID returns the global ID of the current result.
func (it *simpleIterator) ID() uint64 {
	return it.current.id
}

// String returns the current string value.
func (it *simpleIterator) String() []byte {
	if it.mode == CountOnly {
		return nil
	}
	return it.current.key
}

// Op returns the operation type.
func (it *simpleIterator) Op() uint8 {
	return it.current.op
}

// Close releases resources associated with the iterator.
func (it *simpleIterator) Close() error {
	// Nothing to close for now
	return nil
}
