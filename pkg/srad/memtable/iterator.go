package memtable

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"sync"

	q "github.com/CVDpl/go-live-srad/pkg/srad/query"
)

// Iterator provides regex-based iteration over the memtable.
type Iterator struct {
	root    *Node
	regex   *regexp.Regexp
	stack   []*iterState
	current *iterState
	err     error

	// Optimization: precomputed literal prefix
	literal    []byte
	hasLiteral bool

	// NFA engine
	eng *q.Engine

	// NEW: Cache for frequently used paths to avoid repeated NFA checks
	pathCache map[string]bool // prefix -> canMatch
	cacheSize int
	cacheMu   sync.RWMutex

	// NEW: Statistics for optimization
	stats struct {
		pathsChecked  int
		pathsSkipped  int
		cacheHits     int
		cacheMisses   int
		nfaChecks     int
		literalChecks int
	}
}

// iterState represents the state of iteration at a node.
type iterState struct {
	node       *Node
	prefix     []byte
	childIndex int
	nfaState   q.StateSet
}

// Next advances to the next matching entry.
func (it *Iterator) Next(ctx context.Context) bool {
	for len(it.stack) > 0 {
		// Check context cancellation
		select {
		case <-ctx.Done():
			it.err = ctx.Err()
			return false
		default:
		}

		// Get current state
		state := it.stack[len(it.stack)-1]
		// Build current key only when needed
		currentKey := append(state.prefix, state.node.label...)

		// Check if this node has a value that matches
		if state.childIndex == 0 && state.node.value != nil && !state.node.value.tombstone && !state.node.value.IsExpired() {
			if it.regex != nil && it.regex.Match(currentKey) {
				it.current = &iterState{node: state.node, prefix: currentKey}
				state.childIndex++
				return true
			}
		}

		// Process children
		if state.childIndex < len(state.node.children) {
			child := state.node.children[state.childIndex]
			state.childIndex++

			// Early pruning using literal prefix when available: ensure child label matches literal suffix
			if it.hasLiteral {
				curLen := len(currentKey)
				if curLen < len(it.literal) {
					suffix := it.literal[curLen:]
					if !bytes.HasPrefix(suffix, child.label) {
						it.stats.pathsSkipped++
						continue
					}
				}
			}

			// Prefer NFA stepping without allocations when supported
			if it.eng != nil && it.eng.Supports() && state.nfaState != nil {
				next := state.nfaState
				viable := true
				for _, bb := range child.label {
					next, viable = it.eng.Step(next, bb)
					if !viable {
						break
					}
				}
				if viable {
					it.stack = append(it.stack, &iterState{
						node:       child,
						prefix:     currentKey,
						childIndex: 0,
						nfaState:   next,
					})
					continue
				}
				it.stats.pathsSkipped++
				continue
			}

			// Fallback: build full childKey and use couldMatchOptimized (regex or unknown state)
			childKey := append(currentKey, child.label...)
			if it.couldMatchOptimized(childKey, state.nfaState) {
				it.stack = append(it.stack, &iterState{
					node:       child,
					prefix:     currentKey,
					childIndex: 0,
					nfaState:   nil,
				})
			} else {
				it.stats.pathsSkipped++
			}
		} else {
			// Done with this node, pop from stack
			it.stack = it.stack[:len(it.stack)-1]
		}
	}

	return false
}

// couldMatchOptimized is the enhanced version with caching and NFA state tracking
func (it *Iterator) couldMatchOptimized(prefix []byte, nfaState q.StateSet) bool {
	it.stats.pathsChecked++

	// Cache check (only for sufficiently long prefixes and at stride positions)
	const minCacheLen = 6
	const cacheStride = 2
	shouldUseCache := len(prefix) >= minCacheLen && (len(prefix)%cacheStride == 0)
	if shouldUseCache && it.pathCache != nil {
		it.cacheMu.RLock()
		if canMatch, exists := it.pathCache[string(prefix)]; exists {
			it.cacheMu.RUnlock()
			it.stats.cacheHits++
			// We only store negatives; if present and false, short-circuit
			if !canMatch {
				return false
			}
		} else {
			it.cacheMu.RUnlock()
			it.stats.cacheMisses++
		}
	}

	// NFA-BASED pruning (most effective)
	if it.eng != nil && nfaState != nil {
		it.stats.nfaChecks++
		// Use cached NFA state if available
		if !it.eng.CanMatchPrefix(prefix) {
			it.cacheResult(prefix, false)
			return false
		}
	} else if it.eng != nil {
		it.stats.nfaChecks++
		// Fallback to full NFA check
		if !it.eng.CanMatchPrefix(prefix) {
			it.cacheResult(prefix, false)
			return false
		}
	}

	// Enhanced literal prefix checking
	if it.hasLiteral {
		it.stats.literalChecks++
		lit := it.literal
		if len(prefix) <= len(lit) {
			// Check if prefix is a prefix of literal
			if !bytes.HasPrefix(lit, prefix) {
				it.cacheResult(prefix, false)
				return false
			}
		} else {
			// Check if literal is a prefix of prefix
			if !bytes.HasPrefix(prefix, lit) {
				it.cacheResult(prefix, false)
				return false
			}
		}
	} else if it.regex != nil {
		// Dynamic literal prefix extraction
		if literal, _ := it.regex.LiteralPrefix(); literal != "" {
			it.stats.literalChecks++
			lit := []byte(literal)
			if len(prefix) <= len(lit) {
				if !bytes.HasPrefix(lit, prefix) {
					it.cacheResult(prefix, false)
					return false
				}
			} else {
				if !bytes.HasPrefix(prefix, lit) {
					it.cacheResult(prefix, false)
					return false
				}
			}
		}
	}

	// Additional heuristics for common patterns
	if it.regex != nil {
		patternStr := it.regex.String()

		// Anchor pattern optimization
		if strings.HasPrefix(patternStr, "^") && strings.HasSuffix(patternStr, "$") {
			// Exact match pattern - check length
			expectedLen := len(patternStr) - 2 // Remove ^ and $
			if len(prefix) > expectedLen {
				it.cacheResult(prefix, false)
				return false
			}
		}

		// Repeat pattern optimization
		if strings.Contains(patternStr, "{") && strings.Contains(patternStr, "}") {
			// Check if prefix length is within reasonable bounds for repeat patterns
			if len(prefix) > 1000 { // Arbitrary limit for repeat patterns
				it.cacheResult(prefix, false)
				return false
			}
		}
	}

	// Do not cache positives to reduce pressure and alokacje
	return true
}

// cacheResult stores the result in the path cache
func (it *Iterator) cacheResult(prefix []byte, canMatch bool) {
	if it.pathCache == nil || len(it.pathCache) >= it.cacheSize {
		return
	}

	it.cacheMu.Lock()
	defer it.cacheMu.Unlock()

	// Double-check size after acquiring lock
	// Only store negatives, for sufficiently long prefixes and at stride
	const minCacheLen = 6
	const cacheStride = 2
	if !canMatch && len(prefix) >= minCacheLen && (len(prefix)%cacheStride == 0) && len(it.pathCache) < it.cacheSize {
		it.pathCache[string(prefix)] = false
	}
}

// GetStats returns optimization statistics
func (it *Iterator) GetStats() map[string]int {
	return map[string]int{
		"pathsChecked":  it.stats.pathsChecked,
		"pathsSkipped":  it.stats.pathsSkipped,
		"cacheHits":     it.stats.cacheHits,
		"cacheMisses":   it.stats.cacheMisses,
		"nfaChecks":     it.stats.nfaChecks,
		"literalChecks": it.stats.literalChecks,
	}
}

// ResetStats resets the statistics
func (it *Iterator) ResetStats() {
	it.stats.pathsChecked = 0
	it.stats.pathsSkipped = 0
	it.stats.cacheHits = 0
	it.stats.cacheMisses = 0
	it.stats.nfaChecks = 0
	it.stats.literalChecks = 0
}

// Err returns any error encountered during iteration.
func (it *Iterator) Err() error { return it.err }

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.prefix
}

// Value returns the current value.
func (it *Iterator) Value() *Value {
	if it.current == nil || it.current.node == nil {
		return nil
	}
	return it.current.node.value
}

// SeqNum returns the sequence number of the current entry.
func (it *Iterator) SeqNum() uint64 {
	if val := it.Value(); val != nil {
		return val.seqNum
	}
	return 0
}

// IsTombstone returns whether the current entry is a tombstone.
func (it *Iterator) IsTombstone() bool {
	if val := it.Value(); val != nil {
		return val.tombstone
	}
	return false
}

// Close closes the iterator and releases resources.
func (it *Iterator) Close() error {
	// Clear the stack and cache to free memory
	it.stack = nil
	it.current = nil

	// Clear the path cache
	if it.pathCache != nil {
		it.cacheMu.Lock()
		it.pathCache = make(map[string]bool)
		it.cacheMu.Unlock()
	}

	// Reset statistics
	it.ResetStats()

	return nil
}

// AllIterator provides iteration over all entries in sorted order.
type AllIterator struct {
	keys   [][]byte
	values []*Value
	index  int
}

// NewAllIterator creates an iterator over all entries.
func NewAllIterator(snapshot *Snapshot) *AllIterator {
	var keys [][]byte
	var values []*Value

	collectAllWithValues(snapshot.root, nil, &keys, &values)

	// Sort by key
	indices := make([]int, len(keys))
	for i := range indices {
		indices[i] = i
	}

	// Sort indices by key
	for i := 0; i < len(indices)-1; i++ {
		for j := i + 1; j < len(indices); j++ {
			if bytes.Compare(keys[indices[i]], keys[indices[j]]) > 0 {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}

	// Reorder keys and values
	sortedKeys := make([][]byte, len(keys))
	sortedValues := make([]*Value, len(values))
	for i, idx := range indices {
		sortedKeys[i] = keys[idx]
		sortedValues[i] = values[idx]
	}

	return &AllIterator{
		keys:   sortedKeys,
		values: sortedValues,
		index:  -1,
	}
}

// collectAllWithValues collects all keys and values.
func collectAllWithValues(node *Node, prefix []byte, keys *[][]byte, values *[]*Value) {
	currentKey := append(prefix, node.label...)

	if node.value != nil && !node.value.tombstone && !node.value.IsExpired() {
		keyCopy := make([]byte, len(currentKey))
		copy(keyCopy, currentKey)
		*keys = append(*keys, keyCopy)
		*values = append(*values, node.value)
	}

	for _, child := range node.children {
		collectAllWithValues(child, currentKey, keys, values)
	}
}

// Next advances to the next entry.
func (it *AllIterator) Next() bool {
	it.index++
	return it.index < len(it.keys)
}

// Key returns the current key.
func (it *AllIterator) Key() []byte {
	if it.index >= 0 && it.index < len(it.keys) {
		return it.keys[it.index]
	}
	return nil
}

// Value returns the current value.
func (it *AllIterator) Value() *Value {
	if it.index >= 0 && it.index < len(it.values) {
		return it.values[it.index]
	}
	return nil
}

// SeqNum returns the sequence number of the current entry.
func (it *AllIterator) SeqNum() uint64 {
	if val := it.Value(); val != nil {
		return val.seqNum
	}
	return 0
}

// IsTombstone returns whether the current entry is a tombstone.
func (it *AllIterator) IsTombstone() bool {
	if val := it.Value(); val != nil {
		return val.tombstone
	}
	return false
}
