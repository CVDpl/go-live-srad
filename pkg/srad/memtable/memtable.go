package memtable

import (
	"bytes"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	q "github.com/CVDpl/go-live-srad/pkg/srad/query"
)

// Memtable is a memory-efficient radix/Patricia tree for string storage.
// It implements canonical Patricia tree invariants:
// - No leaf has children
// - Internal nodes always have at least 2 children
// - Each edge has a non-empty label
type Memtable struct {
	mu      sync.RWMutex
	root    *Node
	seqNum  uint64
	size    int64 // approximate size in bytes
	count   int64 // number of entries
	deleted int64 // number of tombstones
}

// Node represents a node in the Patricia tree.
type Node struct {
	// Edge label from parent (nil for root)
	label []byte

	// Children nodes, sorted by first byte of label
	children []*Node

	// Value data (nil for internal nodes)
	value *Value

	// Cached size for memory accounting
	cachedSize int64

	// Reference count for Copy-on-Write snapshots
	// When >0, this node is shared with one or more snapshots and must be cloned before modification
	refcount int32
}

// Value represents a stored value with metadata.
type Value struct {
	data      []byte
	seqNum    uint64
	tombstone bool      // true if this is a deletion marker
	expiresAt time.Time // expiration time (zero means no TTL)
}

// IsExpired returns true if the value has expired.
func (v *Value) IsExpired() bool {
	return !v.expiresAt.IsZero() && time.Now().After(v.expiresAt)
}

// ExpiresAt returns the expiration time of the value.
func (v *Value) ExpiresAt() time.Time {
	return v.expiresAt
}

func (v *Value) Tombstone() bool { return v.tombstone }

// New creates a new empty memtable.
func New() *Memtable {
	return &Memtable{
		root: &Node{},
	}
}

// Insert adds a key to the memtable.
func (m *Memtable) Insert(key []byte) error {
	if len(key) == 0 {
		return common.ErrEmptyKey
	}
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}

	// Make a copy of the key to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)
	value := &Value{
		data:      keyCopy,
		seqNum:    seqNum,
		tombstone: false,
	}

	oldSize := m.root.cachedSize
	m.root = m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.count, 1)

	return nil
}

// InsertWithTTL adds a key to the memtable with expiration time.
func (m *Memtable) InsertWithTTL(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return common.ErrEmptyKey
	}
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}
	if ttl < 0 || ttl > common.MaxTTL {
		return common.ErrInvalidTTL
	}

	// Make a copy of the key to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	value := &Value{
		data:      keyCopy,
		seqNum:    seqNum,
		tombstone: false,
		expiresAt: expiresAt,
	}

	oldSize := m.root.cachedSize
	m.root = m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.count, 1)

	return nil
}

// InsertWithExpiry adds a key with an absolute expiration timestamp.
func (m *Memtable) InsertWithExpiry(key []byte, expiresAt time.Time) error {
	if len(key) == 0 {
		return common.ErrEmptyKey
	}
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}

	// Make a copy of the key to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)
	value := &Value{
		data:      keyCopy,
		seqNum:    seqNum,
		tombstone: false,
		expiresAt: expiresAt,
	}

	oldSize := m.root.cachedSize
	m.root = m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.count, 1)

	return nil
}

// InsertWithExpiryNoCopy is a zero-copy variant for internal use.
// The caller MUST guarantee that 'key' will not be modified after this call.
// Used by WAL replay where keys are freshly allocated and single-use.
func (m *Memtable) InsertWithExpiryNoCopy(key []byte, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)
	value := &Value{
		data:      key, // No copy - caller guarantees ownership
		seqNum:    seqNum,
		tombstone: false,
		expiresAt: expiresAt,
	}

	oldSize := m.root.cachedSize
	m.root = m.insertNodeWithPrefix(m.root, nil, key, key, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.count, 1)

	return nil
}

// Delete marks a key as deleted (tombstone).
func (m *Memtable) Delete(key []byte) error {
	if len(key) == 0 {
		return common.ErrEmptyKey
	}
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}

	// Make a copy of the key to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)
	value := &Value{
		data:      keyCopy,
		seqNum:    seqNum,
		tombstone: true,
	}

	oldSize := m.root.cachedSize
	m.root = m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.deleted, 1)

	return nil
}

// DeleteNoCopy is a zero-copy variant for internal use.
// The caller MUST guarantee that 'key' will not be modified after this call.
// Used by WAL replay where keys are freshly allocated and single-use.
func (m *Memtable) DeleteNoCopy(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)
	value := &Value{
		data:      key, // No copy - caller guarantees ownership
		seqNum:    seqNum,
		tombstone: true,
	}

	oldSize := m.root.cachedSize
	m.root = m.insertNodeWithPrefix(m.root, nil, key, key, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.deleted, 1)

	return nil
}

// ensurWritable checks if a node is shared (refcount > 0) and clones it if necessary.
// Returns the node that should be used for writing (may be the original or a clone).
func (m *Memtable) ensureWritable(node *Node) *Node {
	if atomic.LoadInt32(&node.refcount) > 0 {
		// Node is shared with snapshots, must clone before modifying
		return m.shallowCloneNode(node)
	}
	return node
}

// shallowCloneNode creates a shallow copy of a node (doesn't clone children recursively).
// Children slice is copied but children nodes themselves are shared.
func (m *Memtable) shallowCloneNode(node *Node) *Node {
	clone := &Node{
		label:      node.label, // immutable, safe to share
		value:      node.value, // values are immutable, safe to share
		cachedSize: node.cachedSize,
		refcount:   0, // new clone is not shared
	}
	if len(node.children) > 0 {
		clone.children = make([]*Node, len(node.children))
		copy(clone.children, node.children)
	}
	return clone
}

// insertNodeWithPrefix inserts a value into the tree rooted at node, tracking the prefix.
// prefix: the path from root to this node
// key: the remaining key suffix to insert
// fullKey: the complete original key being inserted
// Returns potentially modified node (may be cloned due to COW).
func (m *Memtable) insertNodeWithPrefix(node *Node, prefix []byte, key []byte, fullKey []byte, value *Value) *Node {
	// COW: Clone node if it's shared with snapshots
	node = m.ensureWritable(node)

	// Update cached size
	defer func() {
		node.updateCachedSize()
	}()

	// Build current full key path
	currentKey := make([]byte, len(prefix)+len(node.label))
	copy(currentKey, prefix)
	copy(currentKey[len(prefix):], node.label)

	// If this is a leaf node with a value
	if node.value != nil {
		if bytes.Equal(node.value.data, fullKey) {
			// Replace existing value
			node.value = value
			return node
		}

		// Need to split this leaf into an internal node
		m.splitLeafWithPrefix(node, currentKey, fullKey, value)
		return node
	}

	// Find matching child
	for i, child := range node.children {
		commonLen := commonPrefixLength(key, child.label)

		if commonLen == 0 {
			continue
		}

		if commonLen == len(child.label) {
			// Full match of child label
			if commonLen == len(key) {
				// Exact match - update the child (COW: ensure child is writable)
				child = m.ensureWritable(child)
				child.value = value
				node.children[i] = child // update parent's reference
				return node
			}
			// Continue searching in child - make a copy to avoid shared backing array
			remainingKey := make([]byte, len(key)-commonLen)
			copy(remainingKey, key[commonLen:])
			// COW: child might be cloned, update parent reference
			node.children[i] = m.insertNodeWithPrefix(child, currentKey, remainingKey, fullKey, value)
			return node
		}

		if commonLen == len(key) {
			// Key is a prefix of child label - split the edge
			m.splitEdge(node, i, commonLen, value)
			return node
		}

		// Partial match - split the edge
		m.splitEdge(node, i, commonLen, nil)
		// Make a copy to avoid shared backing array
		remainingKey := make([]byte, len(key)-commonLen)
		copy(remainingKey, key[commonLen:])

		// Insert into the new intermediate node (already writable from splitEdge)
		intermediateNode := node.children[i]
		node.children[i] = m.insertNodeWithPrefix(intermediateNode, currentKey, remainingKey, fullKey, value)
		return node
	}

	// No matching child - create new child with a copy to avoid shared backing array
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	newChild := &Node{
		label: keyCopy,
		value: value,
	}

	// Insert child in sorted order
	node.children = insertSorted(node.children, newChild)
	return node
}

// splitLeafWithPrefix splits a leaf node when it needs to become an internal node.
// currentFullKey is the full key path to this node (prefix + node.label).
// newFullKey is the complete new key being inserted.
func (m *Memtable) splitLeafWithPrefix(leaf *Node, currentFullKey []byte, newFullKey []byte, newValue *Value) {
	existingKey := leaf.value.data
	existingValue := leaf.value

	// Clear the leaf value (it becomes internal)
	leaf.value = nil

	// Find common prefix between the FULL keys
	commonLen := commonPrefixLength(existingKey, newFullKey)

	// Now compute what the remaining suffixes should be RELATIVE to currentFullKey
	// currentFullKey is the path to this node, but leaf.label might be part of existingKey
	// We need to compute the suffix after the common prefix
	prefixLen := len(currentFullKey)

	if commonLen == len(existingKey) {
		// Existing key is a prefix of new key
		leaf.value = existingValue
		// remainingNew is relative to the CURRENT node position (after currentFullKey)
		remainingNew := make([]byte, len(newFullKey)-prefixLen)
		copy(remainingNew, newFullKey[prefixLen:])
		newChild := &Node{
			label: remainingNew,
			value: newValue,
		}
		// FIX: Add to existing children instead of overwriting
		leaf.children = insertSorted(leaf.children, newChild)
	} else if commonLen == len(newFullKey) {
		// New key is a prefix of existing key
		leaf.value = newValue
		// remainingExisting is relative to the CURRENT node position
		remainingExisting := make([]byte, len(existingKey)-prefixLen)
		copy(remainingExisting, existingKey[prefixLen:])
		existingChild := &Node{
			label: remainingExisting,
			value: existingValue,
		}
		// FIX: Add to existing children instead of overwriting
		leaf.children = insertSorted(leaf.children, existingChild)
	} else {
		// Keys diverge - create two children
		// Both are relative to the COMMON PREFIX, not currentFullKey
		// So we need to split at commonLen, create intermediate if needed
		remainingExisting := make([]byte, len(existingKey)-commonLen)
		copy(remainingExisting, existingKey[commonLen:])
		remainingNew := make([]byte, len(newFullKey)-commonLen)
		copy(remainingNew, newFullKey[commonLen:])

		existingChild := &Node{
			label: remainingExisting,
			value: existingValue,
		}

		newChild := &Node{
			label: remainingNew,
			value: newValue,
		}

		// FIX: Add both children to existing children list
		leaf.children = insertSorted(leaf.children, existingChild)
		leaf.children = insertSorted(leaf.children, newChild)
	}
}

// splitEdge splits an edge when inserting a key that partially matches.
func (m *Memtable) splitEdge(parent *Node, childIndex int, splitPoint int,
	intermediateValue *Value) {

	child := parent.children[childIndex]

	// COW: Ensure child is writable before modifying
	child = m.ensureWritable(child)

	// Create intermediate node with common prefix - make a copy to avoid shared backing array
	intermediateLabel := make([]byte, splitPoint)
	copy(intermediateLabel, child.label[:splitPoint])
	intermediate := &Node{
		label: intermediateLabel,
		value: intermediateValue,
	}

	// Update child's label to remaining suffix - make a copy to avoid shared backing array
	childSuffix := make([]byte, len(child.label)-splitPoint)
	copy(childSuffix, child.label[splitPoint:])
	child.label = childSuffix

	// Add child to intermediate node
	intermediate.children = []*Node{child}

	// Replace child in parent with intermediate node
	parent.children[childIndex] = intermediate
}

// RegexSearch performs a regex search on the memtable.
func (m *Memtable) RegexSearch(re *regexp.Regexp) *Iterator {
	m.mu.RLock()
	root := m.root
	m.mu.RUnlock()

	it := &Iterator{
		root:      root,
		regex:     re,
		stack:     []*iterState{{node: root, prefix: nil}},
		current:   nil,
		pathCache: make(map[string]bool, 1000), // Initialize cache with reasonable size
		cacheSize: 1000,                        // Cache up to 1000 path results
	}
	if re != nil {
		if lit, complete := re.LiteralPrefix(); lit != "" {
			it.literal = []byte(lit)
			it.hasLiteral = complete
		}
		it.eng = q.Compile(re)
		// Initialize NFA state at root for product traversal
		if it.eng != nil && len(it.stack) > 0 {
			it.stack[0].nfaState = it.eng.StartState()
		}
	}
	return it
}

// GetAll returns all keys in sorted order (for debugging/testing).
func (m *Memtable) GetAll() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result [][]byte
	m.collectAll(m.root, nil, &result)

	// Sort lexicographically
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i], result[j]) < 0
	})

	return result
}

// collectAll recursively collects all keys.
func (m *Memtable) collectAll(node *Node, prefix []byte, result *[][]byte) {
	// Build current key - create a fresh slice to avoid append reusing backing array
	currentKey := make([]byte, len(prefix)+len(node.label))
	copy(currentKey, prefix)
	copy(currentKey[len(prefix):], node.label)

	// If this node has a value and it's not a tombstone
	if node.value != nil && !node.value.tombstone {
		keyCopy := make([]byte, len(currentKey))
		copy(keyCopy, currentKey)
		*result = append(*result, keyCopy)
	}

	// Recurse to children
	for _, child := range node.children {
		m.collectAll(child, currentKey, result)
	}
}

// Size returns the approximate memory size in bytes.
func (m *Memtable) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

// Count returns the number of entries (including tombstones).
func (m *Memtable) Count() int64 {
	return atomic.LoadInt64(&m.count)
}

// DeletedCount returns the number of tombstones.
func (m *Memtable) DeletedCount() int64 {
	return atomic.LoadInt64(&m.deleted)
}

// Snapshot creates a read-only snapshot of the memtable using Copy-on-Write.
// This is much faster than deep cloning (10-100x) as it only increments a refcount.
// The snapshot shares the tree structure with the memtable until modifications occur.
func (m *Memtable) Snapshot() *Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Increment refcount on root to mark it as shared
	// This prevents the memtable from modifying nodes in-place
	m.incrementRefcountRecursive(m.root)

	return &Snapshot{
		root:    m.root, // Share the tree structure (COW)
		size:    m.size,
		count:   m.count,
		deleted: m.deleted,
	}
}

// incrementRefcountRecursive marks a subtree as shared by incrementing refcounts.
func (m *Memtable) incrementRefcountRecursive(node *Node) {
	if node == nil {
		return
	}
	atomic.AddInt32(&node.refcount, 1)
	for _, child := range node.children {
		m.incrementRefcountRecursive(child)
	}
}

// updateCachedSize updates the cached size of a node.
func (n *Node) updateCachedSize() {
	size := int64(len(n.label))

	if n.value != nil {
		size += int64(len(n.value.data)) + 16 // data + metadata
	}

	for _, child := range n.children {
		size += child.cachedSize
	}

	n.cachedSize = size
}

// commonPrefixLength returns the length of the common prefix between two byte slices.
func commonPrefixLength(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}

	return minLen
}

// insertSorted inserts a node into a sorted slice of nodes.
func insertSorted(nodes []*Node, newNode *Node) []*Node {
	if len(nodes) == 0 {
		return []*Node{newNode}
	}

	// Find insertion point
	idx := sort.Search(len(nodes), func(i int) bool {
		if len(nodes[i].label) == 0 {
			return true
		}
		if len(newNode.label) == 0 {
			return false
		}
		return nodes[i].label[0] >= newNode.label[0]
	})

	// Insert at position
	nodes = append(nodes, nil)
	copy(nodes[idx+1:], nodes[idx:])
	nodes[idx] = newNode

	return nodes
}

// Snapshot represents a read-only snapshot of the memtable.
type Snapshot struct {
	root     *Node
	size     int64
	count    int64
	deleted  int64
	released int32 // atomic flag to prevent double-release
}

// RegexSearch performs a regex search on the snapshot.
func (s *Snapshot) RegexSearch(re *regexp.Regexp) *Iterator {
	it := &Iterator{
		root:      s.root,
		regex:     re,
		stack:     []*iterState{{node: s.root, prefix: nil}},
		current:   nil,
		pathCache: make(map[string]bool, 1000), // Initialize cache with reasonable size
		cacheSize: 1000,                        // Cache up to 1000 path results
	}

	// Initialize NFA engine if regex is provided
	if re != nil {
		if lit, complete := re.LiteralPrefix(); lit != "" {
			it.literal = []byte(lit)
			it.hasLiteral = complete
		}
		it.eng = q.Compile(re)
	}

	return it
}

// GetAll returns all keys in the snapshot.
func (s *Snapshot) GetAll() [][]byte {
	var result [][]byte
	s.collectAll(s.root, nil, &result)

	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i], result[j]) < 0
	})

	return result
}

// GetValue returns the value for a given key.
func (s *Snapshot) GetValue(key []byte) *Value {
	return s.findValue(s.root, key, nil)
}

// findValue recursively searches for a value in the snapshot.
func (s *Snapshot) findValue(node *Node, targetKey, prefix []byte) *Value {
	currentKey := append(prefix, node.label...)

	// Check if this node has the target key
	if bytes.Equal(currentKey, targetKey) {
		return node.value
	}

	// Check if target key could be a child of this node
	if len(targetKey) > len(currentKey) && bytes.HasPrefix(targetKey, currentKey) {
		// Find the child that could contain the target
		for _, child := range node.children {
			if result := s.findValue(child, targetKey, currentKey); result != nil {
				return result
			}
		}
	}

	return nil
}

// collectAll recursively collects all keys from snapshot.
func (s *Snapshot) collectAll(node *Node, prefix []byte, result *[][]byte) {
	// Create a fresh slice to avoid append reusing backing array
	currentKey := make([]byte, len(prefix)+len(node.label))
	copy(currentKey, prefix)
	copy(currentKey[len(prefix):], node.label)

	if node.value != nil && !node.value.tombstone && !node.value.IsExpired() {
		keyCopy := make([]byte, len(currentKey))
		copy(keyCopy, currentKey)
		*result = append(*result, keyCopy)
	}

	for _, child := range node.children {
		s.collectAll(child, currentKey, result)
	}
}

// GetAllWithTombstones returns all keys and their tombstone status from the snapshot.
// DEPRECATED: Use IterateWithTombstones() instead for better memory efficiency.
// This method allocates arrays for all keys which can use significant memory for large snapshots.
func (s *Snapshot) GetAllWithTombstones() ([][]byte, []bool) {
	var keys [][]byte
	var tombs []bool
	var walk func(node *Node, prefix []byte)
	walk = func(node *Node, prefix []byte) {
		// Create a fresh slice to avoid append reusing backing array
		currentKey := make([]byte, len(prefix)+len(node.label))
		copy(currentKey, prefix)
		copy(currentKey[len(prefix):], node.label)

		if node.value != nil && !node.value.IsExpired() {
			k := make([]byte, len(currentKey))
			copy(k, currentKey)
			keys = append(keys, k)
			tombs = append(tombs, node.value.tombstone)
		}

		for _, child := range node.children {
			walk(child, currentKey)
		}
	}
	walk(s.root, nil)
	return keys, tombs
}

// GetAllWithExpiry returns all keys, their tombstone status, and expiry timestamps from the snapshot.
func (s *Snapshot) GetAllWithExpiry() (keys [][]byte, tombs []bool, expiries []int64) {
	var walk func(node *Node, prefix []byte)
	walk = func(node *Node, prefix []byte) {
		// Create a fresh slice to avoid append reusing backing array
		currentKey := make([]byte, len(prefix)+len(node.label))
		copy(currentKey, prefix)
		copy(currentKey[len(prefix):], node.label)

		if node.value != nil && !node.value.IsExpired() {
			k := make([]byte, len(currentKey))
			copy(k, currentKey)
			keys = append(keys, k)
			tombs = append(tombs, node.value.tombstone)
			// Convert time.Time to Unix nanoseconds
			var exp int64
			if !node.value.expiresAt.IsZero() {
				exp = node.value.expiresAt.UnixNano()
			}
			expiries = append(expiries, exp)
		}

		for _, child := range node.children {
			walk(child, currentKey)
		}
	}
	walk(s.root, nil)
	return
}

// SnapshotIterator provides zero-allocation iteration over snapshot keys.
type SnapshotIterator struct {
	stack   []snapshotIterState
	current snapshotIterState
	hasNext bool
}

type snapshotIterState struct {
	node       *Node
	prefix     []byte
	childIndex int
}

// IterateWithTombstones returns an iterator for streaming keys with tombstone status.
// This is more memory-efficient than GetAllWithTombstones() as it doesn't materialize
// all keys in memory at once.
func (s *Snapshot) IterateWithTombstones() *SnapshotIterator {
	return &SnapshotIterator{
		stack:   []snapshotIterState{{node: s.root, prefix: nil, childIndex: 0}},
		hasNext: true,
	}
}

// Next advances to the next key. Returns false when iteration is complete.
func (it *SnapshotIterator) Next() bool {
	for len(it.stack) > 0 {
		// Pop current state
		idx := len(it.stack) - 1
		state := it.stack[idx]

		// Build current key
		currentKey := make([]byte, len(state.prefix)+len(state.node.label))
		copy(currentKey, state.prefix)
		copy(currentKey[len(state.prefix):], state.node.label)

		// If this node has a value and we haven't yielded it yet
		if state.childIndex == 0 && state.node.value != nil && !state.node.value.IsExpired() {
			// Yield this node's value
			it.current = snapshotIterState{
				node:   state.node,
				prefix: currentKey,
			}

			// Push first child if any, and mark that we've processed it
			if len(state.node.children) > 0 {
				// Update parent to skip first child and move to second child next time
				it.stack[idx].childIndex = 2
				// Push first child onto stack
				it.stack = append(it.stack, snapshotIterState{
					node:       state.node.children[0],
					prefix:     currentKey,
					childIndex: 0,
				})
			} else {
				// No children, mark as done
				it.stack[idx].childIndex = 1
			}
			return true
		}

		// Move to next child if available
		// childIndex: 0 = value not processed, 1 = value processed but no children, 2+ = child index (1, 2, 3...)
		if state.childIndex > 1 {
			childIdx := state.childIndex - 2 // child index 0-based: childIndex=2 -> child 0, childIndex=3 -> child 1
			if childIdx < len(state.node.children) {
				// Update parent to process next child
				it.stack[idx].childIndex++

				// Push current child onto stack
				it.stack = append(it.stack, snapshotIterState{
					node:       state.node.children[childIdx],
					prefix:     currentKey,
					childIndex: 0,
				})
			} else {
				// No more children, pop this node
				it.stack = it.stack[:idx]
			}
		} else if state.childIndex == 0 {
			// childIndex == 0 but no value to yield: node is internal-only
			// Move directly to first child
			if len(state.node.children) > 0 {
				it.stack[idx].childIndex = 2
				it.stack = append(it.stack, snapshotIterState{
					node:       state.node.children[0],
					prefix:     currentKey,
					childIndex: 0,
				})
			} else {
				// No value, no children: pop
				it.stack = it.stack[:idx]
			}
		} else {
			// childIndex == 1: value processed, no children
			it.stack = it.stack[:idx]
		}
	}

	return false
}

// Key returns the current key.
func (it *SnapshotIterator) Key() []byte {
	return it.current.prefix
}

// IsTombstone returns true if the current key is a tombstone.
func (it *SnapshotIterator) IsTombstone() bool {
	if it.current.node == nil || it.current.node.value == nil {
		return false
	}
	return it.current.node.value.tombstone
}

// Value returns the current value metadata.
func (it *SnapshotIterator) Value() *Value {
	if it.current.node == nil {
		return nil
	}
	return it.current.node.value
}

// GetAllWithMeta returns keys, tombstone flags, and absolute expiry (Unix nanos, 0 if none).
func (s *Snapshot) GetAllWithMeta() ([][]byte, []bool, []int64) {
	var keys [][]byte
	var tombs []bool
	var exps []int64
	var walk func(node *Node, prefix []byte)
	walk = func(node *Node, prefix []byte) {
		// Create a fresh slice to avoid append reusing backing array
		currentKey := make([]byte, len(prefix)+len(node.label))
		copy(currentKey, prefix)
		copy(currentKey[len(prefix):], node.label)

		if node.value != nil && !node.value.IsExpired() {
			k := make([]byte, len(currentKey))
			copy(k, currentKey)
			keys = append(keys, k)
			tombs = append(tombs, node.value.tombstone)
			var e int64
			if !node.value.expiresAt.IsZero() {
				e = node.value.expiresAt.UnixNano()
			}
			exps = append(exps, e)
		}
		for _, child := range node.children {
			walk(child, currentKey)
		}
	}
	walk(s.root, nil)
	return keys, tombs, exps
}

// Release decrements refcounts for all nodes in the snapshot.
// This allows the memtable to reclaim memory from nodes that are no longer referenced.
// It's safe to call Release multiple times (idempotent).
func (s *Snapshot) Release() {
	// Use atomic CAS to ensure we only release once
	if !atomic.CompareAndSwapInt32(&s.released, 0, 1) {
		return // Already released
	}

	s.decrementRefcountRecursive(s.root)
}

// decrementRefcountRecursive decrements refcounts for a subtree.
func (s *Snapshot) decrementRefcountRecursive(node *Node) {
	if node == nil {
		return
	}
	atomic.AddInt32(&node.refcount, -1)
	for _, child := range node.children {
		s.decrementRefcountRecursive(child)
	}
}

// Size returns the size of the snapshot.
func (s *Snapshot) Size() int64 {
	return s.size
}

// Count returns the number of entries in the snapshot.
func (s *Snapshot) Count() int64 {
	return s.count
}

// DeletedCount returns the number of tombstones in the snapshot.
func (s *Snapshot) DeletedCount() int64 {
	return s.deleted
}
