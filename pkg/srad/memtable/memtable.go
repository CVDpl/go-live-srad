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

func (v *Value) Tombstone() bool { return v.tombstone }

// New creates a new empty memtable.
func New() *Memtable {
	return &Memtable{
		root: &Node{},
	}
}

// Insert adds a key to the memtable.
func (m *Memtable) Insert(key []byte) error {
	return m.InsertWithTTL(key, 0)
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

	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)

	// Make a copy of the key to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

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
	m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
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
	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	value := &Value{
		data:      keyCopy,
		seqNum:    seqNum,
		tombstone: false,
		expiresAt: expiresAt,
	}
	oldSize := m.root.cachedSize
	m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
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

	m.mu.Lock()
	defer m.mu.Unlock()

	seqNum := atomic.AddUint64(&m.seqNum, 1)

	// Make a copy of the key
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	value := &Value{
		data:      keyCopy,
		seqNum:    seqNum,
		tombstone: true,
	}

	oldSize := m.root.cachedSize
	m.insertNodeWithPrefix(m.root, nil, keyCopy, keyCopy, value)
	newSize := m.root.cachedSize

	atomic.AddInt64(&m.size, newSize-oldSize)
	atomic.AddInt64(&m.deleted, 1)

	return nil
}

// insertNodeWithPrefix inserts a value into the tree rooted at node, tracking the prefix.
// prefix: the path from root to this node
// key: the remaining key suffix to insert
// fullKey: the complete original key being inserted
func (m *Memtable) insertNodeWithPrefix(node *Node, prefix []byte, key []byte, fullKey []byte, value *Value) {
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
			return
		}

		// Need to split this leaf into an internal node
		m.splitLeafWithPrefix(node, currentKey, fullKey, value)
		return
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
				// Exact match - update the child
				child.value = value
				return
			}
			// Continue searching in child - make a copy to avoid shared backing array
			remainingKey := make([]byte, len(key)-commonLen)
			copy(remainingKey, key[commonLen:])
			m.insertNodeWithPrefix(child, currentKey, remainingKey, fullKey, value)
			return
		}

		if commonLen == len(key) {
			// Key is a prefix of child label - split the edge
			m.splitEdge(node, i, commonLen, value)
			return
		}

		// Partial match - split the edge
		m.splitEdge(node, i, commonLen, nil)
		// Make a copy to avoid shared backing array
		remainingKey := make([]byte, len(key)-commonLen)
		copy(remainingKey, key[commonLen:])

		// Insert into the new intermediate node
		intermediateNode := node.children[i]
		m.insertNodeWithPrefix(intermediateNode, currentKey, remainingKey, fullKey, value)
		return
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
		leaf.children = []*Node{newChild}
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
		leaf.children = []*Node{existingChild}
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

		// Sort children by first byte
		if remainingExisting[0] < remainingNew[0] {
			leaf.children = []*Node{existingChild, newChild}
		} else {
			leaf.children = []*Node{newChild, existingChild}
		}
	}
}

// splitEdge splits an edge when inserting a key that partially matches.
func (m *Memtable) splitEdge(parent *Node, childIndex int, splitPoint int,
	intermediateValue *Value) {

	child := parent.children[childIndex]

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

// Snapshot creates a read-only snapshot of the memtable.
func (m *Memtable) Snapshot() *Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &Snapshot{
		root:    m.cloneNode(m.root),
		size:    m.size,
		count:   m.count,
		deleted: m.deleted,
	}
}

// cloneNode creates a deep copy of a node and its subtree.
func (m *Memtable) cloneNode(node *Node) *Node {
	if node == nil {
		return nil
	}

	clone := &Node{
		label:      node.label, // Labels are immutable, safe to share
		value:      node.value, // Values are immutable, safe to share
		cachedSize: node.cachedSize,
	}

	if len(node.children) > 0 {
		clone.children = make([]*Node, len(node.children))
		for i, child := range node.children {
			clone.children[i] = m.cloneNode(child)
		}
	}

	return clone
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
	root    *Node
	size    int64
	count   int64
	deleted int64
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
