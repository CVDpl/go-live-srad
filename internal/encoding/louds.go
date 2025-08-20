package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

// LOUDS (Level-Order Unary Degree Sequence) encodes a tree structure.
type LOUDS struct {
	bits     *BitVector  // The LOUDS bit sequence
	rs       *RankSelect // Rank/Select support
	labels   []byte      // Edge labels
	values   [][]byte    // Values for leaf nodes
	numNodes uint64      // Total number of nodes

	// Lazy child lookup index for high-degree nodes: node -> (label -> child)
	childIdx map[uint64]map[byte]uint64
	idxMu    sync.RWMutex
}

// TrieNode represents a node in a trie for LOUDS encoding.
type TrieNode struct {
	Label    byte
	Children []*TrieNode
	IsLeaf   bool
	Value    []byte
}

// NewLOUDS creates a LOUDS encoding from a trie.
func NewLOUDS(root *TrieNode) *LOUDS {
	if root == nil {
		return &LOUDS{
			bits:     NewBitVector(2),
			numNodes: 0,
			childIdx: make(map[uint64]map[byte]uint64),
		}
	}

	// Count nodes and prepare structures
	nodeCount := countNodes(root)
	bitLen := nodeCount*2 + 10 // Extra space for super root

	louds := &LOUDS{
		bits:     NewBitVector(bitLen),
		labels:   make([]byte, 0, nodeCount),
		values:   make([][]byte, 0, nodeCount),
		numNodes: nodeCount,
		childIdx: make(map[uint64]map[byte]uint64),
	}

	// Build LOUDS encoding using BFS
	louds.buildFromTrie(root)

	// Create rank/select support
	louds.rs = NewRankSelect(louds.bits)

	return louds
}

// buildFromTrie builds LOUDS encoding from a trie using BFS.
func (l *LOUDS) buildFromTrie(root *TrieNode) {
	// Super root: 10 (has one child - the actual root)
	l.bits.Set(0)
	l.bits.Clear(1)

	// BFS traversal
	queue := []*TrieNode{root}
	bitPos := uint64(2)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		// Encode node's children
		for _, child := range node.Children {
			l.bits.Set(bitPos)
			bitPos++
			l.labels = append(l.labels, child.Label)

			if child.IsLeaf {
				l.values = append(l.values, child.Value)
			} else {
				l.values = append(l.values, nil)
			}

			queue = append(queue, child)
		}

		// End of children marker
		l.bits.Clear(bitPos)
		bitPos++
	}
}

// FirstChild returns the first child of node i.
func (l *LOUDS) FirstChild(i uint64) uint64 {
	if l.rs == nil {
		return 0
	}

	// First child position = rank0(select1(i+1)) + 1
	nodeStart := l.rs.Select1(i + 1)
	if nodeStart >= l.bits.Length() {
		return 0 // No children
	}

	// Check if there's at least one child
	if !l.bits.Get(nodeStart + 1) {
		return 0 // No children
	}

	return l.rs.Rank0(nodeStart + 1)
}

// NextSibling returns the next sibling of node i.
func (l *LOUDS) NextSibling(i uint64) uint64 {
	if l.rs == nil || i == 0 {
		return 0
	}

	// Next sibling position = i + 1 if bit at select1(i+1)+1 is 1
	nodePos := l.rs.Select1(i + 1)
	if nodePos+1 >= l.bits.Length() || !l.bits.Get(nodePos+1) {
		return 0 // No next sibling
	}

	return i + 1
}

// Parent returns the parent of node i.
func (l *LOUDS) Parent(i uint64) uint64 {
	if l.rs == nil || i <= 1 {
		return 0 // Root has no parent
	}

	// Parent position = rank1(select0(i))
	zeroPos := l.rs.Select0(i)
	if zeroPos >= l.bits.Length() {
		return 0
	}

	return l.rs.Rank1(zeroPos)
}

// GetLabel returns the label of the edge to node i.
func (l *LOUDS) GetLabel(i uint64) byte {
	if i == 0 || i > uint64(len(l.labels)) {
		return 0
	}
	return l.labels[i-1]
}

// GetValue returns the value associated with node i.
func (l *LOUDS) GetValue(i uint64) []byte {
	if i == 0 || i > uint64(len(l.values)) {
		return nil
	}
	return l.values[i-1]
}

// IsLeaf returns true if node i is a leaf.
func (l *LOUDS) IsLeaf(i uint64) bool {
	if i == 0 || i > uint64(len(l.values)) {
		return false
	}
	return l.values[i-1] != nil
}

// NumNodes returns the total number of nodes.
func (l *LOUDS) NumNodes() uint64 {
	return l.numNodes
}

// Search performs a search for the given key.
func (l *LOUDS) Search(key []byte) ([]byte, bool) {
	if len(key) == 0 || l.rs == nil {
		return nil, false
	}

	node := uint64(1) // Start at root

	for i := 0; i < len(key); i++ {
		child := l.findChild(node, key[i])
		if child == 0 {
			return nil, false
		}
		node = child
	}

	// Check if final node is a leaf
	if l.IsLeaf(node) {
		return l.GetValue(node), true
	}

	return nil, false
}

// findChild locates the child of 'node' with edge label 'want'.
// For high-degree nodes it builds a tiny map[byte]child lazily and reuses it.
func (l *LOUDS) findChild(node uint64, want byte) uint64 {
	if node == 0 {
		return 0
	}
	// Fast path: existing index
	l.idxMu.RLock()
	if l.childIdx != nil {
		if idx, ok := l.childIdx[node]; ok {
			c := idx[want]
			l.idxMu.RUnlock()
			return c
		}
	}
	l.idxMu.RUnlock()

	// Enumerate children once
	first := l.FirstChild(node)
	if first == 0 {
		return 0
	}
	// Collect children to determine degree and optionally build index
	type kv struct {
		ch uint64
		lb byte
	}
	list := make([]kv, 0, 8)
	for c := first; c != 0; c = l.NextSibling(c) {
		list = append(list, kv{ch: c, lb: l.GetLabel(c)})
	}
	// Linear search
	for _, x := range list {
		if x.lb == want {
			// If degree high, build index for future
			if len(list) >= 8 {
				m := make(map[byte]uint64, len(list))
				for _, y := range list {
					if _, ok := m[y.lb]; !ok {
						m[y.lb] = y.ch
					}
				}
				l.idxMu.Lock()
				if l.childIdx == nil {
					l.childIdx = make(map[uint64]map[byte]uint64)
				}
				if _, ok := l.childIdx[node]; !ok {
					l.childIdx[node] = m
				}
				l.idxMu.Unlock()
			}
			return x.ch
		}
	}
	// For high-degree miss, still build index to accelerate future queries
	if len(list) >= 8 {
		m := make(map[byte]uint64, len(list))
		for _, y := range list {
			if _, ok := m[y.lb]; !ok {
				m[y.lb] = y.ch
			}
		}
		l.idxMu.Lock()
		if l.childIdx == nil {
			l.childIdx = make(map[uint64]map[byte]uint64)
		}
		if _, ok := l.childIdx[node]; !ok {
			l.childIdx[node] = m
		}
		l.idxMu.Unlock()
	}
	return 0
}

// Marshal serializes the LOUDS structure.
func (l *LOUDS) Marshal() []byte {
	var buf bytes.Buffer

	// Write number of nodes
	binary.Write(&buf, binary.LittleEndian, l.numNodes)

	// Write bit vector
	bvData := l.bits.Marshal()
	binary.Write(&buf, binary.LittleEndian, uint64(len(bvData)))
	buf.Write(bvData)

	// Write labels
	binary.Write(&buf, binary.LittleEndian, uint64(len(l.labels)))
	buf.Write(l.labels)

	// Write values
	binary.Write(&buf, binary.LittleEndian, uint64(len(l.values)))
	for _, val := range l.values {
		if val == nil {
			binary.Write(&buf, binary.LittleEndian, uint32(0))
		} else {
			binary.Write(&buf, binary.LittleEndian, uint32(len(val)))
			buf.Write(val)
		}
	}

	return buf.Bytes()
}

// UnmarshalLOUDS deserializes a LOUDS structure.
func UnmarshalLOUDS(data []byte) (*LOUDS, error) {
	idx := 0
	need := func(n int) error {
		if idx+n > len(data) {
			return fmt.Errorf("louds: truncated data")
		}
		return nil
	}

	// numNodes
	if err := need(8); err != nil {
		return nil, err
	}
	numNodes := binary.LittleEndian.Uint64(data[idx:])
	idx += 8

	// bitvector
	if err := need(8); err != nil {
		return nil, err
	}
	bvLen := int(binary.LittleEndian.Uint64(data[idx:]))
	idx += 8
	if err := need(bvLen); err != nil {
		return nil, err
	}
	bvData := data[idx : idx+bvLen]
	idx += bvLen
	bv := UnmarshalBitVector(bvData)

	// labels
	if err := need(8); err != nil {
		return nil, err
	}
	labelsLen := int(binary.LittleEndian.Uint64(data[idx:]))
	idx += 8
	if err := need(labelsLen); err != nil {
		return nil, err
	}
	labels := data[idx : idx+labelsLen]
	idx += labelsLen

	// values
	if err := need(8); err != nil {
		return nil, err
	}
	valuesLen := int(binary.LittleEndian.Uint64(data[idx:]))
	idx += 8
	values := make([][]byte, valuesLen)
	for i := 0; i < valuesLen; i++ {
		if err := need(4); err != nil {
			return nil, err
		}
		l := int(binary.LittleEndian.Uint32(data[idx:]))
		idx += 4
		if l > 0 {
			if err := need(l); err != nil {
				return nil, err
			}
			values[i] = data[idx : idx+l]
			idx += l
		} else {
			values[i] = nil
		}
	}

	louds := &LOUDS{
		bits:     bv,
		rs:       NewRankSelect(bv),
		labels:   labels,
		values:   values,
		numNodes: numNodes,
		childIdx: make(map[uint64]map[byte]uint64),
	}
	return louds, nil
}

// countNodes counts the total number of nodes in a trie.
func countNodes(node *TrieNode) uint64 {
	if node == nil {
		return 0
	}

	count := uint64(1)
	for _, child := range node.Children {
		count += countNodes(child)
	}

	return count
}

// BuildTrieFromKeys builds a trie from a sorted list of keys.
func BuildTrieFromKeys(keys [][]byte, values [][]byte) *TrieNode {
	root := &TrieNode{}

	for i, key := range keys {
		var value []byte
		if i < len(values) {
			value = values[i]
		}
		insertIntoTrie(root, key, value)
	}

	return root
}

// insertIntoTrie inserts a key-value pair into the trie.
func insertIntoTrie(node *TrieNode, key []byte, value []byte) {
	if len(key) == 0 {
		node.IsLeaf = true
		node.Value = value
		return
	}

	// Find or create child with matching first byte
	var child *TrieNode
	for _, c := range node.Children {
		if c.Label == key[0] {
			child = c
			break
		}
	}

	if child == nil {
		child = &TrieNode{
			Label: key[0],
		}
		node.Children = append(node.Children, child)
	}

	insertIntoTrie(child, key[1:], value)
}
