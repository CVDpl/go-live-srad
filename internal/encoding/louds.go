package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// LOUDS (Level-Order Unary Degree Sequence) encodes a tree structure.
type LOUDS struct {
	bits     *BitVector  // The LOUDS bit sequence
	rs       *RankSelect // Rank/Select support
	labels   []byte      // Edge labels
	values   [][]byte    // Values for leaf nodes (legacy; may be empty)
	numNodes uint64      // Total number of nodes

	// Lazy child lookup index for high-degree nodes: node -> (label -> child)
	childIdx map[uint64]map[byte]uint64
	idxMu    sync.RWMutex

	// Accepting (leaf) nodes; when present, replaces values as leaf marker
	accept *BitVector
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
			accept:   NewBitVector(2),
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
		accept:   NewBitVector(nodeCount + 2),
	}

	// Build LOUDS encoding using BFS
	louds.buildFromTrie(root)

	// Create rank/select support
	louds.rs = NewRankSelect(louds.bits)

	return louds
}

// NewLOUDSNoRS creates a LOUDS encoding from a trie, skipping rank/select build.
// Use this in offline builders where only Marshal() is needed; readers will
// reconstruct rank/select upon load.
func NewLOUDSNoRS(root *TrieNode) *LOUDS {
	if root == nil {
		return &LOUDS{
			bits:     NewBitVector(2),
			numNodes: 0,
			childIdx: make(map[uint64]map[byte]uint64),
			accept:   NewBitVector(2),
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
		accept:   NewBitVector(nodeCount + 2),
	}

	// Build LOUDS encoding using BFS
	louds.buildFromTrie(root)

	// Intentionally skip building rank/select here
	return louds
}

// NewLOUDSFromSortedKeys builds LOUDS directly from lexicographically sorted keys,
// without constructing an intermediate trie. It performs two passes:
// 1) Count edges/nodes by level-wise partitioning on next-byte groups
// 2) Preallocate and emit bitvector runs, labels, and accept bits
// The returned LOUDS does not include Rank/Select structures; readers rebuild RS.
func NewLOUDSFromSortedKeys(keys [][]byte) *LOUDS {
	if len(keys) == 0 {
		return &LOUDS{
			bits:     NewBitVector(2),
			numNodes: 0,
			childIdx: make(map[uint64]map[byte]uint64),
		}
	}

	type group struct {
		start, end int
		depth      int
	}

	// Pass 1: count edges (children) to size structures
	edgesTotal := 0
	cur := []group{{0, len(keys), 0}}
	for len(cur) > 0 {
		next := make([]group, 0, len(cur)*2)
		for _, g := range cur {
			i := g.start
			// Skip keys that end exactly at this depth (they mark leaf at current node)
			for i < g.end {
				if len(keys[i]) <= g.depth {
					i++
					continue
				}
				break
			}
			j := i
			for j < g.end {
				if len(keys[j]) <= g.depth {
					j++
					continue
				}
				// start of a child bucket
				lb := keys[j][g.depth]
				k := j + 1
				for k < g.end {
					if len(keys[k]) <= g.depth {
						k++
						continue
					}
					if keys[k][g.depth] != lb {
						break
					}
					k++
				}
				edgesTotal++
				// child group is [j,k) at depth+1
				next = append(next, group{j, k, g.depth + 1})
				j = k
			}
		}
		cur = next
	}

	nodesTotal := edgesTotal + 1
	bitLen := 2*nodesTotal + 2
	louds := &LOUDS{
		bits:     NewBitVector(uint64(bitLen)),
		labels:   make([]byte, 0, edgesTotal),
		values:   nil,
		numNodes: uint64(nodesTotal),
		childIdx: make(map[uint64]map[byte]uint64),
		accept:   NewBitVector(uint64(nodesTotal) + 2),
	}

	// Pass 2: emit LOUDS bit runs, labels, and accept
	louds.bits.Set(0)
	louds.bits.Clear(1)
	bitPos := uint64(2)
	nodeIdx := uint64(1) // child nodes indices (1-based)
	cur = []group{{0, len(keys), 0}}
	for len(cur) > 0 {
		next := make([]group, 0, len(cur)*2)
		for _, g := range cur {
			// Enumerate child buckets in byte order (already ensured by sorted keys)
			i := g.start
			// Skip keys that end here (leaf at current prefix)
			for i < g.end {
				if len(keys[i]) <= g.depth {
					i++
					continue
				}
				break
			}
			// Count children first to set run
			// But we can set runs iteratively by accumulating contiguous buckets
			// We will collect buckets to set run once
			childStarts := make([]int, 0, 8)
			childEnds := make([]int, 0, 8)
			childLabels := make([]byte, 0, 8)
			for i < g.end {
				if len(keys[i]) <= g.depth {
					i++
					continue
				}
				lb := keys[i][g.depth]
				j := i + 1
				for j < g.end {
					if len(keys[j]) <= g.depth {
						j++
						continue
					}
					if keys[j][g.depth] != lb {
						break
					}
					j++
				}
				childStarts = append(childStarts, i)
				childEnds = append(childEnds, j)
				childLabels = append(childLabels, lb)
				i = j
			}
			// Set LOUDS 1-run for this node's children
			if n := uint64(len(childLabels)); n > 0 {
				louds.bits.SetRun(bitPos, n)
				bitPos += n
				// Append labels and accept bits, enqueue children
				for idx := 0; idx < len(childLabels); idx++ {
					louds.labels = append(louds.labels, childLabels[idx])
					// Accept: exists key with exact length depth+1 in [start,end)
					s, e := childStarts[idx], childEnds[idx]
					leaf := false
					for t := s; t < e; t++ {
						if len(keys[t]) == g.depth+1 {
							leaf = true
							break
						}
						// early break because sorted by bytes first, but lengths may vary; we scan segment fully
					}
					if leaf {
						louds.accept.Set(nodeIdx)
					}
					next = append(next, group{s, e, g.depth + 1})
					nodeIdx++
				}
			}
			// End-of-children marker
			louds.bits.Clear(bitPos)
			bitPos++
		}
		cur = next
	}

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
	// LOUDS node indices are 1-based and follow children discovery order
	nodeIdx := uint64(1)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		// Encode node's children
		children := node.Children
		if len(children) > 0 {
			// Set a contiguous run of 1-bits for all children edges
			l.bits.SetRun(bitPos, uint64(len(children)))
			bitPos += uint64(len(children))
			for _, child := range children {
				l.labels = append(l.labels, child.Label)
				if child.IsLeaf && l.accept != nil {
					l.accept.Set(nodeIdx)
				} else if child.IsLeaf {
					// Legacy fallback path
					l.values = append(l.values, child.Value)
				} else {
					l.values = append(l.values, nil)
				}
				nodeIdx++
				queue = append(queue, child)
			}
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
	if i == 0 {
		return false
	}
	// Prefer accept bitvector when available
	if l.accept != nil && i < l.accept.Length() {
		return l.accept.Get(i)
	}
	if i > uint64(len(l.values)) {
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

	// Write values (legacy): now always zero entries
	binary.Write(&buf, binary.LittleEndian, uint64(0))

	// Write accept bitvector (optional)
	if l.accept != nil {
		acc := l.accept.Marshal()
		binary.Write(&buf, binary.LittleEndian, uint64(len(acc)))
		buf.Write(acc)
	} else {
		binary.Write(&buf, binary.LittleEndian, uint64(0))
	}

	return buf.Bytes()
}

// WriteTo streams the LOUDS structure to w using the same format as Marshal().
func (l *LOUDS) WriteTo(w io.Writer) (int64, error) {
	var written int64
	// Write number of nodes
	if err := binary.Write(w, binary.LittleEndian, l.numNodes); err != nil {
		return written, err
	}
	written += 8
	// Write bit vector length (in bytes) then contents (length + words)
	bvData := l.bits.Marshal()
	if err := binary.Write(w, binary.LittleEndian, uint64(len(bvData))); err != nil {
		return written, err
	}
	written += 8
	if n, err := w.Write(bvData); err != nil {
		return written, err
	} else {
		written += int64(n)
	}
	// Write labels
	if err := binary.Write(w, binary.LittleEndian, uint64(len(l.labels))); err != nil {
		return written, err
	}
	written += 8
	if len(l.labels) > 0 {
		n, err := w.Write(l.labels)
		if err != nil {
			return written, err
		}
		written += int64(n)
	}
	// Write values (legacy): zero entries
	if err := binary.Write(w, binary.LittleEndian, uint64(0)); err != nil {
		return written, err
	}
	written += 8
	// Write accept bitvector
	if l.accept != nil {
		acc := l.accept.Marshal()
		if err := binary.Write(w, binary.LittleEndian, uint64(len(acc))); err != nil {
			return written, err
		}
		written += 8
		if n, err := w.Write(acc); err != nil {
			return written, err
		} else {
			written += int64(n)
		}
	} else {
		if err := binary.Write(w, binary.LittleEndian, uint64(0)); err != nil {
			return written, err
		}
		written += 8
	}
	return written, nil
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

	// values (legacy)
	if err := need(8); err != nil {
		return nil, err
	}
	valuesLen := int(binary.LittleEndian.Uint64(data[idx:]))
	idx += 8
	values := make([][]byte, 0)
	if valuesLen > 0 {
		values = make([][]byte, valuesLen)
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
	}

	// accept bitvector (optional)
	var accept *BitVector
	if idx+8 <= len(data) {
		accLen := int(binary.LittleEndian.Uint64(data[idx:]))
		idx += 8
		if accLen > 0 {
			if err := need(accLen); err != nil {
				return nil, err
			}
			accept = UnmarshalBitVector(data[idx : idx+accLen])
			idx += accLen
		}
	}

	louds := &LOUDS{
		bits:     bv,
		rs:       NewRankSelect(bv),
		labels:   labels,
		values:   values,
		numNodes: numNodes,
		childIdx: make(map[uint64]map[byte]uint64),
		accept:   accept,
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
