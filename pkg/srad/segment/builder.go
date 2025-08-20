package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/internal/encoding"
	"github.com/CVDpl/go-live-srad/internal/filters"
	"github.com/CVDpl/go-live-srad/pkg/srad/memtable"
	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
)

// Builder creates immutable segment files from sorted key-value pairs.
type Builder struct {
	segmentID uint64
	level     int
	dir       string

	// Collected data
	keys       [][]byte
	values     [][]byte
	tombstones []bool
	// For future: persist tombstones for merge policies
	tombstoneKeys [][]byte

	// Statistics
	minKey []byte
	maxKey []byte

	// Options controlling emission
	dropTombstones bool

	// Options
	logger common.Logger

	// Derived filter metadata for manifest/metadata
	filterBloomBits     uint64
	filterBloomK        uint32
	filterBloomFPR      float64
	filterTrigramBits   uint64
	filterTrigramScheme string
}

// NewBuilder creates a new segment builder.
func NewBuilder(segmentID uint64, level int, dir string, logger common.Logger) *Builder {
	if logger == nil {
		logger = &NullLogger{}
	}

	return &Builder{
		segmentID: segmentID,
		level:     level,
		dir:       dir,
		logger:    logger,
	}
}

// DropAllTombstones instructs the builder to omit writing tombstones.dat
// and to not derive metadata key range from tombstone keys.
func (b *Builder) DropAllTombstones() { b.dropTombstones = true }

// AddFromMemtable adds all entries from a memtable snapshot.
func (b *Builder) AddFromMemtable(snapshot *memtable.Snapshot) error {
	// Get all keys with tombstone flags
	allKeys, tombs := snapshot.GetAllWithTombstones()
	// Keep pairs aligned while sorting
	type pair struct {
		k []byte
		t bool
	}
	pairs := make([]pair, len(allKeys))
	for i := range allKeys {
		pairs[i] = pair{k: allKeys[i], t: tombs[i]}
	}
	sort.SliceStable(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })

	// Add entries: drop tombstones from values, but record keys for future policies
	for _, p := range pairs {
		val := snapshot.GetValue(p.k)
		if val == nil || val.IsExpired() {
			continue
		}
		if val.Tombstone() {
			kc := make([]byte, len(p.k))
			copy(kc, p.k)
			b.tombstoneKeys = append(b.tombstoneKeys, kc)
			continue
		}
		if err := b.Add(p.k, p.k, false); err != nil {
			return err
		}
	}

	return nil
}

// Add adds a key-value pair to the builder.
func (b *Builder) Add(key, value []byte, tombstone bool) error {
	// Skip tombstones for now (they're handled by shadowing in merge)
	if tombstone {
		return nil
	}

	// Make copies to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.keys = append(b.keys, keyCopy)
	b.values = append(b.values, valueCopy)
	b.tombstones = append(b.tombstones, tombstone)

	// Update min/max keys
	if b.minKey == nil || bytes.Compare(key, b.minKey) < 0 {
		b.minKey = keyCopy
	}
	if b.maxKey == nil || bytes.Compare(key, b.maxKey) > 0 {
		b.maxKey = keyCopy
	}

	return nil
}

// AddTombstone records a deletion for the given key (used during compaction/merge).
func (b *Builder) AddTombstone(key []byte) {
	kc := make([]byte, len(key))
	copy(kc, key)
	b.tombstoneKeys = append(b.tombstoneKeys, kc)
}

// Build creates the segment files.
func (b *Builder) Build() (*Metadata, error) {
	if len(b.keys) == 0 && len(b.tombstoneKeys) == 0 {
		return nil, fmt.Errorf("no keys to build segment")
	}

	// Create segment directory
	segmentDir := filepath.Join(b.dir, fmt.Sprintf("%016d", b.segmentID))
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create segment directory: %w", err)
	}

	// Create filters directory
	filtersDir := filepath.Join(segmentDir, "filters")
	if err := os.MkdirAll(filtersDir, 0755); err != nil {
		return nil, fmt.Errorf("create filters directory: %w", err)
	}

	b.logger.Info("building segment", "id", b.segmentID, "keys", len(b.keys))

	// Build trie structure (simplified for now)
	trie := b.buildTrie()

	// Build LOUDS representation
	loudsPath := filepath.Join(segmentDir, "index.louds")
	if err := b.buildLOUDS(trie, loudsPath); err != nil {
		return nil, fmt.Errorf("build LOUDS: %w", err)
	}

	// Build edges
	edgesPath := filepath.Join(segmentDir, "index.edges")
	if err := b.buildEdges(trie, edgesPath); err != nil {
		return nil, fmt.Errorf("build edges: %w", err)
	}

	// Build accept states
	acceptPath := filepath.Join(segmentDir, "index.accept")
	if err := b.buildAccept(trie, acceptPath); err != nil {
		return nil, fmt.Errorf("build accept: %w", err)
	}

	// Build tail mapping (simplified - no tails for now)
	tmapPath := filepath.Join(segmentDir, "index.tmap")
	if err := b.buildTMap(tmapPath); err != nil {
		return nil, fmt.Errorf("build tmap: %w", err)
	}

	// Build tails data (simplified - no tails for now)
	tailsPath := filepath.Join(segmentDir, "tails.dat")
	if err := b.buildTails(tailsPath); err != nil {
		return nil, fmt.Errorf("build tails: %w", err)
	}

	// Build filters
	if err := b.buildFilters(filtersDir); err != nil {
		return nil, fmt.Errorf("build filters: %w", err)
	}

	// Write keys file for simple iteration
	keysPath := filepath.Join(segmentDir, "keys.dat")
	if err := b.writeKeysFile(keysPath); err != nil {
		return nil, fmt.Errorf("write keys.dat: %w", err)
	}
	// Optionally write tombstones file (can be omitted when unnecessary)
	if len(b.tombstoneKeys) > 0 && !b.dropTombstones {
		tombPath := filepath.Join(segmentDir, "tombstones.dat")
		if err := b.writeTombstonesFile(tombPath); err != nil {
			return nil, fmt.Errorf("write tombstones.dat: %w", err)
		}
	}
	// Create metadata
	metadata := NewMetadata(b.segmentID, b.level)
	// Ensure key range is populated even for tombstone-only segments
	if b.minKey != nil && b.maxKey != nil {
		metadata.SetKeyRange(b.minKey, b.maxKey)
	} else if len(b.tombstoneKeys) > 0 && !b.dropTombstones {
		minK := b.tombstoneKeys[0]
		maxK := b.tombstoneKeys[0]
		for _, k := range b.tombstoneKeys[1:] {
			if bytes.Compare(k, minK) < 0 {
				minK = k
			}
			if bytes.Compare(k, maxK) > 0 {
				maxK = k
			}
		}
		metadata.SetKeyRange(minK, maxK)
	} else {
		metadata.SetKeyRange([]byte{}, []byte{})
	}
	metadata.Counts = Counts{
		Nodes:    uint64(len(b.keys)),
		Edges:    uint64(len(b.keys)),
		Tails:    0,
		Accepted: uint64(len(b.keys)),
	}
	// Populate filters section
	if b.filterBloomBits > 0 && b.filterBloomK > 0 {
		metadata.Filters.PrefixBloom = &BloomFilter{Bits: b.filterBloomBits, K: b.filterBloomK, FPR: b.filterBloomFPR}
	}
	if b.filterTrigramBits > 0 {
		metadata.Filters.Trigram = &TrigramFilter{Bits: b.filterTrigramBits, Scheme: b.filterTrigramScheme}
	}
	// Compute BLAKE3 for key files we generate here
	metadata.Blake3 = map[string]string{}
	if h, err := utils.ComputeBLAKE3File(loudsPath); err == nil {
		metadata.Blake3["index.louds"] = h
	}
	if h, err := utils.ComputeBLAKE3File(edgesPath); err == nil {
		metadata.Blake3["index.edges"] = h
	}
	if h, err := utils.ComputeBLAKE3File(acceptPath); err == nil {
		metadata.Blake3["index.accept"] = h
	}
	if h, err := utils.ComputeBLAKE3File(tmapPath); err == nil {
		metadata.Blake3["index.tmap"] = h
	}
	if h, err := utils.ComputeBLAKE3File(tailsPath); err == nil {
		metadata.Blake3["tails.dat"] = h
	}
	if h, err := utils.ComputeBLAKE3File(filepath.Join(filtersDir, "prefix.bf")); err == nil {
		metadata.Blake3["filters/prefix.bf"] = h
	}
	if h, err := utils.ComputeBLAKE3File(filepath.Join(filtersDir, "tri.bits")); err == nil {
		metadata.Blake3["filters/tri.bits"] = h
	}
	if h, err := utils.ComputeBLAKE3File(keysPath); err == nil {
		metadata.Blake3["keys.dat"] = h
	}
	if len(b.tombstoneKeys) > 0 && !b.dropTombstones {
		if h, err := utils.ComputeBLAKE3File(filepath.Join(segmentDir, "tombstones.dat")); err == nil {
			metadata.Blake3["tombstones.dat"] = h
		}
	}

	// Save metadata
	metadataPath := filepath.Join(segmentDir, "segment.json")
	if err := metadata.SaveToFile(metadataPath); err != nil {
		return nil, fmt.Errorf("save metadata: %w", err)
	}

	b.logger.Info("segment built successfully", "id", b.segmentID)

	return metadata, nil
}

func (b *Builder) writeKeysFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write common header with magic and version
	if err := WriteCommonHeader(f, common.MagicKeys, common.VersionSegment); err != nil {
		return err
	}

	// Write count
	count := uint32(len(b.keys))
	if err := binary.Write(f, binary.LittleEndian, count); err != nil {
		return err
	}
	// Write each key (len + bytes)
	for _, k := range b.keys {
		kl := uint32(len(k))
		if err := binary.Write(f, binary.LittleEndian, kl); err != nil {
			return err
		}
		if _, err := f.Write(k); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (b *Builder) writeTombstonesFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write common header with magic and version
	if err := WriteCommonHeader(f, common.MagicTombs, common.VersionSegment); err != nil {
		return err
	}
	count := uint32(len(b.tombstoneKeys))
	if err := binary.Write(f, binary.LittleEndian, count); err != nil {
		return err
	}
	for _, k := range b.tombstoneKeys {
		kl := uint32(len(k))
		if err := binary.Write(f, binary.LittleEndian, kl); err != nil {
			return err
		}
		if _, err := f.Write(k); err != nil {
			return err
		}
	}
	return nil
}

// trieNode represents a node in the trie.
type trieNode struct {
	label    []byte
	children []*trieNode
	isLeaf   bool
	value    []byte
}

// buildTrie builds a trie from the keys.
func (b *Builder) buildTrie() *trieNode {
	root := &trieNode{}

	for i, key := range b.keys {
		b.insertIntoTrie(root, key, b.values[i])
	}

	return root
}

// insertIntoTrie inserts a key-value pair into the trie.
func (b *Builder) insertIntoTrie(node *trieNode, key, value []byte) {
	if len(key) == 0 {
		node.isLeaf = true
		node.value = value
		return
	}

	// Find matching child
	for _, child := range node.children {
		if len(child.label) > 0 && child.label[0] == key[0] {
			// Found matching child
			commonLen := commonPrefixLength(child.label, key)

			if commonLen == len(child.label) {
				// Continue with child
				b.insertIntoTrie(child, key[commonLen:], value)
				return
			}

			// Split the edge
			newChild := &trieNode{
				label:    child.label[commonLen:],
				children: child.children,
				isLeaf:   child.isLeaf,
				value:    child.value,
			}

			child.label = child.label[:commonLen]
			child.children = []*trieNode{newChild}
			child.isLeaf = false
			child.value = nil

			if commonLen < len(key) {
				// Add new branch
				leafChild := &trieNode{
					label:  key[commonLen:],
					isLeaf: true,
					value:  value,
				}
				child.children = append(child.children, leafChild)
			} else {
				child.isLeaf = true
				child.value = value
			}

			return
		}
	}

	// No matching child, create new one
	newChild := &trieNode{
		label:  key,
		isLeaf: true,
		value:  value,
	}
	node.children = append(node.children, newChild)
}

// buildLOUDS builds the LOUDS representation.
func (b *Builder) buildLOUDS(trie *trieNode, path string) error {
	// Convert our trie to encoding.TrieNode format
	encodingTrie := b.convertToEncodingTrie(trie)

	// Create LOUDS encoding
	louds := encoding.NewLOUDS(encodingTrie)

	// Marshal LOUDS data
	loudsData := louds.Marshal()

	// Write 6B header + payload (no CRC/padding)
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := WriteCommonHeader(file, common.MagicLouds, common.VersionSegment); err != nil {
		return err
	}
	if _, err := file.Write(loudsData); err != nil {
		return err
	}
	return file.Commit()
}

// convertToEncodingTrie converts our internal trie to encoding.TrieNode format.
func (b *Builder) convertToEncodingTrie(node *trieNode) *encoding.TrieNode {
	if node == nil {
		return nil
	}

	result := &encoding.TrieNode{
		IsLeaf: node.isLeaf,
		Value:  node.value,
	}

	// Set label from the first byte if available
	if len(node.label) > 0 {
		result.Label = node.label[0]
	}

	// Convert children
	for _, child := range node.children {
		if childNode := b.convertToEncodingTrie(child); childNode != nil {
			// For multi-byte labels, we need to create intermediate nodes
			if len(child.label) > 1 {
				// Create chain of nodes for multi-byte label
				current := childNode
				for i := len(child.label) - 1; i > 0; i-- {
					parent := &encoding.TrieNode{
						Label:    child.label[i-1],
						Children: []*encoding.TrieNode{current},
					}
					current = parent
				}
				result.Children = append(result.Children, current)
			} else {
				result.Children = append(result.Children, childNode)
			}
		}
	}

	return result
}

// buildEdges builds the edges file.
func (b *Builder) buildEdges(_ *trieNode, path string) error {
	// Simplified edges building
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	if err := WriteCommonHeader(file, common.MagicEdges, common.VersionSegment); err != nil {
		return err
	}

	// Write placeholder data
	data := make([]byte, 1024)
	if _, err := file.Write(data); err != nil {
		return err
	}

	return file.Commit()
}

// buildAccept builds the accept states file.
func (b *Builder) buildAccept(_ *trieNode, path string) error {
	// Simplified accept building
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	if err := WriteCommonHeader(file, common.MagicAccept, common.VersionSegment); err != nil {
		return err
	}

	// Write placeholder data
	data := make([]byte, 1024)
	if _, err := file.Write(data); err != nil {
		return err
	}

	return file.Commit()
}

// buildTMap builds the tail mapping file.
func (b *Builder) buildTMap(path string) error {
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	if err := WriteCommonHeader(file, common.MagicTMap, common.VersionSegment); err != nil {
		return err
	}

	// Write placeholder data
	data := make([]byte, 256)
	if _, err := file.Write(data); err != nil {
		return err
	}

	return file.Commit()
}

// buildTails builds the tails data file.
func (b *Builder) buildTails(path string) error {
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	if err := WriteCommonHeader(file, common.MagicTails, common.VersionSegment); err != nil {
		return err
	}

	// Write placeholder data
	data := make([]byte, 256)
	if _, err := file.Write(data); err != nil {
		return err
	}

	return file.Commit()
}

// buildFilters builds the filter files.
func (b *Builder) buildFilters(dir string) error {
	// Build prefix bloom filter
	bloomPath := filepath.Join(dir, "prefix.bf")
	if err := b.buildBloomFilter(bloomPath); err != nil {
		return fmt.Errorf("build bloom filter: %w", err)
	}

	// Build trigram filter
	trigramPath := filepath.Join(dir, "tri.bits")
	if err := b.buildTrigramFilter(trigramPath); err != nil {
		return fmt.Errorf("build trigram filter: %w", err)
	}

	return nil
}

// buildBloomFilter builds the prefix bloom filter.
func (b *Builder) buildBloomFilter(path string) error {
	// Create Bloom filter with estimated size using default FPR
	bloom := filters.NewBloomFilter(uint64(len(b.keys)), common.DefaultBloomFPR)

	// Add all keys and their prefixes
	for _, key := range b.keys {
		bloom.AddPrefix(key, int(common.DefaultPrefixBloomLength)) // Add prefixes up to configured length
	}

	// Marshal the filter
	bloomData := bloom.Marshal()

	// Capture bloom params by reading header of bloomData
	if len(bloomData) >= 16 {
		b.filterBloomBits = binary.LittleEndian.Uint64(bloomData[0:8])
		b.filterBloomK = binary.LittleEndian.Uint32(bloomData[8:12])
		b.filterBloomFPR = common.DefaultBloomFPR
	}

	// Write 6B header + payload (no CRC/padding)
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := WriteCommonHeader(file, common.MagicBloom, common.VersionSegment); err != nil {
		return err
	}
	if _, err := file.Write(bloomData); err != nil {
		return err
	}
	return file.Commit()
}

// buildTrigramFilter builds the trigram filter.
func (b *Builder) buildTrigramFilter(path string) error {
	// Build trigram filter from keys
	tri := filters.NewTrigramFilter()
	for _, k := range b.keys {
		tri.AddString(k)
	}
	data := tri.Marshal()

	// Store trigram metadata
	b.filterTrigramBits = uint64(len(data)) * 8
	b.filterTrigramScheme = "bitset"

	// Write 6B header + payload (no CRC/padding)
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := WriteCommonHeader(file, common.MagicTrigram, common.VersionSegment); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Commit()
}

// commonPrefixLength returns the length of the common prefix.
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

// NullLogger is a logger that discards all messages.
type NullLogger struct{}

func (n *NullLogger) Debug(msg string, fields ...interface{}) {}
func (n *NullLogger) Info(msg string, fields ...interface{})  {}
func (n *NullLogger) Warn(msg string, fields ...interface{})  {}
func (n *NullLogger) Error(msg string, fields ...interface{}) {}
