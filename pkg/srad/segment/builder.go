package segment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/internal/encoding"
	"github.com/CVDpl/go-live-srad/internal/filters"
	"github.com/CVDpl/go-live-srad/pkg/srad/memtable"
	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
	blake3 "github.com/zeebo/blake3"
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
	// Per-key absolute expiry times in Unix nanos (0 if none), parallel to keys
	expiries []int64

	// Statistics
	minKey []byte
	maxKey []byte

	// Options controlling emission
	dropTombstones bool
	// Hint that keys were added in sorted order (e.g., via AddFromMemtable)
	alreadySorted bool
	forceTrie     bool

	// Options
	logger common.Logger

	// Derived filter metadata for manifest/metadata
	filterBloomBits     uint64
	filterBloomK        uint32
	filterBloomFPR      float64
	filterTrigramBits   uint64
	filterTrigramScheme string
	// Precomputed BLAKE3 hex sums (to avoid rereads)
	filterBloomBlake3   string
	filterTrigramBlake3 string
	keysBlake3          string
	// Core artifacts BLAKE3
	loudsBlake3 string
	tombsBlake3 string

	// Filter configuration (0/false => defaults)
	customBloomFPR  float64
	customPrefixLen int
	enableTrigram   bool

	// Build parallelization config
	maxShards         int
	shardMinKeys      int
	bloomAdaptMinKeys int

	// When true, Build() skips generating filter files; caller can build them later asynchronously.
	skipFilterBuild bool

	// LOUDS control
	disableLOUDS       bool
	autoDisableMinKeys int

	// GC config during trie build (0 = unchanged; >0 sets GC percent; <0 disables GC)
	trieGCPercent int
}

// pair holds key and tombstone flag used during snapshot sorting.
type pair struct {
	k []byte
	t bool
	e int64
}

// NewBuilder creates a new segment builder.
func NewBuilder(segmentID uint64, level int, dir string, logger common.Logger) *Builder {
	if logger == nil {
		logger = common.NewNullLogger()
	}

	return &Builder{
		segmentID:     segmentID,
		level:         level,
		dir:           dir,
		logger:        logger,
		enableTrigram: true,
		trieGCPercent: 0,
	}
}

// DropAllTombstones instructs the builder to omit writing tombstones.dat
// and to not derive metadata key range from tombstone keys.
func (b *Builder) DropAllTombstones() { b.dropTombstones = true }

// AddFromMemtable adds all entries from a memtable snapshot.
func (b *Builder) AddFromMemtable(snapshot *memtable.Snapshot) error {
	// Use GetAllWithExpiry() to get keys with tombstone flags and expiry timestamps
	keys, tombs, expiries := snapshot.GetAllWithExpiry()

	for i, k := range keys {
		if tombs[i] {
			b.tombstoneKeys = append(b.tombstoneKeys, k)
			continue
		}
		if err := b.AddWithExpiry(k, k, expiries[i], false); err != nil {
			return err
		}
	}

	// Input is already sorted by GetAllWithExpiry (Patricia trie traversal is in-order)
	b.alreadySorted = true

	return nil
}

// AddFromPairs adds entries from provided keys and tomb flags (already in-memory), optimized for partitioned flush.
func (b *Builder) AddFromPairs(keys [][]byte, tombs []bool) error {
	if len(keys) != len(tombs) {
		return fmt.Errorf("keys/tombs length mismatch")
	}
	if len(keys) == 0 {
		return nil
	}
	pairs := make([]pair, len(keys))
	for i := range keys {
		pairs[i] = pair{k: keys[i], t: tombs[i]}
	}
	pairs = b.parallelSortPairs(pairs)
	b.alreadySorted = true
	for _, p := range pairs {
		if p.t {
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
	return b.AddWithExpiry(key, value, 0, tombstone)
}

// AddWithExpiry adds a key-value pair with an absolute expiry timestamp (unix nanos; 0 = none).
func (b *Builder) AddWithExpiry(key, value []byte, expiresAtNanos int64, tombstone bool) error {
	// Skip tombstones for now (they're handled by shadowing in merge)
	if tombstone {
		return nil
	}

	// Make copies to ensure immutability
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Avoid holding full values in memory; store an empty (non-nil) marker slice
	b.keys = append(b.keys, keyCopy)
	b.values = append(b.values, []byte{})
	b.tombstones = append(b.tombstones, tombstone)
	b.expiries = append(b.expiries, expiresAtNanos)

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

// ConfigureFilters sets filter-related options for this builder.
func (b *Builder) ConfigureFilters(bloomFPR float64, prefixLen int, enableTrigram bool) {
	if bloomFPR > 0 {
		b.customBloomFPR = bloomFPR
	}
	if prefixLen > 0 {
		b.customPrefixLen = prefixLen
	}
	b.enableTrigram = enableTrigram
}

// ConfigureBuild sets parallelization and adaptive thresholds.
func (b *Builder) ConfigureBuild(maxShards, shardMinKeys, bloomAdaptMinKeys int) {
	if maxShards > 0 {
		b.maxShards = maxShards
	}
	if shardMinKeys > 0 {
		b.shardMinKeys = shardMinKeys
	}
	if bloomAdaptMinKeys > 0 {
		b.bloomAdaptMinKeys = bloomAdaptMinKeys
	}
}

// SetDisableLOUDS configures whether to skip LOUDS generation during Build.
func (b *Builder) SetDisableLOUDS(skip bool) { b.disableLOUDS = skip }

// SetAutoDisableLOUDSThreshold sets minimal key count to auto-skip LOUDS.
func (b *Builder) SetAutoDisableLOUDSThreshold(n int) { b.autoDisableMinKeys = n }

// SetTrieGCPercent configures a temporary GC percent used only during trie build (0 = unchanged).
func (b *Builder) SetTrieGCPercent(p int) { b.trieGCPercent = p }

// SetForceTrie forces building trie even for sorted inputs (benchmark/debug).
func (b *Builder) SetForceTrie(force bool) { b.forceTrie = force }

// MarkAlreadySorted marks input as sorted to enable fast-path builders.
func (b *Builder) MarkAlreadySorted() { b.alreadySorted = true }

// SetSkipFilters controls whether Build() should skip generating filter files.
// When set to true, filters can be generated later via BuildBloomOnly/BuildTrigramOnly.
func (b *Builder) SetSkipFilters(skip bool) { b.skipFilterBuild = skip }

// Build creates the segment files.
func (b *Builder) Build() (*Metadata, error) {
	return b.BuildWithContext(context.Background())
}

// BuildWithContext creates the segment files with context support.
func (b *Builder) BuildWithContext(ctx context.Context) (*Metadata, error) {
	if len(b.keys) == 0 && len(b.tombstoneKeys) == 0 {
		return nil, fmt.Errorf("no keys to build segment")
	}

	// Normalize: sort keys and deduplicate adjacent duplicates to reduce size and enable linear merges
	if len(b.keys) > 1 && !b.alreadySorted {
		idx := make([]int, len(b.keys))
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(i, j int) bool { return bytes.Compare(b.keys[idx[i]], b.keys[idx[j]]) < 0 })
		dedupKeys := make([][]byte, 0, len(b.keys))
		dedupVals := make([][]byte, 0, len(b.values))
		for _, id := range idx {
			k := b.keys[id]
			if len(dedupKeys) == 0 || !bytes.Equal(dedupKeys[len(dedupKeys)-1], k) {
				dedupKeys = append(dedupKeys, k)
				if id < len(b.values) {
					dedupVals = append(dedupVals, b.values[id])
				} else {
					dedupVals = append(dedupVals, nil)
				}
			}
		}
		b.keys = dedupKeys
		b.values = dedupVals
		if len(b.keys) > 0 {
			b.minKey = b.keys[0]
			b.maxKey = b.keys[len(b.keys)-1]
		}
		// Mark as sorted to enable fast-path builders (trie/LOUDS)
		b.alreadySorted = true
	}

	// Ensure tombstones are sorted to allow streaming merge
	if len(b.tombstoneKeys) > 1 {
		b.tombstoneKeys = b.parallelSortByteSlices(b.tombstoneKeys)
	}

	// Create segment directory
	segmentDir := filepath.Join(b.dir, fmt.Sprintf("%016d", b.segmentID))
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create segment directory: %w", err)
	}
	// Create BUILDING sentinel to protect against RCU cleanup during long builds
	buildingPath := filepath.Join(segmentDir, ".building")
	_ = os.WriteFile(buildingPath, []byte("1"), 0644)

	// Create filters directory
	filtersDir := filepath.Join(segmentDir, "filters")
	if err := os.MkdirAll(filtersDir, 0755); err != nil {
		return nil, fmt.Errorf("create filters directory: %w", err)
	}

	// Kick off parallel tasks that don't depend on trie
	keysPath := filepath.Join(segmentDir, "keys.dat")
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	// filters
	if !b.skipFilterBuild {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.logger.Info("starting filters build", "segment_id", b.segmentID, "keys", len(b.keys))
			start := time.Now()
			if err := b.buildFilters(filtersDir); err != nil {
				b.logger.Error("filters build failed", "segment_id", b.segmentID, "error", err)
				errCh <- err
			} else {
				b.logger.Info("filters build completed", "segment_id", b.segmentID, "duration", time.Since(start))
			}
		}()
	}
	// keys.dat
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.logger.Info("starting keys file write", "segment_id", b.segmentID, "keys", len(b.keys))
		start := time.Now()
		if err := b.writeKeysFile(keysPath); err != nil {
			b.logger.Error("keys file write failed", "segment_id", b.segmentID, "error", err)
			errCh <- err
		} else {
			b.logger.Info("keys file write completed", "segment_id", b.segmentID, "duration", time.Since(start))
		}
	}()
	// expiry.dat (optional, but write even if empty to preserve alignment when present)
	if len(b.keys) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.logger.Info("starting expiry file write", "segment_id", b.segmentID, "keys", len(b.keys))
			start := time.Now()
			expPath := filepath.Join(segmentDir, "expiry.dat")
			if err := b.writeExpiryFile(expPath); err != nil {
				b.logger.Error("expiry file write failed", "segment_id", b.segmentID, "error", err)
				errCh <- err
			} else {
				b.logger.Info("expiry file write completed", "segment_id", b.segmentID, "duration", time.Since(start))
			}
		}()
	}
	// tombstones.dat (optional)
	if len(b.tombstoneKeys) > 0 && !b.dropTombstones {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.logger.Info("starting tombstones file write", "segment_id", b.segmentID, "tombstone_keys", len(b.tombstoneKeys))
			start := time.Now()
			tombPath := filepath.Join(segmentDir, "tombstones.dat")
			if err := b.writeTombstonesFile(tombPath); err != nil {
				b.logger.Error("tombstones file write failed", "segment_id", b.segmentID, "error", err)
				errCh <- err
			} else {
				b.logger.Info("tombstones file write completed", "segment_id", b.segmentID, "duration", time.Since(start))
			}
		}()
	}

	b.logger.Info("building segment", "id", b.segmentID, "keys", len(b.keys))

	stepStart := time.Now()

	var trie *trieNode
	// Force trie build to avoid streaming LOUDS issues
	if false && b.alreadySorted && !b.forceTrie {
		b.logger.Info("trie build skipped (streaming LOUDS)", "id", b.segmentID, "keys", len(b.keys))
	} else {
		// Build trie structure (simplified for now)
		b.logger.Info("trie build start", "id", b.segmentID, "keys", len(b.keys), "note", "This may take a while for large datasets")
		trieStart := time.Now()
		gcPrev := -2
		if b.trieGCPercent != 0 {
			// Save current GC percent and apply temporary policy
			gcPrev = debug.SetGCPercent(b.trieGCPercent)
			b.logger.Debug("adjusted GC percent for trie build", "old", gcPrev, "new", b.trieGCPercent)
		}

		// Monitor memory usage during trie build for large datasets
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		startMem := m.Alloc

		trie = b.buildTrie()

		runtime.ReadMemStats(&m)
		memUsed := m.Alloc - startMem

		if b.trieGCPercent != 0 {
			_ = debug.SetGCPercent(gcPrev)
		}

		trieDuration := time.Since(trieStart)
		b.logger.Info("trie build done",
			"id", b.segmentID,
			"duration", trieDuration,
			"memory_used_mb", memUsed/(1024*1024),
			"keys_per_sec", float64(len(b.keys))/trieDuration.Seconds())

		// Warn if trie build took excessively long
		if trieDuration > 2*time.Minute {
			b.logger.Warn("trie build took unusually long",
				"id", b.segmentID,
				"duration", trieDuration,
				"keys", len(b.keys),
				"note", "Consider using alreadySorted flag or reducing dataset size")
		}
	}

	// Build LOUDS synchronously
	localStart := time.Now()
	if err := b.buildLOUDS(trie, filepath.Join(segmentDir, "index.louds")); err != nil {
		return nil, fmt.Errorf("build LOUDS: %w", err)
	}
	b.logger.Info("step done", "id", b.segmentID, "step", "LOUDS", "duration", time.Since(localStart))

	// Wait for parallel tasks with context cancellation support
	done := make(chan struct{})
	var waitError error

	go func() {
		wg.Wait()
		close(errCh)
		for e := range errCh {
			if e != nil {
				waitError = e
				break
			}
		}
		close(done)
	}()

	select {
	case <-done:
		if waitError != nil {
			return nil, waitError
		}
		// All goroutines completed successfully
	case <-ctx.Done():
		b.logger.Warn("segment build cancelled by context",
			"id", b.segmentID,
			"keys", len(b.keys),
			"reason", ctx.Err())
		return nil, fmt.Errorf("segment build cancelled: %w", ctx.Err())
	}
	b.logger.Info("segment core build done", "id", b.segmentID, "duration", time.Since(stepStart))

	// tombstones written above when present

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

	if b.filterBloomBits > 0 && b.filterBloomK > 0 {
		metadata.Filters.PrefixBloom = &BloomFilter{Bits: b.filterBloomBits, K: b.filterBloomK, FPR: b.filterBloomFPR}
	}
	if b.filterTrigramBits > 0 {
		metadata.Filters.Trigram = &TrigramFilter{Bits: b.filterTrigramBits, Scheme: b.filterTrigramScheme}
	}
	if len(b.keys) > 0 {
		metadata.Files.Expiry = "expiry.dat"
	}
	metadata.Blake3 = map[string]string{}
	if b.loudsBlake3 != "" {
		metadata.Blake3["index.louds"] = b.loudsBlake3
	} else if h, err := utils.ComputeBLAKE3File(filepath.Join(segmentDir, "index.louds")); err == nil {
		metadata.Blake3["index.louds"] = h
	}
	// Deprecated files removed; do not record checksums for edges/accept/tmap/tails
	if b.filterBloomBlake3 != "" {
		metadata.Blake3["filters/prefix.bf"] = b.filterBloomBlake3
	} else if h, err := utils.ComputeBLAKE3File(filepath.Join(filtersDir, "prefix.bf")); err == nil {
		metadata.Blake3["filters/prefix.bf"] = h
	}
	if b.filterTrigramBlake3 != "" {
		metadata.Blake3["filters/tri.bits"] = b.filterTrigramBlake3
	} else if h, err := utils.ComputeBLAKE3File(filepath.Join(filtersDir, "tri.bits")); err == nil {
		metadata.Blake3["filters/tri.bits"] = h
	}
	if b.keysBlake3 != "" {
		metadata.Blake3["keys.dat"] = b.keysBlake3
	} else if h, err := utils.ComputeBLAKE3File(keysPath); err == nil {
		metadata.Blake3["keys.dat"] = h
	}
	if len(b.keys) > 0 {
		if h, err := utils.ComputeBLAKE3File(filepath.Join(segmentDir, "expiry.dat")); err == nil {
			metadata.Blake3["expiry.dat"] = h
		}
	}
	if len(b.tombstoneKeys) > 0 && !b.dropTombstones {
		if b.tombsBlake3 != "" {
			metadata.Blake3["tombstones.dat"] = b.tombsBlake3
		} else if h, err := utils.ComputeBLAKE3File(filepath.Join(segmentDir, "tombstones.dat")); err == nil {
			metadata.Blake3["tombstones.dat"] = h
		}
	}

	// Save metadata
	metadataPath := filepath.Join(segmentDir, "segment.json")
	if err := metadata.SaveToFile(metadataPath); err != nil {
		return nil, fmt.Errorf("save metadata: %w", err)
	}

	// Build completed successfully – remove BUILDING sentinel
	_ = os.Remove(buildingPath)

	b.logger.Info("segment built successfully", "id", b.segmentID)

	return metadata, nil
}

func (b *Builder) writeKeysFile(path string) error {
	af, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer af.Close()

	// Buffered writer to reduce syscall count
	bw := bufio.NewWriterSize(af, 4<<20)
	// Compute BLAKE3 while writing
	hasher := blake3.New()
	out := io.MultiWriter(bw, hasher)

	// Write common header with magic and version
	if err := WriteCommonHeader(out, common.MagicKeys, common.VersionSegment); err != nil {
		return err
	}

	// Write count
	count := uint32(len(b.keys))
	if err := binary.Write(out, binary.LittleEndian, count); err != nil {
		return err
	}
	// Write each key (len + bytes)
	for _, k := range b.keys {
		kl := uint32(len(k))
		if err := binary.Write(out, binary.LittleEndian, kl); err != nil {
			return err
		}
		if _, err := out.Write(k); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	b.keysBlake3 = fmt.Sprintf("%x", hasher.Sum(nil))
	return af.Commit()
}

func (b *Builder) writeExpiryFile(path string) error {
	af, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer af.Close()

	bw := bufio.NewWriterSize(af, 4<<20)
	hasher := blake3.New()
	out := io.MultiWriter(bw, hasher)

	// Write common header with magic and version
	if err := WriteCommonHeader(out, common.MagicExpiry, common.VersionSegment); err != nil {
		return err
	}
	count := uint32(len(b.keys))
	if err := binary.Write(out, binary.LittleEndian, count); err != nil {
		return err
	}
	// Ensure expiries slice length matches keys; missing values are zero
	if len(b.expiries) < int(count) {
		pad := make([]int64, int(count)-len(b.expiries))
		b.expiries = append(b.expiries, pad...)
	}
	for i := 0; i < int(count); i++ {
		var e int64
		if i < len(b.expiries) {
			e = b.expiries[i]
		}
		if err := binary.Write(out, binary.LittleEndian, e); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return af.Commit()
}

func (b *Builder) writeTombstonesFile(path string) error {
	af, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer af.Close()

	bw := bufio.NewWriterSize(af, 4<<20)
	hasher := blake3.New()
	out := io.MultiWriter(bw, hasher)

	// Write common header with magic and version
	if err := WriteCommonHeader(out, common.MagicTombs, common.VersionSegment); err != nil {
		return err
	}
	count := uint32(len(b.tombstoneKeys))
	if err := binary.Write(out, binary.LittleEndian, count); err != nil {
		return err
	}
	for _, k := range b.tombstoneKeys {
		kl := uint32(len(k))
		if err := binary.Write(out, binary.LittleEndian, kl); err != nil {
			return err
		}
		if _, err := out.Write(k); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	b.tombsBlake3 = fmt.Sprintf("%x", hasher.Sum(nil))
	return af.Commit()
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
	if len(b.keys) == 0 {
		return &trieNode{}
	}
	if b.alreadySorted && !b.forceTrie {
		return b.buildTrieFromSortedRange(0, len(b.keys), 0)
	}
	root := &trieNode{}
	for i, key := range b.keys {
		// inline insert (original insertIntoTrie removed)
		var insert func(node *trieNode, key, value []byte)
		insert = func(node *trieNode, key, value []byte) {
			if len(key) == 0 {
				node.isLeaf = true
				node.value = value
				return
			}
			for _, child := range node.children {
				if len(child.label) > 0 && child.label[0] == key[0] {
					commonLen := commonPrefixLength(child.label, key)
					if commonLen == len(child.label) {
						insert(child, key[commonLen:], value)
						return
					}
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
						leafChild := &trieNode{label: key[commonLen:], isLeaf: true, value: value}
						child.children = append(child.children, leafChild)
					} else {
						child.isLeaf = true
						child.value = value
					}
					return
				}
			}
			node.children = append(node.children, &trieNode{label: key, isLeaf: true, value: value})
		}
		insert(root, key, b.values[i])
	}
	return root
}

// buildTrieFromSortedRange builds a compressed trie from sorted keys in range.
func (b *Builder) buildTrieFromSortedRange(start, end, depth int) *trieNode {
	node := &trieNode{}
	if start >= end {
		return node
	}
	i := start
	for i < end && len(b.keys[i]) == depth {
		node.isLeaf = true
		node.value = b.values[i]
		i++
	}
	// Collect child groups for this node
	starts := make([]int, 0, 8)
	ends := make([]int, 0, 8)
	labels := make([][]byte, 0, 8)
	for i < end {
		if len(b.keys[i]) <= depth {
			i++
			continue
		}
		lb := b.keys[i][depth]
		j := i + 1
		for j < end {
			if len(b.keys[j]) <= depth {
				j++
				continue
			}
			if b.keys[j][depth] != lb {
				break
			}
			j++
		}
		// Compute common prefix length among [i:j)
		plen := 1
		for {
			pos := depth + plen
			same := true
			for k := i; k < j; k++ {
				if len(b.keys[k]) <= pos || b.keys[k][pos] != b.keys[i][pos] {
					same = false
					break
				}
			}
			if !same {
				break
			}
			plen++
		}
		starts = append(starts, i)
		ends = append(ends, j)
		labels = append(labels, append([]byte(nil), b.keys[i][depth:depth+plen]...))
		i = j
	}

	// Build children possibly in parallel, preserving order
	n := len(starts)
	if n == 0 {
		return node
	}
	children := make([]*trieNode, n)
	// Decide on parallelism
	shards := b.maxShards
	if shards <= 0 {
		shards = runtime.GOMAXPROCS(0)
	}
	parallel := n > 1 && (ends[n-1]-starts[0]) >= max(200000, b.shardMinKeys) && shards > 1
	if !parallel {
		for idx := 0; idx < n; idx++ {
			sub := b.buildTrieFromSortedRange(starts[idx], ends[idx], depth+len(labels[idx]))
			child := &trieNode{label: labels[idx]}
			child.children = sub.children
			child.isLeaf = sub.isLeaf
			child.value = sub.value
			children[idx] = child
		}
	} else {
		sem := make(chan struct{}, shards)
		var wg sync.WaitGroup
		for idx := 0; idx < n; idx++ {
			sem <- struct{}{}
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				sub := b.buildTrieFromSortedRange(starts[idx], ends[idx], depth+len(labels[idx]))
				child := &trieNode{label: labels[idx]}
				child.children = sub.children
				child.isLeaf = sub.isLeaf
				child.value = sub.value
				children[idx] = child
				<-sem
			}(idx)
		}
		wg.Wait()
	}
	// Append in order
	node.children = append(node.children, children...)
	return node
}

// buildLOUDS builds the LOUDS representation.
func (b *Builder) buildLOUDS(trie *trieNode, path string) error {
	// Convert our trie to encoding.TrieNode format
	encodingTrie := b.convertToEncodingTrie(trie)

	// Create LOUDS encoding unless disabled by options/thresholds
	if b.disableLOUDS || (b.autoDisableMinKeys > 0 && len(b.keys) >= b.autoDisableMinKeys) {
		b.logger.Info("skipping LOUDS build due to config/threshold", "keys", len(b.keys))
		file, err := utils.NewAtomicFile(path)
		if err != nil {
			return err
		}
		defer file.Close()
		bw := bufio.NewWriterSize(file, 4<<20)
		if err := WriteCommonHeader(bw, common.MagicLouds, common.VersionSegment); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		return file.Commit()
	}

	// Build LOUDS without Rank/Select to speed up flush; readers will rebuild RS
	var louds *encoding.LOUDS
	// Use streaming LOUDS builder when possible (faster for sorted keys)
	if b.alreadySorted {
		// Prefer streaming LOUDS builder from sorted keys (fast path)
		louds = encoding.NewLOUDSFromSortedKeys(b.keys)
	} else {
		louds = encoding.NewLOUDSNoRS(encodingTrie)
	}

	// Marshal LOUDS data
	loudsData := louds.Marshal()

	// Write 6B header + payload (no CRC/padding)
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hasher := blake3.New()
	bw := bufio.NewWriterSize(file, 4<<20)
	out := io.MultiWriter(bw, hasher)
	if err := WriteCommonHeader(out, common.MagicLouds, common.VersionSegment); err != nil {
		return err
	}
	if _, err := out.Write(loudsData); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	b.loudsBlake3 = fmt.Sprintf("%x", hasher.Sum(nil))
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

// buildFilters builds the filter files.
func (b *Builder) buildFilters(dir string) error {
	// Build prefix bloom filter
	bloomPath := filepath.Join(dir, "prefix.bf")
	if err := b.buildBloomFilter(bloomPath); err != nil {
		return fmt.Errorf("build bloom filter: %w", err)
	}

	// Build trigram filter
	if b.enableTrigram {
		trigramPath := filepath.Join(dir, "tri.bits")
		if err := b.buildTrigramFilter(trigramPath); err != nil {
			return fmt.Errorf("build trigram filter: %w", err)
		}
	}

	return nil
}

// BuildBloomOnly builds only the bloom filter into dir.
func (b *Builder) BuildBloomOnly(dir string) error {
	bloomPath := filepath.Join(dir, "prefix.bf")
	return b.buildBloomFilter(bloomPath)
}

// BuildTrigramOnly builds only the trigram filter into dir.
func (b *Builder) BuildTrigramOnly(dir string) error {
	trigramPath := filepath.Join(dir, "tri.bits")
	return b.buildTrigramFilter(trigramPath)
}

// buildBloomFilter builds the prefix bloom filter.
func (b *Builder) buildBloomFilter(path string) error {
	// Resolve parameters
	fpr := b.customBloomFPR
	if fpr <= 0 {
		fpr = common.DefaultBloomFPR
	}
	prefixLen := b.customPrefixLen
	if prefixLen <= 0 {
		prefixLen = int(common.DefaultPrefixBloomLength)
	}
	// Adaptively reduce prefix work for very large inputs
	adaptMin := b.bloomAdaptMinKeys
	if adaptMin <= 0 {
		adaptMin = 10_000_000
	}
	if len(b.keys) > adaptMin && prefixLen > 4 {
		prefixLen = prefixLen / 2
		if prefixLen < 4 {
			prefixLen = 4
		}
	}

	// Sharded build for parallelism
	shards := runtime.GOMAXPROCS(0)
	cap := b.maxShards
	if cap <= 0 {
		cap = 8
	}
	if shards > cap {
		shards = cap
	}
	minKeys := b.shardMinKeys
	if minKeys <= 0 {
		minKeys = 200_000
	}
	if len(b.keys) < minKeys {
		shards = 1
	}
	local := make([]*filters.BloomFilter, shards)
	var wg sync.WaitGroup
	wg.Add(shards)
	for sidx := 0; sidx < shards; sidx++ {
		s := sidx
		go func() {
			defer wg.Done()
			bf := filters.NewBloomFilter(uint64(len(b.keys)), fpr)
			start := (len(b.keys) * s) / shards
			end := (len(b.keys) * (s + 1)) / shards
			for i := start; i < end; i++ {
				bf.AddPrefix(b.keys[i], prefixLen)
			}
			local[s] = bf
		}()
	}
	wg.Wait()
	// Merge shards
	var bloom *filters.BloomFilter
	for _, bf := range local {
		if bloom == nil {
			bloom = bf
			continue
		}
		_ = bloom.Merge(bf)
	}

	// Marshal the filter
	bloomData := bloom.Marshal()

	// Capture bloom params
	if len(bloomData) >= 16 {
		b.filterBloomBits = binary.LittleEndian.Uint64(bloomData[0:8])
		b.filterBloomK = binary.LittleEndian.Uint32(bloomData[8:12])
		b.filterBloomFPR = fpr
	}

	// Write 6B header + payload (no CRC/padding) with streaming hash
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hasher := blake3.New()
	bw := bufio.NewWriterSize(file, 4<<20)
	out := io.MultiWriter(bw, hasher)
	if err := WriteCommonHeader(out, common.MagicBloom, common.VersionSegment); err != nil {
		return err
	}
	if _, err := out.Write(bloomData); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	b.filterBloomBlake3 = fmt.Sprintf("%x", hasher.Sum(nil))
	return file.Commit()
}

// buildTrigramFilter builds the trigram filter.
func (b *Builder) buildTrigramFilter(path string) error {
	// Build trigram filter from keys using sharded OR
	shards := runtime.GOMAXPROCS(0)
	cap := b.maxShards
	if cap <= 0 {
		cap = 8
	}
	if shards > cap {
		shards = cap
	}
	minKeys := b.shardMinKeys
	if minKeys <= 0 {
		minKeys = 200_000
	}
	if len(b.keys) < minKeys {
		shards = 1
	}
	tmp := make([][]byte, shards)
	var wg sync.WaitGroup
	wg.Add(shards)
	for sidx := 0; sidx < shards; sidx++ {
		s := sidx
		go func() {
			defer wg.Done()
			tri := filters.NewTrigramFilter()
			start := (len(b.keys) * s) / shards
			end := (len(b.keys) * (s + 1)) / shards
			for i := start; i < end; i++ {
				tri.AddString(b.keys[i])
			}
			tmp[s] = tri.Marshal()
		}()
	}
	wg.Wait()
	// OR all shards
	var data []byte
	for _, part := range tmp {
		if len(part) == 0 {
			continue
		}
		if data == nil {
			data = make([]byte, len(part))
			copy(data, part)
			continue
		}
		for i := range data {
			data[i] |= part[i]
		}
	}

	// Store trigram metadata
	b.filterTrigramBits = uint64(len(data)) * 8
	b.filterTrigramScheme = "bitset"

	// Write 6B header + payload (no CRC/padding) with streaming hash
	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hasher := blake3.New()
	bw := bufio.NewWriterSize(file, 4<<20)
	out := io.MultiWriter(bw, hasher)
	if err := WriteCommonHeader(out, common.MagicTrigram, common.VersionSegment); err != nil {
		return err
	}
	if _, err := out.Write(data); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	b.filterTrigramBlake3 = fmt.Sprintf("%x", hasher.Sum(nil))
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

// parallelSortPairs sorts pairs by key using shard-sort + merge if opportune, otherwise falls back to single-threaded sort.
func (b *Builder) parallelSortPairs(pairs []pair) []pair {
	n := len(pairs)
	if n < 2 {
		return pairs
	}
	shards := runtime.GOMAXPROCS(0)
	if b.maxShards > 0 && shards > b.maxShards {
		shards = b.maxShards
	}
	minKeys := b.shardMinKeys
	if minKeys <= 0 {
		minKeys = 200_000
	}
	if n < minKeys {
		sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })
		return pairs
	}
	if shards < 2 {
		sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })
		return pairs
	}
	runs := make([][]pair, shards)
	var wg sync.WaitGroup
	wg.Add(shards)
	for s := 0; s < shards; s++ {
		start := (n * s) / shards
		end := (n * (s + 1)) / shards
		go func(start, end, idx int) {
			defer wg.Done()
			run := make([]pair, end-start)
			copy(run, pairs[start:end])
			sort.Slice(run, func(i, j int) bool { return bytes.Compare(run[i].k, run[j].k) < 0 })
			runs[idx] = run
		}(start, end, s)
	}
	wg.Wait()
	// Iteratively merge runs
	for len(runs) > 1 {
		next := make([][]pair, 0, (len(runs)+1)/2)
		for i := 0; i < len(runs); i += 2 {
			if i+1 >= len(runs) {
				next = append(next, runs[i])
				break
			}
			a, b := runs[i], runs[i+1]
			merged := make([]pair, 0, len(a)+len(b))
			ia, ib := 0, 0
			for ia < len(a) && ib < len(b) {
				if bytes.Compare(a[ia].k, b[ib].k) <= 0 {
					merged = append(merged, a[ia])
					ia++
				} else {
					merged = append(merged, b[ib])
					ib++
				}
			}
			if ia < len(a) {
				merged = append(merged, a[ia:]...)
			}
			if ib < len(b) {
				merged = append(merged, b[ib:]...)
			}
			next = append(next, merged)
		}
		runs = next
	}
	if len(runs) == 1 {
		return runs[0]
	}
	return pairs
}

// parallelSortByteSlices sorts [][]byte similarly to parallelSortPairs
func (b *Builder) parallelSortByteSlices(keys [][]byte) [][]byte {
	n := len(keys)
	if n < 2 {
		return keys
	}
	shards := runtime.GOMAXPROCS(0)
	if b.maxShards > 0 && shards > b.maxShards {
		shards = b.maxShards
	}
	minKeys := b.shardMinKeys
	if minKeys <= 0 {
		minKeys = 200_000
	}
	if n < minKeys || shards < 2 {
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
		return keys
	}
	runs := make([][][]byte, shards)
	var wg sync.WaitGroup
	wg.Add(shards)
	for s := 0; s < shards; s++ {
		start := (n * s) / shards
		end := (n * (s + 1)) / shards
		go func(start, end, idx int) {
			defer wg.Done()
			run := make([][]byte, end-start)
			copy(run, keys[start:end])
			sort.Slice(run, func(i, j int) bool { return bytes.Compare(run[i], run[j]) < 0 })
			runs[idx] = run
		}(start, end, s)
	}
	wg.Wait()
	// Merge runs
	for len(runs) > 1 {
		next := make([][][]byte, 0, (len(runs)+1)/2)
		for i := 0; i < len(runs); i += 2 {
			if i+1 >= len(runs) {
				next = append(next, runs[i])
				break
			}
			a, b := runs[i], runs[i+1]
			merged := make([][]byte, 0, len(a)+len(b))
			ia, ib := 0, 0
			for ia < len(a) && ib < len(b) {
				if bytes.Compare(a[ia], b[ib]) <= 0 {
					merged = append(merged, a[ia])
					ia++
				} else {
					merged = append(merged, b[ib])
					ib++
				}
			}
			if ia < len(a) {
				merged = append(merged, a[ia:]...)
			}
			if ib < len(b) {
				merged = append(merged, b[ib:]...)
			}
			next = append(next, merged)
		}
		runs = next
	}
	if len(runs) == 1 {
		return runs[0]
	}
	return keys
}
