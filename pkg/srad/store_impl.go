package srad

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/pkg/srad/compaction"
	"github.com/CVDpl/go-live-srad/pkg/srad/manifest"
	"github.com/CVDpl/go-live-srad/pkg/srad/memtable"
	"github.com/CVDpl/go-live-srad/pkg/srad/segment"
	"github.com/CVDpl/go-live-srad/pkg/srad/wal"
	blake3 "lukechampine.com/blake3"
)

// segItem is an item produced by parallel segment workers.
type segItem struct {
	id  uint64
	key []byte
	op  uint8
	err error
}

// storeImpl is the main implementation of the Store interface.
type storeImpl struct {
	mu     sync.RWMutex
	dir    string
	opts   *Options
	logger common.Logger

	// Core components
	wal      *wal.WAL
	memtable *memtable.Memtable

	// State
	closed   int32 // atomic
	readonly bool

	// Background tasks
	flushTicker  *time.Ticker
	flushStop    chan struct{}
	compactStop  chan struct{}
	autotuneStop chan struct{}
	rcuStop      chan struct{}

	// Statistics
	stats         *StatsCollector
	lastFlushTime time.Time

	// Autotuning
	autotunerEnabled bool
	tuningParams     TuningParams

	// RCU
	rcuEnabled bool
	rcuEpoch   uint64

	// Segments
	nextSegmentID uint64
	segments      []*segment.Metadata
	readers       []*segment.Reader
	manifest      *manifest.Manifest
	compactor     *compaction.Compactor

	// Wait group for background tasks
	bgWg sync.WaitGroup
}

// updateStatsFromManifest refreshes StatsCollector using the manifest's active segments.
func (s *storeImpl) updateStatsFromManifest() {
	if s.manifest == nil || s.stats == nil {
		return
	}
	// Per-level aggregation
	levelSizes := make(map[int]int64)
	levelCounts := make(map[int]int)
	active := s.manifest.GetActiveSegments()
	for _, seg := range active {
		levelSizes[seg.Level] += seg.Size
		levelCounts[seg.Level]++
	}
	for lvl, size := range levelSizes {
		s.stats.UpdateLevelStats(lvl, size, levelCounts[lvl], 0)
	}
	s.stats.SetManifestGeneration(s.manifest.GetCurrentVersion())
}

// computeSegmentSize sums sizes of known files within a segment directory.
func (s *storeImpl) computeSegmentSize(segmentID uint64) int64 {
	segmentsDir := filepath.Join(s.dir, common.DirSegments)
	segDir := filepath.Join(segmentsDir, fmt.Sprintf("%016d", segmentID))
	// Try to load metadata to get file names
	md, err := segment.LoadFromFile(filepath.Join(segDir, "segment.json"))
	if err != nil {
		return 0
	}
	var total int64
	add := func(rel string) {
		if rel == "" {
			return
		}
		p := filepath.Join(segDir, rel)
		if st, err := os.Stat(p); err == nil {
			total += st.Size()
		}
	}
	add(md.Files.Louds)
	add(md.Files.Edges)
	add(md.Files.Accept)
	add(md.Files.TMap)
	add(md.Files.Tails)
	// filters and aux files
	for _, rel := range []string{"filters/prefix.bf", "filters/tri.bits", "keys.dat", "tombstones.dat"} {
		add(rel)
	}
	return total
}

// Open creates or opens a store at the specified directory.
func Open(dir string, opts *Options) (Store, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if opts.Logger == nil {
		opts.Logger = NewDefaultLogger()
	}

	// Create directory if it doesn't exist
	if err := createStoreDirectories(dir); err != nil {
		return nil, fmt.Errorf("create store directories: %w", err)
	}

	s := &storeImpl{
		dir:              dir,
		opts:             opts,
		logger:           opts.Logger,
		readonly:         opts.ReadOnly,
		autotunerEnabled: !opts.DisableAutotuner,
		rcuEnabled:       opts.EnableRCU,
		stats:            NewStatsCollector(),
	}

	// Initialize WAL if not read-only
	if !s.readonly {
		walDir := filepath.Join(dir, common.DirWAL)
		wcfg := wal.Config{RotateSize: common.WALRotateSize, MaxFileSize: common.WALMaxFileSize, BufferSize: int(common.WALBufferSize)}
		if opts.WALRotateSize > 0 {
			wcfg.RotateSize = opts.WALRotateSize
		}
		if opts.WALMaxFileSize > 0 {
			wcfg.MaxFileSize = opts.WALMaxFileSize
		}
		if opts.WALBufferSize > 0 {
			wcfg.BufferSize = opts.WALBufferSize
		}
		w, err := wal.NewWithConfig(walDir, s.logger, wcfg)
		if err != nil {
			return nil, fmt.Errorf("initialize WAL: %w", err)
		}
		s.wal = w
	}

	// Initialize memtable
	s.memtable = memtable.New()

	// Replay WAL to restore memtable
	if s.wal != nil {
		if err := s.replayWAL(); err != nil {
			s.Close()
			return nil, fmt.Errorf("replay WAL: %w", err)
		}
	}

	// Initialize manifest
	var manifestErr error
	s.manifest, manifestErr = manifest.New(dir, s.logger)
	if manifestErr != nil {
		s.logger.Warn("failed to create manifest", "error", manifestErr)
		// Continue without manifest for now
	}

	// Load existing segments
	if err := s.loadSegments(); err != nil {
		s.logger.Warn("failed to load segments", "error", err)
	}

	// Update stats based on loaded manifest/segments
	s.updateStatsFromManifest()

	// Initialize compactor
	if s.manifest != nil && !s.readonly {
		s.compactor = compaction.NewCompactor(dir, s.manifest, s.logger)
	}

	// Load tuning parameters
	if err := s.loadTuning(); err != nil {
		s.logger.Warn("failed to load tuning parameters", "error", err)
	}

	// Start background tasks if not read-only
	if !s.readonly {
		s.startBackgroundTasks()
	}

	s.logger.Info("store opened", "dir", dir, "readonly", s.readonly)

	return s, nil
}

// Close closes the store and releases all resources.
func (s *storeImpl) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil // Already closed
	}

	s.logger.Info("closing store", "dir", s.dir)

	// Stop background tasks
	s.stopBackgroundTasks()

	// Flush memtable if not read-only
	if !s.readonly && s.memtable.Count() > 0 {
		if err := s.Flush(context.Background()); err != nil {
			s.logger.Error("failed to flush on close", "error", err)
		}
	}

	// Close WAL
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			s.logger.Error("failed to close WAL", "error", err)
		}
	}

	s.logger.Info("store closed", "dir", s.dir)

	return nil
}

// Insert adds a string to the store.
func (s *storeImpl) Insert(key []byte) error {
	return s.InsertWithTTL(key, s.opts.DefaultTTL)
}

// InsertWithTTL adds a string to the store with expiration time.
func (s *storeImpl) InsertWithTTL(key []byte, ttl time.Duration) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return common.ErrClosed
	}

	if s.readonly {
		return common.ErrReadOnly
	}

	// Write to WAL first for durability
	if err := s.wal.WriteWithTTL(common.OpInsert, key, ttl); err != nil {
		return fmt.Errorf("write to WAL: %w", err)
	}

	// Insert into memtable
	if err := s.memtable.InsertWithTTL(key, ttl); err != nil {
		return fmt.Errorf("insert to memtable: %w", err)
	}

	// Update statistics
	s.stats.RecordInsert()

	// Check if flush is needed
	if s.shouldFlush() && !s.opts.DisableAutoFlush {
		go s.triggerFlush()
	}

	return nil
}

// Delete removes a string from the store.
func (s *storeImpl) Delete(key []byte) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return common.ErrClosed
	}

	if s.readonly {
		return common.ErrReadOnly
	}

	// Write to WAL first
	if err := s.wal.Write(common.OpDelete, key); err != nil {
		return fmt.Errorf("write to WAL: %w", err)
	}

	// Mark as deleted in memtable (tombstone)
	if err := s.memtable.Delete(key); err != nil {
		return fmt.Errorf("delete from memtable: %w", err)
	}

	// Update statistics
	s.stats.RecordDelete()

	// Check if flush is needed
	if s.shouldFlush() && !s.opts.DisableAutoFlush {
		go s.triggerFlush()
	}

	return nil
}

// RegexSearch performs a regex search on the store.
func (s *storeImpl) RegexSearch(ctx context.Context, re *regexp.Regexp, q *QueryOptions) (Iterator, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, common.ErrClosed
	}

	if q == nil {
		q = DefaultQueryOptions()
	}

	// Update statistics
	s.stats.RecordSearch()

	// Create iterators for memtable and segments
	s.mu.RLock()
	// If no readers yet (e.g., after flush), try to load them
	if len(s.readers) == 0 {
		s.mu.RUnlock()
		s.mu.Lock()
		_ = s.loadSegments()
		s.mu.Unlock()
		s.mu.RLock()
	}
	defer s.mu.RUnlock()

	memIter := s.memtable.RegexSearch(re)

	// Preload tombstones from all readers to suppress older duplicates
	preSeen := make(map[string]struct{}, 1024)
	for _, reader := range s.readers {
		_, _ = reader.IterateTombstones(func(tk []byte) bool {
			preSeen[string(tk)] = struct{}{}
			return true
		})
	}

	// If no segments, return simple iterator
	if len(s.readers) == 0 {
		return &simpleIterator{
			memIter: memIter,
			mode:    q.Mode,
			limit:   q.Limit,
			count:   0,
		}, nil
	}

	// Parallel segment iteration with pruning and cancellation
	out := make(chan segItem, 1024)
	wg := &sync.WaitGroup{}
	sem := make(chan struct{}, max(1, q.MaxParallelism))
	workCtx, workCancel := context.WithCancel(ctx)

	// Intelligent reader selection (no global index)
	selected := s.selectRelevantReaders(re, q)
	for _, reader := range selected {
		r := reader
		if !r.MayContain(re) {
			continue
		}
		wg.Add(1)
		go func() {
			// Acquire semaphore inside goroutine so we do not block iterator construction
			sem <- struct{}{}
			defer wg.Done()
			defer func() { <-sem }()
			it := r.RegexIterator(re)
			defer it.Close()
			for it.Next() {
				select {
				case out <- segItem{id: r.GetSegmentID(), key: append([]byte(nil), it.Key()...), op: common.OpInsert}:
				case <-workCtx.Done():
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()

	// Wrap memtable iterator and drain parallel results into a merged iterator
	return &parallelMergedIterator{
		memIter: memIter,
		in:      out,
		seen:    preSeen,
		mode:    q.Mode,
		limit:   q.Limit,
		cancel:  workCancel,
	}, nil
}

// selectRelevantReaders picks a prioritized subset of readers to scan without building global indexes.
func (s *storeImpl) selectRelevantReaders(re *regexp.Regexp, q *QueryOptions) []*segment.Reader {
	readers := s.readers
	if len(readers) == 0 {
		return readers
	}

	// Extract literal prefix if any
	var lit []byte
	if re != nil {
		if l, _ := re.LiteralPrefix(); l != "" {
			lit = []byte(l)
		}
	}

	type cand struct {
		r         *segment.Reader
		score     int
		createdAt int64
		accepted  uint64
	}
	cands := make([]cand, 0, len(readers))

	// Helper to check rough key-range intersection using metadata
	intersects := func(md *segment.Metadata) bool {
		if md == nil || len(lit) == 0 {
			return true
		}
		minb, err1 := md.GetMinKey()
		maxb, err2 := md.GetMaxKey()
		if err1 != nil || err2 != nil || len(minb) == 0 || len(maxb) == 0 {
			return true
		}
		// Fast reject: if max < lit, no overlap
		if bytes.Compare(maxb, lit) < 0 {
			return false
		}
		// Compute next prefix upper bound; if none, treat as +inf
		np := nextPrefix(lit)
		if np == nil {
			return true
		}
		// If min >= nextPrefix(lit) then range [min,max] is entirely above prefix range
		if bytes.Compare(minb, np) >= 0 {
			return false
		}
		return true
	}

	for _, r := range readers {
		md := r.GetMetadata()
		if md == nil {
			// No metadata; keep but with lower score
			cands = append(cands, cand{r: r, score: 0, createdAt: 0, accepted: 0})
			continue
		}
		// Range-based pruning when literal available
		if !intersects(md) {
			continue
		}
		sc := 0
		// Prefer presence of filters
		if md.Filters.PrefixBloom != nil {
			sc += 3
		}
		if md.Filters.Trigram != nil {
			sc += 1
		}
		// Recency
		ca := md.CreatedAtUnix
		// Smaller segments earlier
		acc := md.Counts.Accepted
		cands = append(cands, cand{r: r, score: sc, createdAt: ca, accepted: acc})
	}

	if len(cands) == 0 {
		// Fallback to all readers; Regex loop will still use MayContain()
		return readers
	}

	// Sort: higher score first, newer first, smaller accepted first
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		if cands[i].createdAt != cands[j].createdAt {
			return cands[i].createdAt > cands[j].createdAt
		}
		return cands[i].accepted < cands[j].accepted
	})

	// Cap the number of segments to launch based on parallelism and limit
	capN := len(cands)
	maxPara := max(1, q.MaxParallelism)
	// Allow 2x parallelism as working set, but not too many
	softCap := maxPara * 2
	if softCap < capN {
		capN = softCap
	}
	if q.Limit > 0 {
		// If user requested few results, we do not need many segments
		hardCap := q.Limit * 2
		if hardCap < maxPara {
			hardCap = maxPara
		}
		if capN > hardCap {
			capN = hardCap
		}
	}

	out := make([]*segment.Reader, 0, capN)
	for i := 0; i < capN && i < len(cands); i++ {
		out = append(out, cands[i].r)
	}
	return out
}

// nextPrefix returns the smallest byte slice that is lexicographically greater than
// all strings having the given prefix. If none exists (all bytes are 0xFF), returns nil.
func nextPrefix(p []byte) []byte {
	if len(p) == 0 {
		return nil
	}
	q := append([]byte(nil), p...)
	for i := len(q) - 1; i >= 0; i-- {
		if q[i] != 0xFF {
			q[i]++
			return q[:i+1]
		}
	}
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// parallelMergedIterator merges memtable and channel of segment results.
type parallelMergedIterator struct {
	memIter *memtable.Iterator
	in      chan segItem
	seen    map[string]struct{}
	mode    QueryMode
	limit   int
	count   int
	cur     struct {
		id  uint64
		key []byte
		op  uint8
	}
	err    error
	cancel context.CancelFunc
}

func (it *parallelMergedIterator) Next(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		if it.cancel != nil {
			it.cancel()
		}
		return false
	}
	// drain memtable first
	for it.memIter != nil && it.memIter.Next(ctx) {
		k := it.memIter.Key()
		ks := string(k)
		if _, ok := it.seen[ks]; ok {
			continue
		}
		it.seen[ks] = struct{}{}
		it.cur.key = k
		it.cur.id = it.memIter.SeqNum()
		if it.memIter.IsTombstone() {
			it.cur.op = common.OpDelete
		} else {
			it.cur.op = common.OpInsert
		}
		it.count++
		return true
	}
	it.memIter = nil
	// then read from segments channel
	for it.in != nil {
		select {
		case x, ok := <-it.in:
			if !ok {
				it.in = nil
				return false
			}
			if x.err != nil {
				it.err = x.err
				continue
			}
			ks := string(x.key)
			if _, ok := it.seen[ks]; ok {
				continue
			}
			it.seen[ks] = struct{}{}
			it.cur.key = x.key
			it.cur.id = x.id << 32
			it.cur.op = x.op
			it.count++
			return true
		case <-ctx.Done():
			it.err = ctx.Err()
			if it.cancel != nil {
				it.cancel()
			}
			return false
		}
	}
	return false
}

func (it *parallelMergedIterator) Err() error { return it.err }
func (it *parallelMergedIterator) ID() uint64 { return it.cur.id }
func (it *parallelMergedIterator) String() []byte {
	if it.mode == CountOnly {
		return nil
	}
	return it.cur.key
}
func (it *parallelMergedIterator) Op() uint8 { return it.cur.op }
func (it *parallelMergedIterator) Close() error {
	if it.cancel != nil {
		it.cancel()
	}
	return nil
}

// PrefixScan returns an iterator over keys starting with the given prefix.
func (s *storeImpl) PrefixScan(ctx context.Context, prefix []byte, q *QueryOptions) (Iterator, error) {
	// Implement via regex anchor for now: ^<escaped_prefix>.*
	if q == nil {
		q = DefaultQueryOptions()
	}
	// Escape regex meta in prefix
	esc := regexp.QuoteMeta(string(prefix))
	re := regexp.MustCompile("^" + esc + ".*")
	return s.RegexSearch(ctx, re, q)
}

// RangeScan returns an iterator over keys in [start, end) range.
func (s *storeImpl) RangeScan(ctx context.Context, start, end []byte, q *QueryOptions) (Iterator, error) {
	if q == nil {
		q = DefaultQueryOptions()
	}
	// Implement minimal version by filtering RegexSearch(".*")
	// and wrapping with a client-side filter iterator.
	re := regexp.MustCompile(".*")
	base, err := s.RegexSearch(ctx, re, q)
	if err != nil {
		return nil, err
	}
	return &rangeFilterIterator{base: base, start: start, end: end, mode: q.Mode, limit: q.Limit}, nil
}

type rangeFilterIterator struct {
	base  Iterator
	start []byte
	end   []byte
	mode  QueryMode
	limit int
	count int
	cur   struct {
		id  uint64
		key []byte
		op  uint8
	}
	err error
}

func (it *rangeFilterIterator) Next(ctx context.Context) bool {
	if it.limit > 0 && it.count >= it.limit {
		return false
	}
	for it.base.Next(ctx) {
		key := it.base.String()
		if len(it.start) > 0 && bytes.Compare(key, it.start) < 0 {
			continue
		}
		if len(it.end) > 0 && bytes.Compare(key, it.end) >= 0 {
			continue
		}
		it.cur.key = key
		it.cur.id = it.base.ID()
		it.cur.op = it.base.Op()
		it.count++
		return true
	}
	it.err = it.base.Err()
	return false
}

func (it *rangeFilterIterator) Err() error { return it.err }
func (it *rangeFilterIterator) ID() uint64 { return it.cur.id }
func (it *rangeFilterIterator) String() []byte {
	if it.mode == CountOnly {
		return nil
	}
	return it.cur.key
}
func (it *rangeFilterIterator) Op() uint8    { return it.cur.op }
func (it *rangeFilterIterator) Close() error { return it.base.Close() }

// Stats returns current statistics for the store.
func (s *storeImpl) Stats() Stats {
	return s.stats.GetStats()
}

// RefreshStats forces a refresh of statistics.
func (s *storeImpl) RefreshStats() {
	s.stats.Refresh()
}

// Tune adjusts tuning parameters at runtime.
func (s *storeImpl) Tune(params TuningParams) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if params.MemtableTargetBytes != nil {
		s.opts.MemtableTargetBytes = *params.MemtableTargetBytes
	}

	if params.CacheLabelAdvanceBytes != nil {
		s.opts.CacheLabelAdvanceBytes = *params.CacheLabelAdvanceBytes
	}

	if params.CacheNFATransitionBytes != nil {
		s.opts.CacheNFATransitionBytes = *params.CacheNFATransitionBytes
	}

	s.tuningParams = params

	// Save tuning parameters
	if !s.readonly {
		if err := s.saveTuning(); err != nil {
			s.logger.Error("failed to save tuning parameters", "error", err)
		}
	}
}

// CompactNow triggers immediate compaction.
func (s *storeImpl) CompactNow(ctx context.Context) error {
	if s.readonly {
		return common.ErrReadOnly
	}

	// Prepare inputs under lock
	s.mu.Lock()
	if len(s.readers) == 0 {
		if err := s.loadSegments(); err != nil {
			s.logger.Warn("failed to load segments before compaction", "error", err)
		}
	}
	segmentsDir := filepath.Join(s.dir, common.DirSegments)
	readersSnapshot := append([]*segment.Reader(nil), s.readers...)
	s.mu.Unlock()

	// Dedup set across all inputs (newest wins)
	seen := make(map[[32]byte]struct{}, 1024)

	// Chunked building to cap memory/size
	const chunkTargetBytes = int64(512 * 1024 * 1024)
	type chunkState struct {
		builder       *segment.Builder
		id            uint64
		approxBytes   int64
		acceptedCount uint64
		liveAdded     bool
	}
	var cur chunkState
	newReaders := make([]*segment.Reader, 0, 8)
	ensureBuilder := func() {
		if cur.builder != nil {
			return
		}
		cur.id = atomic.AddUint64(&s.nextSegmentID, 1)
		cur.builder = segment.NewBuilder(cur.id, common.LevelL0, segmentsDir, s.logger)
		cur.approxBytes = 0
		cur.acceptedCount = 0
		cur.liveAdded = false
	}
	finalizeChunk := func() error {
		if cur.builder == nil {
			return nil
		}
		if !cur.liveAdded {
			cur.builder.DropAllTombstones()
		}
		md, err := cur.builder.Build()
		if err != nil {
			s.logger.Warn("compaction built minimal segment", "error", err)
			cur.builder = nil
			cur.id = 0
			cur.approxBytes = 0
			cur.acceptedCount = 0
			cur.liveAdded = false
			return nil
		}
		// Open reader for the compacted chunk and stage it
		reader, rerr := segment.NewReader(cur.id, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad)
		if rerr == nil {
			newReaders = append(newReaders, reader)
		} else {
			s.logger.Warn("failed to open reader for compacted chunk", "id", cur.id, "error", rerr)
		}
		// Add to manifest if available
		if s.manifest != nil {
			info := manifest.SegmentInfo{ID: cur.id, Level: common.LevelL0, NumKeys: md.Counts.Accepted}
			info.Size = s.computeSegmentSize(cur.id)
			if minb, err := md.GetMinKey(); err == nil {
				info.MinKeyHex = fmt.Sprintf("%x", minb)
			}
			if maxb, err := md.GetMaxKey(); err == nil {
				info.MaxKeyHex = fmt.Sprintf("%x", maxb)
			}
			if err := s.manifest.AddSegment(info); err != nil {
				s.logger.Warn("failed to add compacted segment to manifest", "error", err)
			}
		}
		// Reset state
		cur.builder = nil
		cur.id = 0
		cur.approxBytes = 0
		cur.acceptedCount = 0
		cur.liveAdded = false
		return nil
	}
	maybeFlush := func() error {
		if cur.approxBytes >= chunkTargetBytes {
			return finalizeChunk()
		}
		return nil
	}

	// Memtable first
	snapshot := s.memtable.Snapshot()
	all := memtable.NewAllIterator(snapshot)
	for all.Next() {
		k := all.Key()
		h := blake3.Sum256(k)
		if _, ok := seen[h]; ok {
			continue
		}
		ensureBuilder()
		if all.IsTombstone() {
			cur.builder.AddTombstone(k)
			cur.approxBytes += int64(len(k))
		} else {
			if err := cur.builder.Add(k, k, false); err != nil {
				return fmt.Errorf("builder add: %w", err)
			}
			cur.approxBytes += int64(len(k))
			cur.acceptedCount++
			cur.liveAdded = true
		}
		seen[h] = struct{}{}
		if err := maybeFlush(); err != nil {
			return err
		}
	}

	// K-way merge state for sorted sources
	type srcIter struct {
		key   []byte
		ok    bool
		next  func() ([]byte, bool)
		order int // higher = newer
	}
	less := func(a, b *srcIter) bool {
		cmp := bytes.Compare(a.key, b.key)
		if cmp != 0 {
			return cmp < 0
		}
		return a.order > b.order // for equal keys, newer first
	}
	// Build source iterators: memtable (posortowany) + segments keys; tombstones heap to suppress
	srcs := make([]*srcIter, 0, len(readersSnapshot)+1)
	type tsIter struct {
		key  []byte
		ok   bool
		next func() ([]byte, bool)
	}
	tsHeap := make([]*tsIter, 0, len(readersSnapshot)+1)
	tsLess := func(i, j int) bool { return bytes.Compare(tsHeap[i].key, tsHeap[j].key) < 0 }
	pushTS := func(ti *tsIter) {
		if ti.ok {
			tsHeap = append(tsHeap, ti)
			sort.Slice(tsHeap, tsLess)
		}
	}
	popTS := func() *tsIter {
		if len(tsHeap) == 0 {
			return nil
		}
		x := tsHeap[0]
		tsHeap = tsHeap[1:]
		return x
	}

	// memtable iterator
	{
		it := memtable.NewAllIterator(snapshot)
		advance := func() ([]byte, bool) {
			if it.Next() {
				if it.IsTombstone() {
					return nil, true
				} // memtable tombs handled by deletes phase; ignore here
				return it.Key(), true
			}
			return nil, false
		}
		k, ok := advance()
		srcs = append(srcs, &srcIter{key: k, ok: ok, next: advance, order: len(readersSnapshot) + 1})
	}
	// segments
	for idx := len(readersSnapshot) - 1; idx >= 0; idx-- { // newest first gets wyższy priorytet
		r := readersSnapshot[idx]
		advance := r.StreamKeys()
		k, ok := advance()
		srcs = append(srcs, &srcIter{key: k, ok: ok, next: advance, order: idx + 1})
		// tombstones stream
		advT := r.StreamTombstones()
		if tk, tok := advT(); tok {
			pushTS(&tsIter{key: tk, ok: tok, next: advT})
		}
	}
	// Min-heap
	h := make([]*srcIter, 0, len(srcs))
	push := func(si *srcIter) {
		if si.ok {
			h = append(h, si)
			sort.Slice(h, func(i, j int) bool { return less(h[i], h[j]) })
		}
	}
	pop := func() *srcIter {
		if len(h) == 0 {
			return nil
		}
		x := h[0]
		h = h[1:]
		return x
	}
	for _, siter := range srcs {
		push(siter)
	}

	var curKey []byte
	for len(h) > 0 {
		si := pop()
		if si.key == nil && si.ok { // skip empty
			if k, ok := si.next(); ok {
				si.key = k
				si.ok = true
				push(si)
			}
			continue
		}
		// New key or same key but older
		emit := false
		if curKey == nil || !bytes.Equal(si.key, curKey) {
			// finalize duplicate run: nothing to do
			curKey = append([]byte(nil), si.key...)
			_ = si.order
			// check tombstone in newer sources? For simplicity: we don't carry tombstones in memtable iterator; deletions are handled earlier
			emit = true
		} else {
			// same key; newer already chosen; skip
			emit = false
		}
		if emit {
			// advance tombstone heap to current key (drop any ts < curKey)
			for {
				if len(tsHeap) == 0 {
					break
				}
				if bytes.Compare(tsHeap[0].key, curKey) < 0 {
					ti := popTS()
					if nk, ok := ti.next(); ok {
						ti.key = nk
						ti.ok = true
						pushTS(ti)
					}
					continue
				}
				break
			}
			// if tombstone equals current key, suppress emit and advance that tombstone
			if len(tsHeap) > 0 && bytes.Equal(tsHeap[0].key, curKey) {
				ti := popTS()
				if nk, ok := ti.next(); ok {
					ti.key = nk
					ti.ok = true
					pushTS(ti)
				}
				// do not emit this key
			} else {
				ensureBuilder()
				if err := cur.builder.Add(curKey, curKey, false); err != nil {
					return fmt.Errorf("builder add: %w", err)
				}
				cur.approxBytes += int64(len(curKey))
				cur.acceptedCount++
				cur.liveAdded = true
				if err := maybeFlush(); err != nil {
					return err
				}
			}
		}
		if k, ok := si.next(); ok {
			si.key = k
			si.ok = true
			push(si)
		}
	}

	// Finalize last chunk
	if err := finalizeChunk(); err != nil {
		return err
	}

	// Replace readers: if produced new segments, swap in; if none, clear readers entirely
	s.mu.Lock()
	if len(newReaders) > 0 {
		for _, old := range s.readers {
			_ = old.Close()
		}
		s.readers = newReaders
	} else {
		for _, old := range s.readers {
			_ = old.Close()
		}
		s.readers = nil
	}
	s.mu.Unlock()

	// Remove old segments from manifest
	if s.manifest != nil {
		if len(s.segments) > 0 {
			oldIDs := make([]uint64, 0, len(s.segments))
			for _, md := range s.segments {
				oldIDs = append(oldIDs, md.SegmentID)
			}
			_ = s.manifest.RemoveSegments(oldIDs)
		}
	}

	// Replace in-memory segments metadata with none for now
	s.mu.Lock()
	s.segments = nil
	s.mu.Unlock()

	// Refresh stats after compaction
	s.updateStatsFromManifest()
	return nil
}

// PruneWAL deletes obsolete WAL files older than the current WAL sequence.
func (s *storeImpl) PruneWAL() error {
	if s.readonly {
		return common.ErrReadOnly
	}
	if s.wal == nil {
		return nil
	}
	seq := s.wal.CurrentSeq()
	return s.wal.DeleteOldFiles(seq)
}

// Flush forces a flush of the memtable to disk.
func (s *storeImpl) Flush(ctx context.Context) error {
	if s.readonly {
		return common.ErrReadOnly
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.memtable.Count() == 0 {
		// If there are no inserts but there are tombstones, still flush to persist deletions
		if s.memtable.DeletedCount() == 0 {
			return nil // Nothing to flush
		}
	}

	start := time.Now()
	s.logger.Info("starting flush", "entries", s.memtable.Count())

	// Create a snapshot of the memtable
	snapshot := s.memtable.Snapshot()

	// Build segment from memtable
	segmentID := atomic.AddUint64(&s.nextSegmentID, 1)
	segmentsDir := filepath.Join(s.dir, common.DirSegments)

	builder := segment.NewBuilder(segmentID, common.LevelL0, segmentsDir, s.logger)
	if err := builder.AddFromMemtable(snapshot); err != nil {
		return fmt.Errorf("add memtable to builder: %w", err)
	}

	metadata, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build segment: %w", err)
	}

	// Add segment to list
	s.segments = append(s.segments, metadata)

	// Open reader for the new segment so queries can see it immediately
	reader, rerr := segment.NewReader(segmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad)
	if rerr != nil {
		// Non-fatal: log and continue
		s.logger.Warn("failed to open reader for new segment", "id", segmentID, "error", rerr)
	} else {
		s.readers = append(s.readers, reader)
	}

	// Add to manifest if available
	if s.manifest != nil {
		info := manifest.SegmentInfo{
			ID:      segmentID,
			Level:   common.LevelL0,
			NumKeys: uint64(snapshot.Count()),
			Size:    s.computeSegmentSize(segmentID),
		}
		// Populate min/max HEX from metadata
		if minb, err := metadata.GetMinKey(); err == nil {
			info.MinKeyHex = fmt.Sprintf("%x", minb)
		}
		if maxb, err := metadata.GetMaxKey(); err == nil {
			info.MaxKeyHex = fmt.Sprintf("%x", maxb)
		}
		if err := s.manifest.AddSegment(info); err != nil {
			s.logger.Warn("failed to add segment to manifest", "error", err)
		}
	}

	// Refresh stats after flush
	s.updateStatsFromManifest()

	// Sync WAL
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}

	// Create new memtable
	s.memtable = memtable.New()

	// Rotate WAL to start a fresh file after flush
	if s.wal != nil && s.opts.RotateWALOnFlush {
		if err := s.wal.Rotate(); err != nil {
			s.logger.Warn("failed to rotate WAL after flush", "error", err)
		}
	}
	// Note: do not force WAL rotation here; let size-based rotation control WAL file size

	s.lastFlushTime = time.Now()
	s.stats.RecordFlush(time.Since(start))

	s.logger.Info("flush completed", "duration", time.Since(start), "segmentID", segmentID)

	return nil
}

// RCUEnabled returns whether RCU is enabled.
func (s *storeImpl) RCUEnabled() bool {
	return s.rcuEnabled
}

// AdvanceRCU advances the RCU epoch.
func (s *storeImpl) AdvanceRCU(ctx context.Context) error {
	if !s.rcuEnabled {
		return fmt.Errorf("RCU is not enabled")
	}

	atomic.AddUint64(&s.rcuEpoch, 1)
	s.logger.Info("RCU epoch advanced", "epoch", s.rcuEpoch)

	return nil
}

// VacuumPrefix removes all keys with the specified prefix.
func (s *storeImpl) VacuumPrefix(ctx context.Context, prefix []byte) error {
	if s.readonly {
		return common.ErrReadOnly
	}

	// Delete keys by writing tombstones for matches
	esc := regexp.QuoteMeta(string(prefix))
	re := regexp.MustCompile("^" + esc + ".*")
	iter, err := s.RegexSearch(ctx, re, DefaultQueryOptions())
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next(ctx) {
		if err := s.Delete(iter.String()); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

// SetAutotunerEnabled enables or disables the autotuner.
func (s *storeImpl) SetAutotunerEnabled(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.autotunerEnabled = enabled
	s.logger.Info("autotuner state changed", "enabled", enabled)
}

// replayWAL replays the WAL to restore the memtable.
func (s *storeImpl) replayWAL() error {
	count := 0
	err := s.wal.Replay(func(op uint8, key []byte, ttl time.Duration) error {
		switch op {
		case common.OpInsert:
			if err := s.memtable.InsertWithTTL(key, ttl); err != nil {
				return err
			}
		case common.OpDelete:
			if err := s.memtable.Delete(key); err != nil {
				return err
			}
		default:
			s.logger.Warn("unknown operation in WAL", "op", op)
		}
		count++
		return nil
	})

	if err != nil {
		return err
	}

	s.logger.Info("WAL replay completed", "records", count)
	return nil
}

// shouldFlush checks if the memtable should be flushed.
func (s *storeImpl) shouldFlush() bool {
	return s.memtable.Size() >= s.opts.MemtableTargetBytes
}

// triggerFlush triggers an asynchronous flush.

func (s *storeImpl) triggerFlush() {
	if err := s.Flush(context.Background()); err != nil {
		s.logger.Error("async flush failed", "error", err)
	}
}

// startBackgroundTasks starts all background tasks.
func (s *storeImpl) startBackgroundTasks() {
	// Flush task
	if !s.opts.DisableAutoFlush {
		s.flushStop = make(chan struct{})
		s.flushTicker = time.NewTicker(10 * time.Second)
		s.bgWg.Add(1)
		go s.flushTask()
	}

	// Compaction task
	if s.compactor != nil && !s.opts.DisableBackgroundCompaction {
		s.compactStop = make(chan struct{})
		s.bgWg.Add(1)
		go s.compactionTask()

		// Start compactor background loop
		ctx := context.Background()
		s.compactor.Start(ctx)
	}

	// Autotuning task
	if s.autotunerEnabled {
		s.autotuneStop = make(chan struct{})
		s.bgWg.Add(1)
		go s.autotuneTask()
	}

	// RCU cleanup task
	if s.rcuEnabled {
		s.rcuStop = make(chan struct{})
		s.bgWg.Add(1)
		go s.rcuCleanupTask()
	}
}

// stopBackgroundTasks stops all background tasks.
func (s *storeImpl) stopBackgroundTasks() {
	// Stop flush task
	if s.flushStop != nil {
		close(s.flushStop)
		if s.flushTicker != nil {
			s.flushTicker.Stop()
		}
	}

	// Stop compaction task
	if s.compactStop != nil {
		close(s.compactStop)
	}
	// Stop compactor loop
	if s.compactor != nil {
		s.compactor.Stop()
	}

	// Stop autotune task
	if s.autotuneStop != nil {
		close(s.autotuneStop)
	}

	// Stop RCU cleanup task
	if s.rcuStop != nil {
		close(s.rcuStop)
	}

	// Wait for all tasks to complete
	s.bgWg.Wait()

	// Close open segment readers
	s.mu.Lock()
	for _, r := range s.readers {
		_ = r.Close()
	}
	s.readers = nil
	s.mu.Unlock()
}

// flushTask is the background flush task.
func (s *storeImpl) flushTask() {
	defer s.bgWg.Done()

	for {
		select {
		case <-s.flushStop:
			return
		case <-s.flushTicker.C:
			if s.shouldFlush() {
				if err := s.Flush(context.Background()); err != nil {
					s.logger.Error("periodic flush failed", "error", err)
				}
			}
		}
	}
}

// compactionTask is the background compaction task.
func (s *storeImpl) compactionTask() {
	defer s.bgWg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.compactStop:
			return
		case <-ticker.C:
			// Naive background compaction: if too many segment readers, compact
			s.mu.RLock()
			readerCount := len(s.readers)
			s.mu.RUnlock()
			if readerCount > 4 {
				_ = s.CompactNow(context.Background())
			}
		}
	}
}

// autotuneTask is the background autotuning task.
func (s *storeImpl) autotuneTask() {
	defer s.bgWg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.autotuneStop:
			return
		case <-ticker.C:
			// Simple autotuning heuristic:
			// - If write rate is high, increase memtable target up to 2x
			// - If query rate is high, bias caches up to configured budgets
			s.stats.Refresh()
			st := s.stats.GetStats()
			// Adjust memtable size
			if st.WritesPerSecond > 500 { // threshold heuristic
				newTarget := s.opts.MemtableTargetBytes * 2
				if newTarget > common.DefaultMemtableTargetBytes*8 {
					newTarget = common.DefaultMemtableTargetBytes * 8
				}
				s.Tune(TuningParams{MemtableTargetBytes: &newTarget})
			}
			// No-op else branch could shrink over time; keep simple for now
			_ = st
		}
	}
}

// rcuCleanupTask is the background RCU cleanup task.
func (s *storeImpl) rcuCleanupTask() {
	defer s.bgWg.Done()

	ticker := time.NewTicker(s.opts.RCUCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.rcuStop:
			return
		case <-ticker.C:
			// Remove physically segments marked deleted past grace period
			if s.manifest == nil {
				continue
			}
			active := s.manifest.GetActiveSegments()
			// Collect all segment IDs currently active
			activeIDs := make(map[uint64]struct{}, len(active))
			for _, seg := range active {
				activeIDs[seg.ID] = struct{}{}
			}
			// Scan filesystem segments and delete those not active and older than grace
			segmentsDir := filepath.Join(s.dir, common.DirSegments)
			entries, err := os.ReadDir(segmentsDir)
			if err != nil {
				continue
			}
			cutoff := time.Now().Add(-time.Duration(common.RCUGracePeriod) * time.Second).Unix()
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				var segID uint64
				if _, err := fmt.Sscanf(e.Name(), "%016d", &segID); err != nil {
					continue
				}
				if _, ok := activeIDs[segID]; ok {
					continue
				}
				segPath := filepath.Join(segmentsDir, e.Name())
				// Check dir mtime as proxy for age
				if info, err := os.Stat(segPath); err == nil {
					if info.ModTime().Unix() < cutoff {
						_ = os.RemoveAll(segPath)
						s.logger.Info("rcu removed obsolete segment dir", "id", segID)
					}
				}
			}
		}
	}
}

// loadSegments loads existing segments from disk.
func (s *storeImpl) loadSegments() error {
	segmentsDir := filepath.Join(s.dir, common.DirSegments)

	// Prefer manifest listing when available
	if s.manifest != nil {
		active := s.manifest.GetActiveSegments()
		for _, seg := range active {
			segmentID := seg.ID
			// Load segment metadata
			metadataPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", segmentID), "segment.json")
			metadata, err := segment.LoadFromFile(metadataPath)
			if err != nil {
				s.logger.Warn("failed to load segment metadata", "id", segmentID, "error", err)
				continue
			}
			reader, err := segment.NewReader(segmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad)
			if err != nil {
				s.logger.Warn("failed to create segment reader", "id", segmentID, "error", err)
				continue
			}
			s.segments = append(s.segments, metadata)
			s.readers = append(s.readers, reader)
			if segmentID >= s.nextSegmentID {
				s.nextSegmentID = segmentID + 1
			}
		}
		s.logger.Info("loaded segments", "count", len(s.segments))
		return nil
	}

	// Fallback: scan filesystem
	// Check if segments directory exists
	if _, err := os.Stat(segmentsDir); os.IsNotExist(err) {
		return nil // No segments yet
	}

	// List segment directories
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return fmt.Errorf("read segments directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Parse segment ID
		var segmentID uint64
		if _, err := fmt.Sscanf(entry.Name(), "%016d", &segmentID); err != nil {
			continue
		}

		// Load segment metadata
		metadataPath := filepath.Join(segmentsDir, entry.Name(), "segment.json")
		metadata, err := segment.LoadFromFile(metadataPath)
		if err != nil {
			s.logger.Warn("failed to load segment metadata", "id", segmentID, "error", err)
			continue
		}

		// Create segment reader
		reader, err := segment.NewReader(segmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad)
		if err != nil {
			s.logger.Warn("failed to create segment reader", "id", segmentID, "error", err)
			continue
		}

		s.segments = append(s.segments, metadata)
		s.readers = append(s.readers, reader)

		// Update next segment ID
		if segmentID >= s.nextSegmentID {
			s.nextSegmentID = segmentID + 1
		}
	}

	s.logger.Info("loaded segments", "count", len(s.segments))
	return nil
}

// loadTuning loads tuning parameters from disk.
func (s *storeImpl) loadTuning() error {
	path := filepath.Join(s.dir, "tuning.json")
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	type onDisk struct {
		MemtableTargetBytes     *int64 `json:"memtable_target_bytes"`
		CacheLabelAdvanceBytes  *int64 `json:"cache_label_advance_bytes"`
		CacheNFATransitionBytes *int64 `json:"cache_nfa_transition_bytes"`
	}
	var cfg onDisk
	dec := json.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return nil
	}
	if cfg.MemtableTargetBytes != nil {
		s.opts.MemtableTargetBytes = *cfg.MemtableTargetBytes
	}
	if cfg.CacheLabelAdvanceBytes != nil {
		s.opts.CacheLabelAdvanceBytes = *cfg.CacheLabelAdvanceBytes
	}
	if cfg.CacheNFATransitionBytes != nil {
		s.opts.CacheNFATransitionBytes = *cfg.CacheNFATransitionBytes
	}
	return nil
}

// saveTuning saves tuning parameters to disk.
func (s *storeImpl) saveTuning() error {
	path := filepath.Join(s.dir, "tuning.json")
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	type onDisk struct {
		MemtableTargetBytes     int64 `json:"memtable_target_bytes"`
		CacheLabelAdvanceBytes  int64 `json:"cache_label_advance_bytes"`
		CacheNFATransitionBytes int64 `json:"cache_nfa_transition_bytes"`
	}
	cfg := onDisk{
		MemtableTargetBytes:     s.opts.MemtableTargetBytes,
		CacheLabelAdvanceBytes:  s.opts.CacheLabelAdvanceBytes,
		CacheNFATransitionBytes: s.opts.CacheNFATransitionBytes,
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&cfg); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// createStoreDirectories creates the necessary directories for the store.
func createStoreDirectories(dir string) error {
	dirs := []string{
		dir,
		filepath.Join(dir, common.DirWAL),
		filepath.Join(dir, common.DirSegments),
		filepath.Join(dir, common.DirManifest),
	}

	for _, d := range dirs {
		if err := createDirIfNotExists(d); err != nil {
			return err
		}
	}

	return nil
}

// createDirIfNotExists creates a directory if it doesn't exist.
func createDirIfNotExists(dir string) error {
	return os.MkdirAll(dir, 0755)
}
