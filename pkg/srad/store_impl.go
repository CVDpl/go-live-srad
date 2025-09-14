package srad

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
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
	wal         *wal.WAL
	memtablePtr atomic.Pointer[memtable.Memtable]

	// Guard to serialize memtable swap with in-flight writes
	mtGuard sync.RWMutex

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
	for _, rel := range []string{"filters/prefix.bf", "filters/tri.bits", "keys.dat", "expiry.dat", "tombstones.dat"} {
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
		// Durability options
		wcfg.SyncOnEveryWrite = opts.WALSyncOnEveryWrite
		wcfg.FlushOnEveryWrite = opts.WALFlushOnEveryWrite
		wcfg.FlushEveryBytes = opts.WALFlushEveryBytes
		w, err := wal.NewWithConfig(walDir, s.logger, wcfg)
		if err != nil {
			return nil, fmt.Errorf("initialize WAL: %w", err)
		}
		s.wal = w
	}

	// Initialize memtable
	s.memtablePtr.Store(memtable.New())

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
		alloc := func() uint64 { return atomic.AddUint64(&s.nextSegmentID, 1) }
		s.compactor = compaction.NewCompactor(dir, s.manifest, s.logger, alloc)
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
	if !s.readonly {
		if mt := s.memtablePtr.Load(); mt != nil && (mt.Count() > 0 || mt.DeletedCount() > 0) {
			if err := s.Flush(context.Background()); err != nil {
				s.logger.Error("failed to flush on close", "error", err)
			}
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

	// Validate before WAL write
	if len(key) == 0 {
		return common.ErrEmptyKey
	}
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}
	if ttl < 0 || ttl > common.MaxTTL {
		return common.ErrInvalidTTL
	}

	// Write to WAL for durability
	if err := s.wal.WriteWithTTL(common.OpInsert, key, ttl); err != nil {
		return fmt.Errorf("write to WAL: %w", err)
	}

	// Insert into current memtable (atomic load) under read guard
	s.mtGuard.RLock()
	mt := s.memtablePtr.Load()
	if mt == nil {
		s.mtGuard.RUnlock()
		return fmt.Errorf("memtable not initialized")
	}
	err := mt.InsertWithTTL(key, ttl)
	s.mtGuard.RUnlock()
	if err != nil {
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

	// Validate before WAL write
	if len(key) == 0 {
		return common.ErrEmptyKey
	}
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}

	// Write to WAL first
	if err := s.wal.Write(common.OpDelete, key); err != nil {
		return fmt.Errorf("write to WAL: %w", err)
	}

	// Mark as deleted in memtable (tombstone) under read guard
	s.mtGuard.RLock()
	mt := s.memtablePtr.Load()
	if mt == nil {
		s.mtGuard.RUnlock()
		return fmt.Errorf("memtable not initialized")
	}
	err := mt.Delete(key)
	s.mtGuard.RUnlock()
	if err != nil {
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

	// Use a snapshot to avoid racing with concurrent writers
	var memIter *memtable.Iterator
	var memSnap *memtable.Snapshot
	if mt := s.memtablePtr.Load(); mt != nil {
		memSnap = mt.Snapshot()
		memIter = memSnap.RegexSearch(re)
	}
	// Seed seen with memtable tombstones only (do not preload segment tombstones globally)
	seen := make(map[string]struct{}, 1024)
	seenMu := &sync.Mutex{}
	if memSnap != nil {
		keys, tombs := memSnap.GetAllWithTombstones()
		for i := range keys {
			if tombs[i] {
				seenMu.Lock()
				seen[string(keys[i])] = struct{}{}
				seenMu.Unlock()
			}
		}
	}
	// If pattern is broad (nil or ".*"), bypass memtable regex iterator and stream all memtable keys via channel
	broad := false
	if re == nil {
		broad = true
	} else {
		if re.String() == ".*" || re.String() == "^.*$" {
			broad = true
		}
	}
	if broad {
		memIter = nil
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

	// Choose relevant readers and order newest-first
	selected := s.selectRelevantReaders(re, q)
	// If pattern is nil or ".*", disable filter pruning by scanning all readers
	unsafeBroadScan := false
	if re == nil {
		unsafeBroadScan = true
	} else {
		if re.String() == ".*" || re.String() == "^.*$" {
			unsafeBroadScan = true
		}
	}
	if unsafeBroadScan {
		// override with all readers newest-first
		selected = append([]*segment.Reader(nil), s.readers...)
	}
	sort.Slice(selected, func(i, j int) bool {
		mi := selected[i].GetMetadata()
		mj := selected[j].GetMetadata()
		if mi == nil || mj == nil {
			return false
		}
		if mi.CreatedAtUnix != mj.CreatedAtUnix {
			return mi.CreatedAtUnix > mj.CreatedAtUnix
		}
		return selected[i].GetSegmentID() > selected[j].GetSegmentID()
	})

	out := make(chan segItem, 1024)
	go func(readers []*segment.Reader) {
		defer close(out)
		// For broad scans, emit all memtable keys first via channel
		if broad && memSnap != nil {
			all := memSnap.GetAll()
			emitted := 0
			for _, k := range all {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if re != nil && !re.Match(k) {
					continue
				}
				out <- segItem{id: 0, key: append([]byte(nil), k...), op: common.OpInsert}
				emitted++
			}
			s.logger.Info("broad scan emitted memtable keys", "count", emitted)
		}
		for _, r := range readers {
			if !unsafeBroadScan {
				if re != nil && !r.MayContain(re) {
					continue
				}
			}
			r.IncRef()
			// ensure release regardless of how we exit
			defer r.Release()
			// preload this reader's tombstones to suppress older dupes
			_, _ = r.IterateTombstones(func(tk []byte) bool {
				seenMu.Lock()
				seen[string(tk)] = struct{}{}
				seenMu.Unlock()
				return true
			})
			emitted := 0
			adv := r.StreamKeys()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				k, ok := adv()
				if !ok {
					break
				}
				if re != nil && !re.Match(k) {
					continue
				}
				out <- segItem{id: r.GetSegmentID(), key: append([]byte(nil), k...), op: common.OpInsert}
				emitted++
			}
			s.logger.Info("broad scan emitted segment keys", "segment", r.GetSegmentID(), "count", emitted)
		}
	}(selected)

	// Drain into merged iterator (memtable first)
	return &parallelMergedIterator{
		memIter: memIter,
		in:      out,
		seen:    seen,
		seenMu:  seenMu,
		mode:    q.Mode,
		limit:   q.Limit,
		cancel:  nil,
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

// fnv32 returns FNV-1a 32-bit hash of b.
func fnv32(b []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(b)
	return h.Sum32()
}

// parallelMergedIterator merges memtable and channel of segment results.
type parallelMergedIterator struct {
	memIter *memtable.Iterator
	in      chan segItem
	seen    map[string]struct{}
	seenMu  *sync.Mutex
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
		it.seenMu.Lock()
		if _, ok := it.seen[ks]; ok {
			it.seenMu.Unlock()
			continue
		}
		it.seen[ks] = struct{}{}
		it.seenMu.Unlock()
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
			it.seenMu.Lock()
			if _, ok := it.seen[ks]; ok {
				it.seenMu.Unlock()
				continue
			}
			it.seen[ks] = struct{}{}
			it.seenMu.Unlock()
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
	// Increment refcounts to protect lifetime during compaction
	for _, r := range readersSnapshot {
		r.IncRef()
	}
	s.mu.Unlock()

	// Partition readers by first-byte bucket (0..255) folded to P buckets
	parts := s.opts.BuildRangePartitions
	if parts <= 1 {
		parts = 1
	}
	buckets := make([][]*segment.Reader, parts)
	for _, r := range readersSnapshot {
		// Heuristic: use metadata minKey to choose bucket; fallback 0
		minK, _ := r.MinKey()
		idx := 0
		if len(minK) > 0 {
			idx = int(minK[0]) * parts / 256
			if idx >= parts {
				idx = parts - 1
			}
		}
		buckets[idx] = append(buckets[idx], r)
	}

	// If range partition collapses, fallback to hash-based distribution for better parallelism
	if parts > 1 {
		nonEmpty := 0
		for i := 0; i < parts; i++ {
			if len(buckets[i]) > 0 {
				nonEmpty++
			}
		}
		if nonEmpty <= 1 {
			for i := 0; i < parts; i++ {
				buckets[i] = buckets[i][:0]
			}
			for _, r := range readersSnapshot {
				minK, _ := r.MinKey()
				var idx int
				if len(minK) > 0 {
					idx = int(fnv32(minK) % uint32(parts))
				} else {
					idx = int(r.GetSegmentID() % uint64(parts))
				}
				buckets[idx] = append(buckets[idx], r)
			}
			s.logger.Info("compaction using hash-based partitioning", "parts", parts)
		}
	}

	// Dedup set across all inputs (newest wins) kept per bucket scope
	type compRes struct {
		readers []*segment.Reader
		err     error
	}
	out := make(chan compRes, parts)
	var wg sync.WaitGroup
	for b := 0; b < parts; b++ {
		rs := buckets[b]
		if len(rs) == 0 {
			continue
		}
		wg.Add(1)
		go func(rs []*segment.Reader) {
			defer wg.Done()
			// Reuse existing single-bucket compaction but scoped to rs
			// --- begin single-bucket from original code (trimmed) ---
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
				// propagate builder config
				bloomFPR := s.opts.PrefixBloomFPR
				if bloomFPR <= 0 {
					bloomFPR = common.DefaultBloomFPR
				}
				prefixLen := s.opts.PrefixBloomMaxPrefixLen
				if prefixLen <= 0 {
					prefixLen = int(common.DefaultPrefixBloomLength)
				}
				cur.builder.ConfigureFilters(bloomFPR, prefixLen, s.opts.EnableTrigramFilter)
				cur.builder.ConfigureBuild(s.opts.BuildMaxShards, s.opts.BuildShardMinKeys, s.opts.BloomAdaptiveMinKeys)
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
				reader, rerr := segment.NewReader(cur.id, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad)
				if rerr == nil {
					newReaders = append(newReaders, reader)
				} else {
					s.logger.Warn("failed to open reader for compacted chunk", "id", cur.id, "error", rerr)
				}
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
			// Sources for this bucket only
			type srcIter struct {
				key   []byte
				ok    bool
				next  func() ([]byte, bool)
				order int
			}
			less := func(a, b *srcIter) bool {
				cmp := bytes.Compare(a.key, b.key)
				if cmp != 0 {
					return cmp < 0
				}
				return a.order > b.order
			}
			srcs := make([]*srcIter, 0, len(rs))
			type tsIter struct {
				key  []byte
				ok   bool
				next func() ([]byte, bool)
			}
			tsHeap := make([]*tsIter, 0, len(rs))
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
			// readers
			for idx := len(rs) - 1; idx >= 0; idx-- {
				r := rs[idx]
				advance := r.StreamKeys()
				k, ok := advance()
				srcs = append(srcs, &srcIter{key: k, ok: ok, next: advance, order: idx + 1})
				advT := r.StreamTombstones()
				if tk, tok := advT(); tok {
					pushTS(&tsIter{key: tk, ok: tok, next: advT})
				}
			}
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
				if si.key == nil && si.ok {
					if k, ok := si.next(); ok {
						si.key = k
						si.ok = true
						push(si)
					}
					continue
				}
				emit := false
				if curKey == nil || !bytes.Equal(si.key, curKey) {
					curKey = append([]byte(nil), si.key...)
					emit = true
				}
				if emit {
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
					if len(tsHeap) > 0 && bytes.Equal(tsHeap[0].key, curKey) {
						ti := popTS()
						if nk, ok := ti.next(); ok {
							ti.key = nk
							ti.ok = true
							pushTS(ti)
						}
					} else {
						ensureBuilder()
						if err := cur.builder.Add(curKey, curKey, false); err != nil {
							out <- compRes{nil, fmt.Errorf("builder add: %w", err)}
							return
						}
						cur.approxBytes += int64(len(curKey))
						cur.acceptedCount++
						cur.liveAdded = true
						if err := maybeFlush(); err != nil {
							out <- compRes{nil, err}
							return
						}
					}
				}
				if k, ok := si.next(); ok {
					si.key = k
					si.ok = true
					push(si)
				}
			}
			if err := finalizeChunk(); err != nil {
				out <- compRes{nil, err}
				return
			}
			out <- compRes{readers: newReaders, err: nil}
			// --- end bucket compaction ---
		}(rs)
	}
	go func() { wg.Wait(); close(out) }()

	// Replace readers: if produced new segments, swap in; if none, keep current
	s.mu.Lock()
	var newReadersAll []*segment.Reader
	for r := range out {
		if r.err != nil {
			// Decrement refs before returning
			for _, rr := range readersSnapshot {
				rr.DecRef()
			}
			s.mu.Unlock()
			return r.err
		}
		newReadersAll = append(newReadersAll, r.readers...)
	}
	if len(newReadersAll) > 0 {
		oldReaders := s.readers
		s.readers = newReadersAll
		// Release old readers; faktyczne zamkniecie nastąpi przy refcnt==0
		for _, old := range oldReaders {
			old.Release()
		}
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

// After flush, optionally build filters asynchronously (placeholder)
func (s *storeImpl) maybeScheduleAsyncFilters() {
	if !s.opts.AsyncFilterBuild {
		return
	}
	if s.manifest == nil {
		return
	}
	// Best-effort background job
	go func() {
		s.mu.RLock()
		active := s.manifest.GetActiveSegments()
		dir := filepath.Join(s.dir, common.DirSegments)
		logger := s.logger
		s.mu.RUnlock()
		for _, seg := range active {
			// Open reader and check filters
			reader, err := segment.NewReader(seg.ID, dir, logger, s.opts.VerifyChecksumsOnLoad)
			if err != nil {
				continue
			}
			needBloom := reader.Bloom() == nil
			needTri := s.opts.EnableTrigramFilter && reader.Trigram() == nil
			if !needBloom && !needTri {
				_ = reader.Close()
				continue
			}
			// Rebuild filters from keys stream
			// Load keys via StreamKeys and feed a temporary builder's filter routines
			var keys [][]byte
			adv := reader.StreamKeys()
			for {
				k, ok := adv()
				if !ok {
					break
				}
				kk := make([]byte, len(k))
				copy(kk, k)
				keys = append(keys, kk)
			}
			_ = reader.Close()
			b := segment.NewBuilder(seg.ID, common.LevelL0, dir, logger)
			// Only configure filters; skip values and LOUDS
			bloomFPR := s.opts.PrefixBloomFPR
			if bloomFPR <= 0 {
				bloomFPR = common.DefaultBloomFPR
			}
			prefixLen := s.opts.PrefixBloomMaxPrefixLen
			if prefixLen <= 0 {
				prefixLen = int(common.DefaultPrefixBloomLength)
			}
			b.ConfigureFilters(bloomFPR, prefixLen, s.opts.EnableTrigramFilter)
			// Inject keys and build only filters files
			// Hack: populate b.keys, then call internal filter build
			// Safer: reuse buildFilters API after setting keys
			// We do minimal approach: export keys via AddFromPairs
			tombs := make([]bool, len(keys))
			if err := b.AddFromPairs(keys, tombs); err != nil {
				continue
			}
			filtersDir := filepath.Join(dir, fmt.Sprintf("%016d", seg.ID), "filters")
			_ = os.MkdirAll(filtersDir, 0755)
			// Build requested filters
			if needBloom {
				_ = b.BuildBloomOnly(filtersDir)
			}
			if needTri && s.opts.EnableTrigramFilter {
				_ = b.BuildTrigramOnly(filtersDir)
			}
		}
	}()
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

	start := time.Now()

	// Prevent concurrent Flush (best-effort) and serialize with in-flight writes
	s.mu.Lock()
	// Block writers from swapping memtable while we swap by taking write lock on mtGuard
	s.mtGuard.Lock()
	oldMem := s.memtablePtr.Swap(memtable.New())
	if oldMem == nil {
		s.mtGuard.Unlock()
		s.mu.Unlock()
		return nil // Nothing to flush
	}
	oldCount := oldMem.Count()
	oldDeleted := oldMem.DeletedCount()
	if oldCount == 0 && oldDeleted == 0 {
		s.mtGuard.Unlock()
		s.mu.Unlock()
		return nil // Nothing to flush
	}
	// Reserve a new base segment ID and cache dir while under lock
	baseID := atomic.AddUint64(&s.nextSegmentID, 1)
	segmentsDir := filepath.Join(s.dir, common.DirSegments)
	s.mtGuard.Unlock()
	s.mu.Unlock()

	s.logger.Info("starting flush", "entries", oldCount)

	// Snapshot the frozen memtable after swap
	snapshot := oldMem.Snapshot()

	// Partition count
	parts := s.opts.BuildRangePartitions
	if parts <= 1 {
		parts = 1
	}

	// Helper to configure a builder
	cfgBuilder := func(b *segment.Builder) {
		bloomFPR := s.opts.PrefixBloomFPR
		if bloomFPR <= 0 {
			bloomFPR = common.DefaultBloomFPR
		}
		prefixLen := s.opts.PrefixBloomMaxPrefixLen
		if prefixLen <= 0 {
			prefixLen = int(common.DefaultPrefixBloomLength)
		}
		enableTri := s.opts.EnableTrigramFilter
		b.ConfigureFilters(bloomFPR, prefixLen, enableTri)
		b.ConfigureBuild(s.opts.BuildMaxShards, s.opts.BuildShardMinKeys, s.opts.BloomAdaptiveMinKeys)
		// If AsyncFilterBuild is enabled, skip inline filter generation to shorten Flush wall-clock
		if s.opts.AsyncFilterBuild {
			b.SetSkipFilters(true)
		}
	}

	type partResult struct {
		id  uint64
		md  *segment.Metadata
		err error
	}

	results := make(chan partResult, parts)

	if parts == 1 {
		// Single builder path (existing)
		segID := baseID
		builder := segment.NewBuilder(segID, common.LevelL0, segmentsDir, s.logger)
		cfgBuilder(builder)
		if err := builder.AddFromMemtable(snapshot); err != nil {
			// Rollback: reinsert snapshot into current memtable to avoid data loss
			s.rollbackSnapshotIntoMemtable(snapshot)
			return fmt.Errorf("add memtable to builder: %w", err)
		}
		md, err := builder.Build()
		if err != nil {
			s.rollbackSnapshotIntoMemtable(snapshot)
			return fmt.Errorf("build segment: %w", err)
		}
		results <- partResult{id: segID, md: md, err: nil}
		close(results)
	} else {
		// Range-partition snapshot by first byte (0..255). If it collapses to <=1 non-empty bucket,
		// fallback to hash-based partitioning to ensure parallel build even for heavy shared prefixes.
		keys, tombs := snapshot.GetAllWithTombstones()
		buckets := make([][][]byte, parts)
		btombs := make([][]bool, parts)
		for i := range buckets {
			buckets[i] = make([][]byte, 0, len(keys)/parts+1)
			btombs[i] = make([]bool, 0, len(keys)/parts+1)
		}
		for i, k := range keys {
			idx := 0
			if len(k) > 0 {
				idx = int(k[0]) * parts / 256
				if idx >= parts {
					idx = parts - 1
				}
			}
			buckets[idx] = append(buckets[idx], k)
			btombs[idx] = append(btombs[idx], tombs[i])
		}
		// count non-empty
		nonEmpty := 0
		for p := 0; p < parts; p++ {
			if len(buckets[p]) > 0 {
				nonEmpty++
			}
		}
		if parts > 1 && nonEmpty <= 1 {
			// fallback: hash partition all keys evenly
			for i := range buckets {
				buckets[i] = buckets[i][:0]
				btombs[i] = btombs[i][:0]
			}
			for i, k := range keys {
				h := fnv32(k)
				idx := int(h % uint32(parts))
				buckets[idx] = append(buckets[idx], k)
				btombs[idx] = append(btombs[idx], tombs[i])
			}
			s.logger.Info("flush using hash-based partitioning", "parts", parts)
		}
		var wg sync.WaitGroup
		for p := 0; p < parts; p++ {
			if len(buckets[p]) == 0 {
				continue
			}
			wg.Add(1)
			go func(pid int) {
				defer wg.Done()
				segID := atomic.AddUint64(&s.nextSegmentID, 1)
				builder := segment.NewBuilder(segID, common.LevelL0, segmentsDir, s.logger)
				cfgBuilder(builder)
				if err := builder.AddFromPairs(buckets[pid], btombs[pid]); err != nil {
					results <- partResult{id: segID, err: fmt.Errorf("add partition: %w", err)}
					return
				}
				md, err := builder.Build()
				if err != nil {
					results <- partResult{id: segID, err: fmt.Errorf("build partition: %w", err)}
					return
				}
				results <- partResult{id: segID, md: md, err: nil}
			}(p)
		}
		go func() { wg.Wait(); close(results) }()
	}

	// Collect results
	var mds []*segment.Metadata
	for r := range results {
		if r.err != nil {
			s.rollbackSnapshotIntoMemtable(snapshot)
			return r.err
		}
		mds = append(mds, r.md)
	}

	// Update in-memory state and manifest under lock
	s.mu.Lock()
	for _, md := range mds {
		s.segments = append(s.segments, md)
		reader, rerr := segment.NewReader(md.SegmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad)
		if rerr == nil {
			s.readers = append(s.readers, reader)
		} else {
			s.logger.Warn("failed to open reader for new segment", "id", md.SegmentID, "error", rerr)
		}
		if s.manifest != nil {
			info := manifest.SegmentInfo{ID: md.SegmentID, Level: common.LevelL0, NumKeys: md.Counts.Accepted, Size: s.computeSegmentSize(md.SegmentID)}
			if minb, err := md.GetMinKey(); err == nil {
				info.MinKeyHex = fmt.Sprintf("%x", minb)
			}
			if maxb, err := md.GetMaxKey(); err == nil {
				info.MaxKeyHex = fmt.Sprintf("%x", maxb)
			}
			if err := s.manifest.AddSegment(info); err != nil {
				s.logger.Warn("failed to add segment to manifest", "error", err)
			}
		}
	}
	// Refresh stats after flush
	s.updateStatsFromManifest()
	s.mu.Unlock()

	// Sync WAL after persisting segments
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}

	// Rotate WAL to start a fresh file after flush
	if s.wal != nil && s.opts.RotateWALOnFlush {
		if err := s.wal.Rotate(); err != nil {
			s.logger.Warn("failed to rotate WAL after flush", "error", err)
		}
	}

	s.lastFlushTime = time.Now()
	s.stats.RecordFlush(time.Since(start))
	s.logger.Info("flush completed", "duration", time.Since(start), "segments", len(mds))

	// Optionally schedule async filter building for missing filters
	s.maybeScheduleAsyncFilters()

	return nil
}

// rollbackSnapshotIntoMemtable reinserts snapshot data back into the active memtable on flush failure.
func (s *storeImpl) rollbackSnapshotIntoMemtable(snapshot *memtable.Snapshot) {
	if snapshot == nil {
		return
	}
	// Best-effort reinsertion (no WAL writes; WAL already contains original ops)
	keys, tombs := snapshot.GetAllWithTombstones()
	for i := range keys {
		if mt := s.memtablePtr.Load(); mt != nil {
			if tombs[i] {
				_ = mt.Delete(keys[i])
			} else {
				_ = mt.InsertWithTTL(keys[i], 0)
			}
		}
	}
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
	err := s.wal.Replay(func(op uint8, key []byte, expiresAt time.Time) error {
		switch op {
		case common.OpInsert:
			if expiresAt.IsZero() || time.Now().Before(expiresAt) {
				if mt := s.memtablePtr.Load(); mt != nil {
					if expiresAt.IsZero() {
						if err := mt.InsertWithTTL(key, 0); err != nil {
							return err
						}
					} else {
						if err := mt.InsertWithExpiry(key, expiresAt); err != nil {
							return err
						}
					}
				} else {
					return fmt.Errorf("memtable not initialized")
				}
			}
		case common.OpDelete:
			if mt := s.memtablePtr.Load(); mt != nil {
				if err := mt.Delete(key); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("memtable not initialized")
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
	if mt := s.memtablePtr.Load(); mt != nil {
		return mt.Size() >= s.opts.MemtableTargetBytes
	}
	return false
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
				segPath := filepath.Join(segmentsDir, e.Name())
				// Skip in-progress builds marked by sentinel
				if _, err := os.Stat(filepath.Join(segPath, ".building")); err == nil {
					continue
				}
				// Skip directories that do not yet have metadata (incomplete build)
				if _, err := os.Stat(filepath.Join(segPath, "segment.json")); err != nil {
					continue
				}
				var segID uint64
				if _, err := fmt.Sscanf(e.Name(), "%d", &segID); err != nil {
					continue
				}
				if _, ok := activeIDs[segID]; ok {
					continue
				}
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
		if _, err := fmt.Sscanf(entry.Name(), "%d", &segmentID); err != nil {
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
