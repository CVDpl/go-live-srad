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

// writeRequest is a request to the dedicated WAL writer goroutine.
type writeRequest struct {
	entry *wal.WALEntry
	err   chan error
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

	// WAL writer
	writeCh chan *writeRequest
	walStop chan struct{}
	walWg   sync.WaitGroup

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

	// Frozen memtable snapshots visible to readers during flush
	frozenSnaps []*memtable.Snapshot

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

	// Create directories only when not in read-only mode
	if !opts.ReadOnly {
		if err := createStoreDirectories(dir); err != nil {
			return nil, fmt.Errorf("create store directories: %w", err)
		}
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
		s.writeCh = make(chan *writeRequest, 128) // Buffer for performance
	}

	// Initialize memtable
	s.memtablePtr.Store(memtable.New())

	// Replay WAL to restore memtable
	if s.wal != nil {
		if err := s.replayWAL(); err != nil {
			s.Close()
			return nil, fmt.Errorf("replay WAL: %w", err)
		}
	} else if s.readonly {
		// Read-only: best-effort WAL replay without creating/rotating files
		walDir := filepath.Join(dir, common.DirWAL)
		if err := wal.ReplayDir(walDir, s.logger, func(op uint8, key []byte, expiresAt time.Time) error {
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
				s.logger.Warn("unknown operation in WAL (RO)", "op", op)
			}
			return nil
		}); err != nil {
			s.logger.Warn("failed to replay WAL in read-only mode", "error", err)
		}
	}

	// Initialize manifest (read-only avoids creating files)
	var manifestErr error
	if s.readonly {
		s.manifest, manifestErr = manifest.OpenReadOnly(dir, s.logger)
	} else {
		s.manifest, manifestErr = manifest.New(dir, s.logger)
	}
	if manifestErr != nil {
		s.logger.Warn("failed to open manifest", "readonly", s.readonly, "error", manifestErr)
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
		// Ensure compaction builders are configured consistently with flush
		cfg := func(b *segment.Builder) {
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
			if s.opts.AsyncFilterBuild {
				b.SetSkipFilters(true)
			}
			if s.opts.GCPercentDuringTrie != 0 {
				b.SetTrieGCPercent(s.opts.GCPercentDuringTrie)
			}
			b.SetDisableLOUDS(s.opts.DisableLOUDSBuild)
			if s.opts.AutoDisableLOUDSMinKeys > 0 {
				b.SetAutoDisableLOUDSThreshold(s.opts.AutoDisableLOUDSMinKeys)
			}
			if s.opts.ForceTrieBuild {
				b.SetForceTrie(true)
			}
		}
		s.compactor.SetBuilderConfigurator(cfg)
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

	// Block new/in-flight writes from reaching WAL while shutting it down.
	// Ensures no goroutine can send on writeCh after it is closed by walWriter.
	s.mtGuard.Lock()
	// Stop background tasks, including WAL writer (waits for WAL goroutine to exit)
	s.stopBackgroundTasks()
	s.mtGuard.Unlock()

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

	entry := &wal.WALEntry{
		Op:  common.OpInsert,
		Key: key,
		TTL: ttl,
	}

	req := &writeRequest{
		entry: entry,
		err:   make(chan error, 1),
	}

	// Use write lock to prevent flush during insert and WAL write
	s.mtGuard.Lock()
	mt := s.memtablePtr.Load()
	if mt == nil {
		s.mtGuard.Unlock()
		return fmt.Errorf("memtable not initialized")
	}
	err := mt.InsertWithTTL(key, ttl)
	if err != nil {
		s.mtGuard.Unlock()
		return fmt.Errorf("insert to memtable: %w", err)
	}
	// Send to WAL while holding lock
	s.writeCh <- req
	// Wait for confirmation while holding lock
	if err := <-req.err; err != nil {
		// Rollback
		mt.Delete(key)
		s.mtGuard.Unlock()
		return fmt.Errorf("write to WAL: %w", err)
	}
	s.mtGuard.Unlock()

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

	entry := &wal.WALEntry{
		Op:  common.OpDelete,
		Key: key,
	}

	req := &writeRequest{
		entry: entry,
		err:   make(chan error, 1),
	}

	// Use write lock to prevent flush during delete and WAL write
	s.mtGuard.Lock()
	mt := s.memtablePtr.Load()
	if mt == nil {
		s.mtGuard.Unlock()
		return fmt.Errorf("memtable not initialized")
	}
	err := mt.Delete(key)
	if err != nil {
		s.mtGuard.Unlock()
		return fmt.Errorf("delete from memtable: %w", err)
	}
	// Send to WAL while holding lock
	s.writeCh <- req
	// Wait for confirmation while holding lock
	if err := <-req.err; err != nil {
		s.mtGuard.Unlock()
		return fmt.Errorf("write to WAL: %w", err)
	}
	s.mtGuard.Unlock()

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

	// Snapshot readers and frozenSnaps under short RLock, then release
	s.mu.RLock()
	readersSnap := append([]*segment.Reader(nil), s.readers...)
	frozenSnapList := append([]*memtable.Snapshot(nil), s.frozenSnaps...)
	s.mu.RUnlock()

	// Use a snapshot to avoid racing with concurrent writers
	var memIter *memtable.Iterator
	var memSnap *memtable.Snapshot
	if mt := s.memtablePtr.Load(); mt != nil {
		memSnap = mt.Snapshot()
		memIter = memSnap.RegexSearch(re)
	}

	// Build iterators for frozen snapshots
	frozenIters := make([]*memtable.Iterator, 0, len(frozenSnapList))
	for _, fs := range frozenSnapList {
		if fs == nil {
			continue
		}
		frozenIters = append(frozenIters, fs.RegexSearch(re))
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

	// Exact-match fast path: anchored literal ^...$ and small limit
	exactLiteral := []byte(nil)
	if re != nil {
		pat := re.String()
		if len(pat) >= 3 && pat[0] == '^' && pat[len(pat)-1] == '$' {
			if lit, _ := re.LiteralPrefix(); lit != "" {
				exactLiteral = []byte(lit)
			}
		}
	}
	// Prefer exact path when user requests few results
	if exactLiteral != nil && (q.Limit <= 1 || q.Mode == CountOnly) {
		// proceed with exact path below
	} else if exactLiteral != nil {
		// keep exactLiteral but we may still choose streaming later; no-op
	}

	// If we have an exact literal and no segments, resolve from memtable snapshots only
	if exactLiteral != nil && len(readersSnap) == 0 {
		items := make([]segItem, 0, 2)
		if memSnap != nil {
			if v := memSnap.GetValue(exactLiteral); v != nil && !v.IsExpired() {
				op := common.OpInsert
				if v.Tombstone() {
					op = common.OpDelete
				}
				items = append(items, segItem{id: 0, key: append([]byte(nil), exactLiteral...), op: op})
			}
		}
		for _, fs := range frozenSnapList {
			if fs == nil {
				continue
			}
			if v := fs.GetValue(exactLiteral); v != nil && !v.IsExpired() {
				op := common.OpInsert
				if v.Tombstone() {
					op = common.OpDelete
				}
				items = append(items, segItem{id: 0, key: append([]byte(nil), exactLiteral...), op: op})
			}
		}
		return &listIterator{items: items, idx: 0, mode: q.Mode, limit: q.Limit}, nil
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
	// If no segments, return simple iterator but include frozen snapshots
	if len(readersSnap) == 0 {
		return &simpleIterator{
			memIter:     memIter,
			frozenIters: frozenIters,
			mode:        q.Mode,
			limit:       q.Limit,
			count:       0,
		}, nil
	}

	// Early exact-match path: try memtable + direct segment Get without streaming
	if exactLiteral != nil {
		items := make([]segItem, 0, 4)
		// from memtable snapshot
		if memSnap != nil {
			v := memSnap.GetValue(exactLiteral)
			if v != nil && !v.IsExpired() {
				op := common.OpInsert
				if v.Tombstone() {
					op = common.OpDelete
				}
				items = append(items, segItem{id: 0, key: append([]byte(nil), exactLiteral...), op: op})
			}
		}
		// from frozen snapshots
		for _, fs := range frozenSnapList {
			if fs == nil {
				continue
			}
			v := fs.GetValue(exactLiteral)
			if v != nil && !v.IsExpired() {
				op := common.OpInsert
				if v.Tombstone() {
					op = common.OpDelete
				}
				items = append(items, segItem{id: 0, key: append([]byte(nil), exactLiteral...), op: op})
			}
		}
		// query segments newest-first and stop early when limit reached
		selected := append([]*segment.Reader(nil), readersSnap...)
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
		for _, r := range selected {
			if ctx.Err() != nil {
				break
			}
			if re != nil && !r.MayContain(re) {
				continue
			}
			r.IncRef()
			val, ok := r.Get(exactLiteral)
			if ok && len(val) > 0 {
				// filter out tombstone quickly
				if r.HasTombstoneExact(ctx, exactLiteral) {
					r.Release()
					continue
				}
				items = append(items, segItem{id: r.GetSegmentID(), key: append([]byte(nil), exactLiteral...), op: common.OpInsert})
			}
			r.Release()
			if q.Limit > 0 && len(items) >= q.Limit {
				break
			}
		}
		return &listIterator{items: items, idx: 0, mode: q.Mode, limit: q.Limit}, nil
	}

	// Choose relevant readers and order newest-first
	selected := s.selectRelevantReaders(readersSnap, re, q)
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
		selected = append([]*segment.Reader(nil), readersSnap...)
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
	// Create a cancellable context for the background producer
	pctx, cancel := context.WithCancel(ctx)
	go func(readers []*segment.Reader) {
		defer close(out)
		// For broad scans, emit all memtable keys first via channel
		if broad && memSnap != nil {
			all := memSnap.GetAll()
			emitted := 0
			for _, k := range all {
				select {
				case <-pctx.Done():
					return
				default:
				}
				if re != nil && !re.Match(k) {
					continue
				}
				select {
				case out <- segItem{id: 0, key: append([]byte(nil), k...), op: common.OpInsert}:
				case <-pctx.Done():
					return
				}
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
			// preload tombstones only when not an exact-match fast path
			if exactLiteral == nil {
				_, _ = r.IterateTombstones(func(tk []byte) bool {
					seenMu.Lock()
					seen[string(tk)] = struct{}{}
					seenMu.Unlock()
					return true
				})
			}
			emitted := 0
			adv, closeFn := r.StreamKeys()
			for {
				select {
				case <-pctx.Done():
					closeFn()
					r.Release()
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
				// Exact fast path: skip send if tombstoned in this reader
				if exactLiteral != nil {
					if r.HasTombstoneExact(pctx, exactLiteral) {
						continue
					}
				}
				select {
				case out <- segItem{id: r.GetSegmentID(), key: append([]byte(nil), k...), op: common.OpInsert}:
				case <-pctx.Done():
					closeFn()
					r.Release()
					return
				}
				emitted++
			}
			closeFn()
			r.Release()
			s.logger.Info("broad scan emitted segment keys", "segment", r.GetSegmentID(), "count", emitted)
		}
	}(selected)

	// Drain into merged iterator (memtable first)
	return &parallelMergedIterator{
		memIter:     memIter,
		frozenIters: frozenIters,
		in:          out,
		seen:        seen,
		seenMu:      seenMu,
		mode:        q.Mode,
		limit:       q.Limit,
		cancel:      cancel,
	}, nil
}

// selectRelevantReaders picks a prioritized subset of readers to scan without building global indexes.
func (s *storeImpl) selectRelevantReaders(readers []*segment.Reader, re *regexp.Regexp, q *QueryOptions) []*segment.Reader {
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
	memIter     *memtable.Iterator
	frozenIters []*memtable.Iterator
	in          chan segItem
	seen        map[string]struct{}
	seenMu      *sync.Mutex
	mode        QueryMode
	limit       int
	count       int
	cur         struct {
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
	// then drain frozen snapshots (one by one)
	for len(it.frozenIters) > 0 {
		cur := it.frozenIters[0]
		if cur == nil {
			it.frozenIters = it.frozenIters[1:]
			continue
		}
		if cur.Next(ctx) {
			k := cur.Key()
			ks := string(k)
			it.seenMu.Lock()
			if _, ok := it.seen[ks]; ok {
				it.seenMu.Unlock()
				continue
			}
			it.seen[ks] = struct{}{}
			it.seenMu.Unlock()
			it.cur.key = k
			it.cur.id = cur.SeqNum()
			if cur.IsTombstone() {
				it.cur.op = common.OpDelete
			} else {
				it.cur.op = common.OpInsert
			}
			it.count++
			return true
		}
		_ = cur.Close()
		it.frozenIters = it.frozenIters[1:]
	}
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

// listIterator is a simple iterator over a prepared list of segItems.
type listIterator struct {
	items []segItem
	idx   int
	mode  QueryMode
	limit int
	cur   segItem
	err   error
}

func (it *listIterator) Next(ctx context.Context) bool {
	if it.limit > 0 && it.idx >= it.limit {
		return false
	}
	if it.idx >= len(it.items) {
		return false
	}
	// respect cancellation but allow quick exit
	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		return false
	default:
	}
	it.cur = it.items[it.idx]
	it.idx++
	return true
}

func (it *listIterator) Err() error { return it.err }
func (it *listIterator) ID() uint64 { return it.cur.id }
func (it *listIterator) String() []byte {
	if it.mode == CountOnly {
		return nil
	}
	return it.cur.key
}
func (it *listIterator) Op() uint8    { return it.cur.op }
func (it *listIterator) Close() error { return nil }

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

	s.mu.Lock()
	if s.compactor == nil {
		s.mu.Unlock()
		return fmt.Errorf("compactor not initialized")
	}

	plan := s.compactor.Plan()
	if plan == nil {
		s.mu.Unlock()
		s.logger.Info("compaction triggered manually, but no work to be done")
		return nil
	}
	s.mu.Unlock()

	// Execute compaction plan
	newReaders, err := s.compactor.Execute(plan)
	if err != nil {
		return fmt.Errorf("failed to execute compaction plan: %w", err)
	}

	// Update store's readers
	s.mu.Lock()
	defer s.mu.Unlock()

	// This logic needs to be robust. We should replace the old readers with the new ones.
	// First, build a map of old segment IDs that were part of the compaction.
	compactedIDs := make(map[uint64]struct{})
	for _, s := range plan.Inputs {
		compactedIDs[s.ID] = struct{}{}
	}
	for _, s := range plan.Overlaps {
		compactedIDs[s.ID] = struct{}{}
	}

	// Create a new list of readers, keeping the ones not compacted and adding the new ones.
	var updatedReaders []*segment.Reader
	for _, r := range s.readers {
		if _, found := compactedIDs[r.GetSegmentID()]; !found {
			updatedReaders = append(updatedReaders, r)
		} else {
			// This reader's segment was compacted, release it.
			r.Release()
		}
	}
	updatedReaders = append(updatedReaders, newReaders...)
	s.readers = updatedReaders

	// Refresh stats
	s.updateStatsFromManifest()

	// If filters are built asynchronously, schedule building for new segments now
	s.maybeScheduleAsyncFilters()

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
			adv, closeFn := reader.StreamKeys()
			for {
				k, ok := adv()
				if !ok {
					break
				}
				kk := make([]byte, len(k))
				copy(kk, k)
				keys = append(keys, kk)
			}
			closeFn()
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
			// Inject keys and build filters using public API
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

// RebuildMissingFilters synchronously rebuilds missing Bloom/Trigram filters for active segments.
func (s *storeImpl) RebuildMissingFilters(ctx context.Context) error {
	if s.manifest == nil {
		return nil
	}
	s.mu.RLock()
	active := s.manifest.GetActiveSegments()
	dir := filepath.Join(s.dir, common.DirSegments)
	logger := s.logger
	enableTri := s.opts.EnableTrigramFilter
	bloomFPR := s.opts.PrefixBloomFPR
	if bloomFPR <= 0 {
		bloomFPR = common.DefaultBloomFPR
	}
	prefixLen := s.opts.PrefixBloomMaxPrefixLen
	if prefixLen <= 0 {
		prefixLen = int(common.DefaultPrefixBloomLength)
	}
	s.mu.RUnlock()

	for _, seg := range active {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		reader, err := segment.NewReader(seg.ID, dir, logger, s.opts.VerifyChecksumsOnLoad)
		if err != nil {
			continue
		}
		needBloom := reader.Bloom() == nil
		needTri := enableTri && reader.Trigram() == nil
		if !needBloom && !needTri {
			_ = reader.Close()
			continue
		}
		// Stream keys
		var keys [][]byte
		adv, closeFn := reader.StreamKeys()
		for {
			k, ok := adv()
			if !ok {
				break
			}
			kk := make([]byte, len(k))
			copy(kk, k)
			keys = append(keys, kk)
		}
		closeFn()
		_ = reader.Close()

		// Build requested filters
		b := segment.NewBuilder(seg.ID, common.LevelL0, dir, logger)
		b.ConfigureFilters(bloomFPR, prefixLen, enableTri)
		tombs := make([]bool, len(keys))
		if err := b.AddFromPairs(keys, tombs); err != nil {
			continue
		}
		filtersDir := filepath.Join(dir, fmt.Sprintf("%016d", seg.ID), "filters")
		if err := os.MkdirAll(filtersDir, 0755); err != nil {
			continue
		}
		if needBloom {
			_ = b.BuildBloomOnly(filtersDir)
		}
		if needTri {
			_ = b.BuildTrigramOnly(filtersDir)
		}
	}
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

// PurgeObsoleteSegments removes non-active segment directories immediately.
// WARNING: This bypasses the RCU grace period and should only be used when
// you are sure no readers are referencing old segments.
func (s *storeImpl) PurgeObsoleteSegments() error {
	if s.manifest == nil {
		return fmt.Errorf("manifest not initialized")
	}
	active := s.manifest.GetActiveSegments()
	activeMap := make(map[string]struct{}, len(active))
	for _, seg := range active {
		activeMap[fmt.Sprintf("%016d", seg.ID)] = struct{}{}
	}
	segmentsDir := filepath.Join(s.dir, common.DirSegments)
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if _, ok := activeMap[name]; ok {
			continue
		}
		// Skip in-progress builds
		if _, err := os.Stat(filepath.Join(segmentsDir, name, ".building")); err == nil {
			continue
		}
		// Skip directories without metadata (incomplete)
		if _, err := os.Stat(filepath.Join(segmentsDir, name, "segment.json")); err != nil {
			continue
		}
		_ = os.RemoveAll(filepath.Join(segmentsDir, name))
		s.logger.Info("purged obsolete segment dir", "name", name)
	}
	return nil
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

	s.logger.Info("starting flush", "entries", oldCount)

	// Snapshot the frozen memtable after swap and publish under lock
	snapshot := oldMem.Snapshot()
	s.frozenSnaps = append(s.frozenSnaps, snapshot)
	// Now release store lock
	s.mu.Unlock()

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
		// Apply optional GC policy for trie build
		if s.opts.GCPercentDuringTrie != 0 {
			b.SetTrieGCPercent(s.opts.GCPercentDuringTrie)
		}
		// LOUDS build controls
		b.SetDisableLOUDS(s.opts.DisableLOUDSBuild)
		if s.opts.AutoDisableLOUDSMinKeys > 0 {
			b.SetAutoDisableLOUDSThreshold(s.opts.AutoDisableLOUDSMinKeys)
		}
		// Force building trie even for sorted inputs
		if s.opts.ForceTrieBuild {
			b.SetForceTrie(true)
		}
	}

	type partResult struct {
		id  uint64
		md  *segment.Metadata
		err error
	}

	results := make(chan partResult, parts)

	flushPlanStart := time.Now()

	if parts == 1 {
		// Single builder path (existing)
		segID := baseID
		builder := segment.NewBuilder(segID, common.LevelL0, segmentsDir, s.logger)
		cfgBuilder(builder)
		s.logger.Info("flush plan: single-partition prepared", "segment_id", segID, "entries", oldCount)
		if err := builder.AddFromMemtable(snapshot); err != nil {
			// Rollback: reinsert snapshot into current memtable to avoid data loss
			s.rollbackSnapshotIntoMemtable(snapshot)
			return fmt.Errorf("add memtable to builder: %w", err)
		}
		addDone := time.Now()
		s.logger.Info("flush stage: builder input added", "segment_id", segID, "duration", addDone.Sub(flushPlanStart))
		buildStart := time.Now()
		md, err := builder.Build()
		if err != nil {
			s.rollbackSnapshotIntoMemtable(snapshot)
			return fmt.Errorf("build segment: %w", err)
		}
		s.logger.Info("flush stage: builder.Build completed", "segment_id", segID, "duration", time.Since(buildStart))
		results <- partResult{id: segID, md: md, err: nil}
		close(results)
	} else {
		// Range-partition snapshot by first byte (0..255). If it collapses to <=1 non-empty bucket,
		// fallback to hash-based partitioning to ensure parallel build even for heavy shared prefixes.
		partPrepStart := time.Now()
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
		s.logger.Info("flush stage: partitioning done", "parts", parts, "non_empty", nonEmpty, "duration", time.Since(partPrepStart), "total_keys", len(keys))
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
				partStart := time.Now()
				if err := builder.AddFromPairs(buckets[pid], btombs[pid]); err != nil {
					results <- partResult{id: segID, err: fmt.Errorf("add partition: %w", err)}
					return
				}
				s.logger.Info("flush stage: partition input added", "partition", pid, "segment_id", segID, "keys", len(buckets[pid]), "duration", time.Since(partStart))
				buildStart := time.Now()
				md, err := builder.Build()
				if err != nil {
					results <- partResult{id: segID, err: fmt.Errorf("build partition: %w", err)}
					return
				}
				s.logger.Info("flush stage: partition built", "partition", pid, "segment_id", segID, "duration", time.Since(buildStart))
				results <- partResult{id: segID, md: md, err: nil}
			}(p)
		}
		go func() { wg.Wait(); close(results) }()
	}

	// Collect results
	var mds []*segment.Metadata
	collectStart := time.Now()
	for r := range results {
		if r.err != nil {
			s.rollbackSnapshotIntoMemtable(snapshot)
			return r.err
		}
		mds = append(mds, r.md)
	}
	s.logger.Info("flush stage: collected results", "segments", len(mds), "duration", time.Since(collectStart))

	// Update in-memory state and manifest under lock
	applyStart := time.Now()
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
	s.logger.Info("flush stage: applied manifest and readers", "segments", len(mds), "duration", time.Since(applyStart))

	// Remove frozen snapshot from visibility once flush completes
	s.mu.Lock()
	if len(s.frozenSnaps) > 0 {
		// remove the first matching snapshot reference
		idx := -1
		for i, sn := range s.frozenSnaps {
			if sn == snapshot {
				idx = i
				break
			}
		}
		if idx >= 0 {
			last := len(s.frozenSnaps) - 1
			s.frozenSnaps[idx] = s.frozenSnaps[last]
			s.frozenSnaps = s.frozenSnaps[:last]
		}
	}
	s.mu.Unlock()

	// Sync WAL after persisting segments
	walSyncStart := time.Now()
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	s.logger.Info("flush stage: WAL synced", "duration", time.Since(walSyncStart))

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

	// Opportunistically trigger one cleanup pass to remove obsolete dirs
	go s.rcuCleanupOnce()

	return nil
}

// rcuCleanupOnce runs a single RCU cleanup pass (same logic as background task)
func (s *storeImpl) rcuCleanupOnce() {
	if s.manifest == nil {
		return
	}
	active := s.manifest.GetActiveSegments()
	activeIDs := make(map[uint64]struct{}, len(active))
	for _, seg := range active {
		activeIDs[seg.ID] = struct{}{}
	}
	segmentsDir := filepath.Join(s.dir, common.DirSegments)
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-time.Duration(common.RCUGracePeriod) * time.Second).Unix()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		segPath := filepath.Join(segmentsDir, e.Name())
		if _, err := os.Stat(filepath.Join(segPath, ".building")); err == nil {
			continue
		}
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
		if info, err := os.Stat(segPath); err == nil {
			if info.ModTime().Unix() < cutoff {
				_ = os.RemoveAll(segPath)
				s.logger.Info("rcu removed obsolete segment dir", "id", segID)
			}
		}
	}
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

// walWriter is a dedicated goroutine for writing to the WAL.
// It batches writes together ("group commit") to improve performance.
func (s *storeImpl) walWriter() {
	defer s.walWg.Done()
	requests := make([]*writeRequest, 0, 128)
	entries := make([]*wal.WALEntry, 0, 128)

	for {
		var firstReq *writeRequest

		select {
		case <-s.walStop:
			// Drain any remaining requests on shutdown
			close(s.writeCh)
			for req := range s.writeCh {
				requests = append(requests, req)
			}
			if len(requests) > 0 {
				s.flushWALRequests(&requests, &entries)
			}
			return
		case firstReq = <-s.writeCh:
			requests = append(requests, firstReq)
		}

		// Batching: try to collect more requests that are already waiting
	collectMore:
		for len(requests) < cap(requests) {
			select {
			case req := <-s.writeCh:
				requests = append(requests, req)
			default:
				// No more requests waiting
				break collectMore
			}
		}

		s.flushWALRequests(&requests, &entries)
	}
}

func (s *storeImpl) flushWALRequests(requests *[]*writeRequest, entries *[]*wal.WALEntry) {
	if len(*requests) == 0 {
		return
	}

	// Prepare entries for WriteBatch
	for _, req := range *requests {
		*entries = append(*entries, req.entry)
	}

	err := s.wal.WriteBatch(*entries)

	// Notify all requesters
	for _, req := range *requests {
		req.err <- err
	}

	// Reset slices for next batch
	*requests = (*requests)[:0]
	*entries = (*entries)[:0]
}

// startBackgroundTasks starts all background tasks.
func (s *storeImpl) startBackgroundTasks() {
	// WAL writer task (start first)
	s.walStop = make(chan struct{})
	s.walWg.Add(1)
	go s.walWriter()

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
	// Stop WAL writer first to ensure all pending writes are flushed before closing WAL.
	if s.walStop != nil {
		close(s.walStop)
	}
	s.walWg.Wait()

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

	ticker := time.NewTicker(10 * time.Second) // Check for compaction work every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.compactStop:
			return
		case <-ticker.C:
			// Planning is done without any store-level lock.
			// It relies on the manifest's internal lock for a consistent view.
			plan := s.compactor.Plan()

			if plan == nil {
				continue // No work to do
			}

			// The execution is the long-running part, also without a store-level lock.
			// It will atomically update the manifest upon completion.
			_, err := s.compactor.Execute(plan)
			if err != nil {
				s.logger.Error("background compaction failed", "error", err)
				continue
			}

			// After a successful compaction, the source of truth (the manifest) has changed.
			// The safest and most robust way to update the store's in-memory state
			// is to reload all segment readers from the manifest. This prevents race conditions
			// where another operation (like a flush) could have changed the segment list
			// while compaction was running.
			s.mu.Lock()
			s.logger.Info("compaction finished, reloading segment readers to reflect changes")

			// Release all old readers. They will be closed once no search is using them.
			for _, r := range s.readers {
				r.Release()
			}

			// Clear and reload from the manifest's source of truth.
			s.readers = nil
			s.segments = nil
			if err := s.loadSegments(); err != nil {
				s.logger.Error("failed to reload segments after compaction", "error", err)
			}
			s.updateStatsFromManifest()
			s.mu.Unlock()
		}
	}
}

// autotuneTask is the background autotuning task.
func (s *storeImpl) autotuneTask() {
	defer s.bgWg.Done()

	ticker := time.NewTicker(10 * time.Second) // Check for compaction work every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.autotuneStop:
			return
		case <-ticker.C:
			s.mu.RLock()
			if s.compactor == nil {
				s.mu.RUnlock()
				continue
			}
			plan := s.compactor.Plan()
			s.mu.RUnlock()

			if plan == nil {
				continue // No work to do
			}

			newReaders, err := s.compactor.Execute(plan)
			if err != nil {
				s.logger.Error("background compaction failed", "error", err)
				continue
			}
			if newReaders == nil {
				continue // Compaction resulted in no new segments
			}

			// Update store's readers state
			s.mu.Lock()
			compactedIDs := make(map[uint64]struct{})
			for _, s := range plan.Inputs {
				compactedIDs[s.ID] = struct{}{}
			}
			for _, s := range plan.Overlaps {
				compactedIDs[s.ID] = struct{}{}
			}

			var updatedReaders []*segment.Reader
			for _, r := range s.readers {
				if _, found := compactedIDs[r.GetSegmentID()]; !found {
					updatedReaders = append(updatedReaders, r)
				} else {
					r.Release() // Decrement ref count for old reader
				}
			}
			updatedReaders = append(updatedReaders, newReaders...)
			s.readers = updatedReaders
			s.updateStatsFromManifest()
			s.mu.Unlock()
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
