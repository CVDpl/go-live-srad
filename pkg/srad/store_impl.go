package srad

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
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

// segBatch is a batch of items for efficient channel communication.
type segBatch struct {
	items []segItem
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

	// Coordination counters
	compactionPauseCount int32 // >0 means paused

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
	readersMap    map[uint64]*segment.Reader // Track readers by segmentID for RCU refcount checks
	manifest      *manifest.Manifest
	compactor     *compaction.Compactor
	mmapCache     *segment.MmapCache // LRU cache for keys.dat mmap to limit VIRT

	// Frozen memtable snapshots visible to readers during flush
	frozenSnaps []*memtable.Snapshot

	// Wait group for background tasks
	bgWg sync.WaitGroup

	// Tracks in-progress flush operations to allow graceful shutdown
	flushWg sync.WaitGroup

	// Memory pooling for hot-path allocations
	memPool *MemoryPool
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
		memPool:          NewMemoryPool(),
		readersMap:       make(map[uint64]*segment.Reader),
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

	// Initialize mmap cache for keys.dat files (unless disabled with -1)
	if opts.MaxMmapCacheSize >= 0 {
		cacheSize := opts.MaxMmapCacheSize
		if cacheSize == 0 {
			cacheSize = segment.DefaultMaxMmapCacheSize
		}
		idleTimeout := opts.MmapCacheIdleTimeout
		if idleTimeout == 0 {
			idleTimeout = segment.DefaultMmapCacheIdleTimeout
		}
		s.mmapCache = segment.NewMmapCacheWithTimeout(cacheSize, idleTimeout, s.logger)
		s.logger.Info("mmap cache initialized", "max_size", cacheSize, "idle_timeout", idleTimeout)
	}

	// Load existing segments
	if err := s.loadSegments(); err != nil {
		s.logger.Warn("failed to load segments", "error", err)
	}

	// Safety check for orphaned segments (segments on disk not in manifest)
	if err := s.detectOrphanedSegments(); err != nil {
		s.logger.Warn("failed to detect orphaned segments", "error", err)
	}

	// Update stats based on loaded manifest/segments
	s.updateStatsFromManifest()

	// Initialize compactor
	if s.manifest != nil && !s.readonly {
		alloc := func() uint64 { return atomic.AddUint64(&s.nextSegmentID, 1) }
		s.compactor = compaction.NewCompactor(dir, s.manifest, s.logger, alloc)
		// Share mmap cache with compactor
		if s.mmapCache != nil {
			s.compactor.SetMmapCache(s.mmapCache)
		}
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

	// Flush memtable if not read-only (with timeout to prevent hanging)
	if !s.readonly {
		if mt := s.memtablePtr.Load(); mt != nil && (mt.Count() > 0 || mt.DeletedCount() > 0) {
			entryCount := mt.Count() + mt.DeletedCount()
			s.logger.Info("flushing memtable during close", "entries", entryCount)

			// Use timeout to prevent hanging on close
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			if err := s.Flush(ctx); err != nil {
				s.logger.Error("failed to flush memtable during close - data may be lost",
					"error", err,
					"entries", entryCount,
					"note", "Memtable data will be preserved in WAL if WAL sync succeeded")

				// Try to sync WAL at least to preserve data
				if s.wal != nil {
					if syncErr := s.wal.Sync(); syncErr != nil {
						s.logger.Error("failed to sync WAL during close after flush failure - potential data loss",
							"flush_error", err,
							"sync_error", syncErr,
							"entries", entryCount)
					} else {
						s.logger.Info("WAL synced successfully after flush failure - data preserved for recovery", "entries", entryCount)
					}
				}
			} else {
				s.logger.Info("memtable flushed successfully during close", "entries", entryCount)
			}
		}

		// Wait for any in-progress flush to complete to avoid truncating segment writes
		// No timeout - we must ensure all data is safely written to disk
		s.logger.Info("waiting for flush to complete before closing")
		s.flushWg.Wait()
		s.logger.Info("flush completed, proceeding with close")
	}

	// Close WAL
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			s.logger.Error("failed to close WAL", "error", err)
		}
	}

	// Close mmap cache (unmaps all remaining cached keys.dat files)
	if s.mmapCache != nil {
		if err := s.mmapCache.Close(); err != nil {
			s.logger.Error("failed to close mmap cache", "error", err)
		}
	}

	s.logger.Info("store closed", "dir", s.dir)

	return nil
}

// Insert adds a string to the store.
func (s *storeImpl) Insert(key []byte) error {
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

	ttl := s.opts.DefaultTTL
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
	// Check if store is still open before sending to avoid panic on closed channel
	if atomic.LoadInt32(&s.closed) == 1 {
		s.mtGuard.Unlock()
		return common.ErrClosed
	}
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
	// Check if store is still open before sending to avoid panic on closed channel
	if atomic.LoadInt32(&s.closed) == 1 {
		s.mtGuard.Unlock()
		return common.ErrClosed
	}
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
	// Check if store is still open before sending to avoid panic on closed channel
	if atomic.LoadInt32(&s.closed) == 1 {
		s.mtGuard.Unlock()
		return common.ErrClosed
	}
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
	// Use bounded map with LRU-style eviction to prevent unbounded growth
	const maxSeenSize = 100000 // Limit to 100k entries

	// Pre-size seen map based on expected tombstone count for better performance
	initialSeenSize := 1024
	if memSnap != nil {
		// Estimate tombstone count: deleted / total
		mt := s.memtablePtr.Load()
		if mt != nil {
			totalCount := mt.Count()
			deletedCount := mt.DeletedCount()
			if totalCount > 0 && deletedCount > 0 {
				// Estimate with some buffer
				estimatedTombstones := int(float64(deletedCount) * 1.2)
				if estimatedTombstones > initialSeenSize && estimatedTombstones < maxSeenSize {
					initialSeenSize = estimatedTombstones
				} else if estimatedTombstones >= maxSeenSize {
					initialSeenSize = maxSeenSize / 2
				}
			}
		}
	}

	seen := make(map[string]struct{}, initialSeenSize)
	seenMu := &sync.Mutex{}
	if memSnap != nil {
		keys, tombs := memSnap.GetAllWithTombstones()
		for i := range keys {
			if tombs[i] {
				seenMu.Lock()
				// Prevent unbounded growth
				if len(seen) >= maxSeenSize {
					// Clear oldest half when limit reached (simple eviction strategy)
					newSeen := make(map[string]struct{}, maxSeenSize/2)
					count := 0
					for k := range seen {
						if count >= maxSeenSize/2 {
							break
						}
						newSeen[k] = struct{}{}
						count++
					}
					seen = newSeen
				}
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
			// Only use exact path if literal prefix is complete (no regex metacharacters)
			if lit, complete := re.LiteralPrefix(); lit != "" && complete {
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
			// COW: Release snapshot immediately - we've copied all data we need
			memSnap.Release()
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
		// For broad scans with no segments and memIter==nil, pre-load memtable keys
		var preloadedKeys [][]byte
		if broad && memSnap != nil {
			keys, tombs, _ := memSnap.GetAllWithExpiry()
			for i, k := range keys {
				if !tombs[i] { // Skip tombstones
					preloadedKeys = append(preloadedKeys, k)
				}
			}
		}
		return &simpleIterator{
			memIter:       memIter,
			memSnap:       memSnap, // COW: Store snapshot for Release() on Close()
			frozenIters:   frozenIters,
			preloadedKeys: preloadedKeys,
			mode:          q.Mode,
			limit:         q.Limit,
			count:         0,
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
			// COW: Release snapshot immediately - we've copied all data we need
			memSnap.Release()
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
			if !r.IncRef() {
				// Reader is being closed, skip it
				continue
			}
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

	out := make(chan segBatch, 64) // Fewer, larger messages
	// Create a cancellable context for the background producer
	pctx, cancel := context.WithCancel(ctx)
	go func(readers []*segment.Reader) {
		defer close(out)
		defer func() {
			// Cleanup: release any readers that weren't released during iteration
			if r := recover(); r != nil {
				s.logger.Error("panic in regex search producer goroutine", "panic", r)
			}
		}()

		const batchSize = 512 // Batch items for efficient channel communication
		batch := make([]segItem, 0, batchSize)

		// Helper to send current batch
		sendBatch := func() bool {
			if len(batch) == 0 {
				return true
			}
			select {
			case out <- segBatch{items: batch}:
				batch = make([]segItem, 0, batchSize)
				return true
			case <-pctx.Done():
				return false
			}
		}

		// For broad scans, emit all memtable keys first via channel
		if broad && memSnap != nil {
			keys, tombs, _ := memSnap.GetAllWithExpiry()
			emitted := 0
			for i, k := range keys {
				select {
				case <-pctx.Done():
					return
				default:
				}
				// Skip tombstones (they're tracked in seen map)
				if tombs[i] {
					continue
				}
				if re != nil && !re.Match(k) {
					continue
				}

				batch = append(batch, segItem{id: 0, key: append([]byte(nil), k...), op: common.OpInsert})
				emitted++

				if len(batch) >= batchSize {
					if !sendBatch() {
						return
					}
				}
			}
			sendBatch() // Flush remaining
			s.logger.Info("broad scan emitted memtable keys", "count", emitted)
		}

		for _, r := range readers {
			if !unsafeBroadScan {
				if re != nil && !r.MayContain(re) {
					continue
				}
			}
			if !r.IncRef() {
				// Reader is being closed, skip it
				continue
			}
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
			// Use AllKeys() instead of StreamKeys() to avoid batching bugs
			allKeys := r.AllKeys()
			for _, k := range allKeys {
				select {
				case <-pctx.Done():
					r.Release()
					return
				default:
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

				batch = append(batch, segItem{id: r.GetSegmentID(), key: append([]byte(nil), k...), op: common.OpInsert})
				emitted++

				if len(batch) >= batchSize {
					if !sendBatch() {
						r.Release()
						return
					}
				}
			}
			sendBatch() // Flush remaining items for this segment
			r.Release()
			s.logger.Info("broad scan emitted segment keys", "segment", r.GetSegmentID(), "count", emitted)
		}
		sendBatch() // Final flush
	}(selected)

	// Drain into merged iterator (memtable first)
	return &parallelMergedIterator{
		memIter:     memIter,
		memSnap:     memSnap, // COW: Store snapshot for Release() on Close()
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
	memSnap     *memtable.Snapshot // COW: Need to Release() when done
	frozenIters []*memtable.Iterator
	in          chan segBatch // Batched channel
	batchBuf    []segItem     // Buffer for current batch
	batchIdx    int           // Current index in batch
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
		// Prevent unbounded growth of seen map
		const maxSeenSize = 100000
		if len(it.seen) >= maxSeenSize {
			// Clear oldest half when limit reached
			newSeen := make(map[string]struct{}, maxSeenSize/2)
			count := 0
			for k := range it.seen {
				if count >= maxSeenSize/2 {
					break
				}
				newSeen[k] = struct{}{}
				count++
			}
			it.seen = newSeen
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
	// then read from segments channel (batched)
	for it.in != nil {
		// Process items from current batch buffer
		for it.batchIdx < len(it.batchBuf) {
			x := it.batchBuf[it.batchIdx]
			it.batchIdx++

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
		}

		// Batch exhausted, fetch next batch
		select {
		case batch, ok := <-it.in:
			if !ok {
				it.in = nil
				return false
			}
			it.batchBuf = batch.items
			it.batchIdx = 0
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
	// COW: Release snapshot to decrement refcounts
	if it.memSnap != nil {
		it.memSnap.Release()
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

// maybeScheduleAsyncFilters schedules background filter building for segments without filters.
// This is called after flush/compact when AsyncFilterBuild is enabled.
func (s *storeImpl) maybeScheduleAsyncFilters() {
	if !s.opts.AsyncFilterBuild {
		return
	}
	if s.manifest == nil {
		return
	}
	// Best-effort background job
	go func() {
		// Check if store is closed before starting work
		if atomic.LoadInt32(&s.closed) == 1 {
			return
		}

		s.mu.RLock()
		active := s.manifest.GetActiveSegments()
		dir := filepath.Join(s.dir, common.DirSegments)
		logger := s.logger
		s.mu.RUnlock()

		for _, seg := range active {
			// Check for shutdown on each iteration
			if atomic.LoadInt32(&s.closed) == 1 {
				return
			}
			// Open reader and check filters
			reader, err := segment.NewReaderWithCache(seg.ID, dir, logger, s.opts.VerifyChecksumsOnLoad, s.mmapCache)
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
				if err := b.BuildBloomOnly(filtersDir); err != nil {
					logger.Warn("failed to build bloom filter (async)", "segment", seg.ID, "error", err)
				} else {
					logger.Info("built bloom filter (async)", "segment", seg.ID)
				}
			}
			if needTri && s.opts.EnableTrigramFilter {
				if err := b.BuildTrigramOnly(filtersDir); err != nil {
					logger.Warn("failed to build trigram filter (async)", "segment", seg.ID, "error", err)
				} else {
					logger.Info("built trigram filter (async)", "segment", seg.ID)
				}
			}
		}
	}()
}

// RebuildMissingFilters synchronously rebuilds missing Bloom/Trigram filters for active segments.
func (s *storeImpl) RebuildMissingFilters(ctx context.Context) error {
	atomic.AddInt32(&s.compactionPauseCount, 1)
	defer atomic.AddInt32(&s.compactionPauseCount, -1)
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
		reader, err := segment.NewReaderWithCache(seg.ID, dir, logger, s.opts.VerifyChecksumsOnLoad, s.mmapCache)
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
			if err := b.BuildBloomOnly(filtersDir); err != nil {
				logger.Warn("failed to build bloom filter", "segment", seg.ID, "error", err)
			} else {
				logger.Info("built bloom filter", "segment", seg.ID)
			}
		}
		if needTri {
			if err := b.BuildTrigramOnly(filtersDir); err != nil {
				logger.Warn("failed to build trigram filter", "segment", seg.ID, "error", err)
			} else {
				logger.Info("built trigram filter", "segment", seg.ID)
			}
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
		segPath := filepath.Join(segmentsDir, name)
		if err := os.RemoveAll(segPath); err != nil {
			s.logger.Warn("failed to purge obsolete segment dir", "name", name, "error", err)
			atomic.AddUint64(&s.stats.CleanupFailures, 1)
		} else {
			s.logger.Info("purged obsolete segment dir", "name", name)
			atomic.AddUint64(&s.stats.SegmentsDeleted, 1)
		}
	}
	return nil
}

// Flush forces a flush of the memtable to disk.
func (s *storeImpl) Flush(ctx context.Context) error {
	if s.readonly {
		return common.ErrReadOnly
	}

	start := time.Now()

	// Mark flush as in-progress to allow Close() to wait for completion
	s.flushWg.Add(1)
	defer s.flushWg.Done()

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

	if parts == 1 {
		// Single builder path (existing)
		segID := atomic.AddUint64(&s.nextSegmentID, 1)
		builder := segment.NewBuilder(segID, common.LevelL0, segmentsDir, s.logger)
		cfgBuilder(builder)
		s.logger.Info("flush plan: single-partition prepared", "segment_id", segID, "entries", oldCount)

		addStart := time.Now()
		if err := builder.AddFromMemtable(snapshot); err != nil {
			s.logger.Error("failed to add memtable to builder", "segment_id", segID, "error", err)
			// Rollback: reinsert snapshot into current memtable to avoid data loss
			s.rollbackSnapshotIntoMemtable(snapshot)
			return fmt.Errorf("add memtable to builder: %w", err)
		}
		addDone := time.Now()
		s.logger.Info("flush stage: builder input added", "segment_id", segID, "duration", addDone.Sub(addStart))

		// Check if builder has any keys after adding from memtable
		// This can happen if all entries were tombstones or expired
		if len(snapshot.GetAll()) == 0 {
			s.logger.Warn("flush completed with no keys to write (all expired or deleted)",
				"segment_id", segID,
				"original_count", oldCount,
				"original_deleted", oldDeleted)
			close(results)
			// Don't return error - this is a valid case, just unusual
			// Continue to let flush complete normally (no segments created, but that's OK)
		} else {
			buildStart := time.Now()
			s.logger.Info("flush stage: starting build", "segment_id", segID)
			md, err := builder.BuildWithContext(ctx)
			if err != nil {
				s.logger.Error("failed to build segment", "segment_id", segID, "error", err)
				s.rollbackSnapshotIntoMemtable(snapshot)
				return fmt.Errorf("build segment: %w", err)
			}
			s.logger.Info("flush stage: builder.Build completed", "segment_id", segID, "duration", time.Since(buildStart))
			results <- partResult{id: segID, md: md, err: nil}
			close(results)
		}
	} else {
		// Range-partition snapshot by first byte (0..255). If it collapses to <=1 non-empty bucket,
		// fallback to hash-based partitioning to ensure parallel build even for heavy shared prefixes.
		partPrepStart := time.Now()

		// Use iterator-based approach to avoid materializing all keys at once
		it := snapshot.IterateWithTombstones()
		buckets := make([][][]byte, parts)
		btombs := make([][]bool, parts)
		for i := range buckets {
			buckets[i] = make([][]byte, 0, int(oldCount)/parts+1)
			btombs[i] = make([]bool, 0, int(oldCount)/parts+1)
		}

		totalKeys := 0
		// First pass: collect keys and partition by first byte
		allKeys := make([][]byte, 0, oldCount)
		allTombs := make([]bool, 0, oldCount)

		for it.Next() {
			key := it.Key()
			// Make a copy since iterator may reuse the slice
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)

			allKeys = append(allKeys, keyCopy)
			allTombs = append(allTombs, it.IsTombstone())
			totalKeys++

			idx := 0
			if len(keyCopy) > 0 {
				idx = int(keyCopy[0]) * parts / 256
				if idx >= parts {
					idx = parts - 1
				}
			}
			buckets[idx] = append(buckets[idx], keyCopy)
			btombs[idx] = append(btombs[idx], it.IsTombstone())
		}

		// Critical check: if snapshot is empty after filtering expired keys
		if totalKeys == 0 {
			s.logger.Warn("flush completed with no keys after expiry filtering",
				"original_count", oldCount,
				"original_deleted", oldDeleted,
				"note", "All entries expired between flush start and snapshot creation")
			// Don't rollback - data already expired naturally
			// Still sync WAL to ensure any non-expired data in WAL is preserved
			if s.wal != nil {
				if err := s.wal.Sync(); err != nil {
					s.logger.Error("failed to sync WAL after empty snapshot", "error", err)
				}
			}
			// Don't prune WAL since no segments were written
			s.lastFlushTime = time.Now()
			return nil // Not an error - data naturally expired
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
			for i, k := range allKeys {
				h := fnv32(k)
				idx := int(h % uint32(parts))
				buckets[idx] = append(buckets[idx], k)
				btombs[idx] = append(btombs[idx], allTombs[i])
			}
			s.logger.Info("flush using hash-based partitioning", "parts", parts)
		}
		s.logger.Info("flush stage: partitioning done", "parts", parts, "non_empty", nonEmpty, "duration", time.Since(partPrepStart), "total_keys", totalKeys)
		var wg sync.WaitGroup
		for p := 0; p < parts; p++ {
			if len(buckets[p]) == 0 {
				continue
			}
			wg.Add(1)
			go func(pid int) {
				defer wg.Done()

				// Panic recovery to prevent goroutine crashes from hanging flush
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error("panic in partition builder goroutine",
							"partition", pid,
							"panic", r,
							"note", "This indicates a serious bug in segment building")
						segID := atomic.AddUint64(&s.nextSegmentID, 1)
						results <- partResult{id: segID, err: fmt.Errorf("partition builder panic: %v", r)}
					}
				}()

				segID := atomic.AddUint64(&s.nextSegmentID, 1)
				s.logger.Info("flush stage: starting partition build", "partition", pid, "segment_id", segID, "keys", len(buckets[pid]))

				builder := segment.NewBuilder(segID, common.LevelL0, segmentsDir, s.logger)
				cfgBuilder(builder)

				partStart := time.Now()
				if err := builder.AddFromPairs(buckets[pid], btombs[pid]); err != nil {
					s.logger.Error("failed to add partition pairs to builder", "partition", pid, "segment_id", segID, "error", err)
					results <- partResult{id: segID, err: fmt.Errorf("add partition: %w", err)}
					return
				}
				s.logger.Info("flush stage: partition input added", "partition", pid, "segment_id", segID, "keys", len(buckets[pid]), "duration", time.Since(partStart))

				buildStart := time.Now()
				s.logger.Debug("flush stage: starting partition build phase", "partition", pid, "segment_id", segID)
				md, err := builder.BuildWithContext(ctx)
				if err != nil {
					s.logger.Error("failed to build partition segment", "partition", pid, "segment_id", segID, "error", err)
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
	s.logger.Info("flush stage: waiting for partition results", "expected_partitions", parts)

	resultsReceived := 0

	// Add context timeout protection to prevent infinite hanging
	resultsDone := make(chan bool, 1)
	var collectError error

	go func() {
		defer func() { resultsDone <- true }()
		for r := range results {
			resultsReceived++
			s.logger.Debug("flush stage: received result", "result_number", resultsReceived, "segment_id", r.id, "has_error", r.err != nil)

			if r.err != nil {
				s.logger.Error("flush stage: partition failed", "segment_id", r.id, "error", r.err, "rolling_back", true)
				collectError = r.err
				return
			}
			mds = append(mds, r.md)
		}
	}()

	// Wait for results with context timeout
	select {
	case <-resultsDone:
		if collectError != nil {
			s.rollbackSnapshotIntoMemtable(snapshot)
			return collectError
		}
		s.logger.Info("flush stage: collected results", "segments", len(mds), "results_received", resultsReceived, "duration", time.Since(collectStart))
	case <-ctx.Done():
		s.logger.Error("flush stage: timeout waiting for partition results",
			"timeout", ctx.Err(),
			"results_received", resultsReceived,
			"expected", parts,
			"note", "This indicates partition building is taking too long or goroutines are stuck")
		s.rollbackSnapshotIntoMemtable(snapshot)
		return fmt.Errorf("flush timeout: %w", ctx.Err())
	}

	// Critical check: if no segments were built, log this clearly
	// This can happen if all entries were expired or deleted
	if len(mds) == 0 {
		s.logger.Warn("flush completed with no segments created",
			"original_count", oldCount,
			"original_deleted", oldDeleted,
			"note", "This may indicate all entries expired or were tombstones. Data in WAL will be preserved.")

		// Sync WAL to ensure data is preserved for potential recovery
		if s.wal != nil {
			if err := s.wal.Sync(); err != nil {
				return fmt.Errorf("sync WAL after empty flush: %w", err)
			}
		}

		// Don't prune WAL since no segments were written
		s.lastFlushTime = time.Now()
		s.logger.Info("empty flush completed - WAL preserved", "duration", time.Since(start))
		return nil
	}

	// Prepare manifest entries for all segments first
	var manifestInfos []manifest.SegmentInfo
	if s.manifest != nil {
		for _, md := range mds {
			info := manifest.SegmentInfo{
				ID:      md.SegmentID,
				Level:   common.LevelL0,
				NumKeys: md.Counts.Accepted,
				Size:    s.computeSegmentSize(md.SegmentID),
			}
			if minb, err := md.GetMinKey(); err == nil {
				info.MinKeyHex = fmt.Sprintf("%x", minb)
			}
			if maxb, err := md.GetMaxKey(); err == nil {
				info.MaxKeyHex = fmt.Sprintf("%x", maxb)
			}
			manifestInfos = append(manifestInfos, info)
		}

		// Atomically add all segments to manifest first
		// If this fails, we need to clean up segment files and fail the flush
		s.logger.Info("attempting atomic manifest update", "segments", len(manifestInfos))
		if err := s.manifest.AddSegments(manifestInfos); err != nil {
			s.logger.Error("failed to add segments to manifest - cleaning up segment files", "error", err, "segments", len(mds))
			// Clean up segment files since manifest update failed
			cleanupFailed := 0
			for _, md := range mds {
				segmentPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", md.SegmentID))
				if err := os.RemoveAll(segmentPath); err != nil {
					cleanupFailed++
					s.logger.Warn("failed to clean up segment file after manifest failure", "id", md.SegmentID, "path", segmentPath, "error", err)
				} else {
					s.logger.Info("cleaned up segment file after manifest failure", "id", md.SegmentID, "path", segmentPath)
				}
			}
			if cleanupFailed > 0 {
				s.logger.Error("cleanup incomplete - some orphaned segment files may remain", "failed_cleanups", cleanupFailed)
			}
			// Rollback: reinsert snapshot into current memtable to avoid data loss
			s.rollbackSnapshotIntoMemtable(snapshot)
			return fmt.Errorf("atomic manifest update failed: %w", err)
		}
		s.logger.Info("atomic manifest update succeeded", "segments", len(manifestInfos))

		// Remove .building sentinels now that segments are safely in manifest
		for _, md := range mds {
			buildingPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", md.SegmentID), ".building")
			if err := os.Remove(buildingPath); err != nil && !os.IsNotExist(err) {
				s.logger.Warn("failed to remove .building sentinel after manifest update", "id", md.SegmentID, "error", err)
				// Non-fatal: sentinel will eventually be cleaned up by RCU after grace period
			}
		}
	}

	// Update in-memory state after successful manifest update
	applyStart := time.Now()
	s.mu.Lock()
	for _, md := range mds {
		s.segments = append(s.segments, md)
		reader, rerr := segment.NewReaderWithCache(md.SegmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad, s.mmapCache)
		if rerr == nil {
			s.readers = append(s.readers, reader)
			s.readersMap[md.SegmentID] = reader // Track for RCU refcount checks
		} else {
			s.logger.Warn("failed to open reader for new segment", "id", md.SegmentID, "error", rerr)
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

			// COW: Release snapshot now that it's no longer needed
			// This allows memtable to reclaim memory from nodes no longer referenced
			snapshot.Release()
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
			s.logger.Error("failed to rotate WAL after flush", "error", err)
			return fmt.Errorf("rotate WAL after flush: %w", err)
		}
	}

	// Automatically prune old WAL files after successful flush
	// This is safe because data is now persisted in segments and manifest
	if s.wal != nil {
		if err := s.PruneWAL(); err != nil {
			s.logger.Warn("failed to prune old WAL files after flush", "error", err)
			// Don't fail the flush for WAL cleanup issues
		} else {
			s.logger.Info("pruned old WAL files after flush")
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

		// CRITICAL FIX: Check if segment is locked for compaction
		if s.manifest.IsSegmentLockedForCompaction(segID) {
			s.logger.Debug("rcu skipping segment locked for compaction", "id", segID)
			continue
		}

		// CRITICAL: Check refcount before deletion to prevent race conditions
		s.mu.RLock()
		reader, hasReader := s.readersMap[segID]
		s.mu.RUnlock()

		if hasReader && reader != nil {
			refcount := reader.GetRefcount()
			if refcount > 0 {
				s.logger.Debug("rcu skipping segment with active refcount", "id", segID, "refcount", refcount)
				continue
			}
			// Refcount is 0, safe to delete - remove from map
			s.mu.Lock()
			delete(s.readersMap, segID)
			s.mu.Unlock()
		}

		// Double-check compaction lock after refcount check (prevents TOCTOU race)
		if s.manifest.IsSegmentLockedForCompaction(segID) {
			s.logger.Debug("rcu skipping segment locked for compaction (second check)", "id", segID)
			continue
		}

		if info, err := os.Stat(segPath); err == nil {
			if info.ModTime().Unix() < cutoff {
				if err := os.RemoveAll(segPath); err != nil {
					s.logger.Warn("rcu failed to remove segment dir", "id", segID, "error", err)
					atomic.AddUint64(&s.stats.CleanupFailures, 1)
				} else {
					s.logger.Info("rcu removed obsolete segment dir", "id", segID)
					atomic.AddUint64(&s.stats.SegmentsDeleted, 1)
				}
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
		mt := s.memtablePtr.Load()
		if mt == nil {
			return fmt.Errorf("memtable not initialized")
		}

		switch op {
		case common.OpInsert:
			if expiresAt.IsZero() || time.Now().Before(expiresAt) {
				// OPTIMIZATION: WAL already allocates a fresh key for each record,
				// so we can use the zero-copy variant to avoid redundant copying.
				if expiresAt.IsZero() {
					expiresAt = time.Time{} // Ensure zero value
				}
				if err := mt.InsertWithExpiryNoCopy(key, expiresAt); err != nil {
					return err
				}
			}
		case common.OpDelete:
			// OPTIMIZATION: Same reasoning - WAL provides fresh key, use zero-copy
			if err := mt.DeleteNoCopy(key); err != nil {
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

	// Track if we've already closed the channel to prevent double-close panic
	channelClosed := false

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in WAL writer goroutine", "panic", r)
			// Close channel only if not already closed
			if s.writeCh != nil && !channelClosed {
				close(s.writeCh)
				channelClosed = true
			}
		}
	}()
	requests := make([]*writeRequest, 0, 128)
	entries := make([]*wal.WALEntry, 0, 128)

	var flushTicker *time.Ticker
	if s.opts.WALFlushEveryInterval > 0 {
		flushTicker = time.NewTicker(s.opts.WALFlushEveryInterval)
		defer flushTicker.Stop()
	}

	for {
		var firstReq *writeRequest

		select {
		case <-s.walStop:
			// Drain any remaining requests on shutdown
			if !channelClosed {
				close(s.writeCh)
				channelClosed = true
			}
			for req := range s.writeCh {
				requests = append(requests, req)
			}
			if len(requests) > 0 {
				s.flushWALRequests(&requests, &entries)
			}
			return
		case <-func() <-chan time.Time {
			if flushTicker != nil {
				return flushTicker.C
			}
			return nil
		}():
			// Periodic flush (best-effort): if nothing pending, still call Flush()
			// to push any WAL buffer content to OS even without new requests.
			if err := s.wal.Flush(); err != nil {
				s.logger.Warn("periodic WAL flush failed", "error", err)
			}
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
	closeFailed := 0
	for _, r := range s.readers {
		if err := r.Close(); err != nil {
			s.logger.Warn("failed to close segment reader during shutdown", "id", r.GetSegmentID(), "error", err)
			closeFailed++
		}
	}
	if closeFailed > 0 {
		s.logger.Warn("some segment readers failed to close", "failed", closeFailed)
	}
	s.readers = nil
	s.readersMap = make(map[uint64]*segment.Reader) // Clear readersMap
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

			// Re-check pause right before executing to avoid race with pause engaged after planning.
			if atomic.LoadInt32(&s.compactionPauseCount) > 0 {
				continue
			}

			// SELF-HEALING: Pre-validate that all planned segments exist on disk
			// This catches cases where segments were deleted between Plan() and Execute()
			segmentsDir := filepath.Join(s.dir, common.DirSegments)
			var missingSelfHeal []uint64
			for _, seg := range plan.Inputs {
				segPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", seg.ID), "segment.json")
				if _, err := os.Stat(segPath); os.IsNotExist(err) {
					s.logger.Warn("pre-compaction check: segment missing from disk",
						"segment_id", seg.ID,
						"path", segPath,
						"note", "Will remove from manifest during self-healing",
					)
					missingSelfHeal = append(missingSelfHeal, seg.ID)
				}
			}
			for _, seg := range plan.Overlaps {
				segPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", seg.ID), "segment.json")
				if _, err := os.Stat(segPath); os.IsNotExist(err) {
					s.logger.Warn("pre-compaction check: overlapping segment missing from disk",
						"segment_id", seg.ID,
						"path", segPath,
						"note", "Will remove from manifest during self-healing",
					)
					missingSelfHeal = append(missingSelfHeal, seg.ID)
				}
			}

			// Self-heal missing segments before attempting compaction
			if len(missingSelfHeal) > 0 {
				s.logger.Info("self-healing: removing missing segments from manifest before compaction",
					"count", len(missingSelfHeal),
					"segment_ids", missingSelfHeal,
				)
				if err := s.manifest.RemoveSegments(missingSelfHeal); err != nil {
					s.logger.Error("self-healing: failed to remove missing segments",
						"error", err,
						"segment_ids", missingSelfHeal,
					)
				} else {
					s.logger.Info("self-healing: successfully removed missing segments from manifest",
						"count", len(missingSelfHeal),
					)
				}
				// Re-plan after self-healing
				plan = s.compactor.Plan()
				if plan == nil {
					s.logger.Info("after self-healing: no compaction work needed")
					continue
				}
			}

			// The execution is the long-running part, also without a store-level lock.
			// It will atomically update the manifest upon completion.
			_, err := s.compactor.Execute(plan)
			if err != nil {
				if atomic.LoadInt32(&s.closed) == 1 || atomic.LoadInt32(&s.compactionPauseCount) > 0 {
					// Suppress errors while shutting down or paused
					continue
				}

				// Check if this is a "segment not found" error and attempt self-healing
				if errors.Is(err, os.ErrNotExist) || (err != nil && (strings.Contains(err.Error(), "no such file") || strings.Contains(err.Error(), "not found"))) {
					s.logger.Warn("compaction failed due to missing segment - triggering self-healing scan",
						"error", err,
						"reason", plan.Reason,
					)
					// Trigger a full self-healing scan
					if healErr := s.selfHealMissingSegments(); healErr != nil {
						s.logger.Error("self-healing scan failed", "error", healErr)
					}
					continue
				}

				// Log comprehensive diagnostic information for troubleshooting
				s.logger.Error("background compaction failed",
					"error", err,
					"reason", plan.Reason,
					"level", plan.Level,
					"target_level", plan.TargetLevel,
					"input_segments", len(plan.Inputs),
					"input_ids", plan.Inputs,
					"overlapping_segments", len(plan.Overlaps),
				)
				continue
			}

			// After a successful compaction, the source of truth (the manifest) has changed.
			// Reload segments without creating a gap in query access
			s.logger.Info("compaction finished, reloading segment readers to reflect changes")

			// Load new segments first (outside of critical section)
			// Note: segmentsDir is already defined earlier in this function
			var newSegments []*segment.Metadata
			var newReaders []*segment.Reader
			var loadErr error

			if s.manifest != nil {
				active := s.manifest.GetActiveSegments()
				var missingSegmentIDs []uint64
				for _, seg := range active {
					segmentID := seg.ID
					// Load segment metadata
					metadataPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", segmentID), "segment.json")
					metadata, err := segment.LoadFromFile(metadataPath)
					if err != nil {
						// Self-healing: if segment is missing, remove from manifest
						if errors.Is(err, os.ErrNotExist) {
							s.logger.Warn("segment missing during reload, will remove from manifest",
								"segment_id", segmentID,
								"path", metadataPath,
								"error", err,
							)
							missingSegmentIDs = append(missingSegmentIDs, segmentID)
							continue
						}
						s.logger.Warn("failed to load segment metadata during reload", "id", segmentID, "error", err)
						// Cleanup already loaded readers before continuing
						for _, r := range newReaders {
							r.Close()
						}
						newReaders = newReaders[:0]
						newSegments = newSegments[:0]
						loadErr = fmt.Errorf("load metadata for segment %d: %w", segmentID, err)
						break
					}
					reader, err := segment.NewReaderWithCache(segmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad, s.mmapCache)
					if err != nil {
						// Self-healing: if segment is missing, remove from manifest
						if errors.Is(err, os.ErrNotExist) {
							s.logger.Warn("segment missing during reload, will remove from manifest",
								"segment_id", segmentID,
								"error", err,
							)
							missingSegmentIDs = append(missingSegmentIDs, segmentID)
							continue
						}
						s.logger.Warn("failed to create segment reader during reload", "id", segmentID, "error", err)
						// Cleanup already loaded readers before continuing
						for _, r := range newReaders {
							r.Close()
						}
						newReaders = newReaders[:0]
						newSegments = newSegments[:0]
						loadErr = fmt.Errorf("create reader for segment %d: %w", segmentID, err)
						break
					}
					newSegments = append(newSegments, metadata)
					newReaders = append(newReaders, reader)
				}

				// Remove missing segments from manifest
				if len(missingSegmentIDs) > 0 {
					s.logger.Info("removing missing segments from manifest during reload",
						"count", len(missingSegmentIDs),
						"segment_ids", missingSegmentIDs,
					)
					if err := s.manifest.RemoveSegments(missingSegmentIDs); err != nil {
						s.logger.Error("failed to remove missing segments from manifest during reload",
							"error", err,
							"segment_ids", missingSegmentIDs,
						)
						// Don't fail the reload - we can continue with loaded segments
					} else {
						s.logger.Info("successfully removed missing segments from manifest", "count", len(missingSegmentIDs))
					}
				}
			}

			// If loading failed, skip the swap and keep old readers
			if loadErr != nil {
				s.logger.Error("failed to reload segments after compaction, keeping old readers", "error", loadErr)
				continue
			}

			// Atomic swap under lock to prevent query access gap
			s.mu.Lock()
			oldReaders := s.readers

			// Install new readers and segments atomically
			s.readers = newReaders
			s.segments = newSegments
			s.updateStatsFromManifest()

			s.mu.Unlock()

			// Release old readers after the swap (outside critical section)
			for _, r := range oldReaders {
				r.Release()
			}

			s.logger.Info("segment readers reloaded successfully", "new_count", len(newReaders), "old_count", len(oldReaders))
		}
	}
}

// autotuneTask is the background autotuning task.
// This performs actual parameter tuning based on performance metrics.
func (s *storeImpl) autotuneTask() {
	defer s.bgWg.Done()

	ticker := time.NewTicker(30 * time.Second) // Check for tuning opportunities every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.autotuneStop:
			return
		case <-ticker.C:
			if atomic.LoadInt32(&s.closed) == 1 {
				return
			}
			// Actual autotuning logic would go here
			// For now, this is a placeholder that doesn't duplicate compaction logic
			s.performAutotuning()
		}
	}
}

// performAutotuning analyzes performance metrics and adjusts parameters
func (s *storeImpl) performAutotuning() {
	if !s.autotunerEnabled {
		return
	}

	// Get current statistics
	stats := s.stats.GetStats()
	s.logger.Debug("autotuning analysis starting",
		"writes_per_sec", stats.WritesPerSecond,
		"queries_per_sec", stats.QueriesPerSecond,
		"p99_latency_ms", stats.LatencyP99.Milliseconds(),
		"total_bytes", stats.TotalBytes)

	var adjustmentsMade []string
	changesMade := false

	// 1. Memtable Size Tuning
	if newSize := s.tuneMemtableSize(stats); newSize != s.opts.MemtableTargetBytes {
		s.mu.Lock()
		oldSize := s.opts.MemtableTargetBytes
		s.opts.MemtableTargetBytes = newSize
		s.mu.Unlock()

		adjustmentsMade = append(adjustmentsMade, fmt.Sprintf("memtable: %d -> %d bytes", oldSize, newSize))
		changesMade = true
	}

	// 2. Cache Size Tuning
	if newLabelCache := s.tuneLabelAdvanceCache(stats); newLabelCache != s.opts.CacheLabelAdvanceBytes {
		s.mu.Lock()
		oldSize := s.opts.CacheLabelAdvanceBytes
		s.opts.CacheLabelAdvanceBytes = newLabelCache
		s.mu.Unlock()

		adjustmentsMade = append(adjustmentsMade, fmt.Sprintf("label_cache: %d -> %d bytes", oldSize, newLabelCache))
		changesMade = true
	}

	if newNFACache := s.tuneNFATransitionCache(stats); newNFACache != s.opts.CacheNFATransitionBytes {
		s.mu.Lock()
		oldSize := s.opts.CacheNFATransitionBytes
		s.opts.CacheNFATransitionBytes = newNFACache
		s.mu.Unlock()

		adjustmentsMade = append(adjustmentsMade, fmt.Sprintf("nfa_cache: %d -> %d bytes", oldSize, newNFACache))
		changesMade = true
	}

	// 3. Compaction Tuning (adjust compactor thresholds if needed)
	s.tuneCompactionBehavior(stats)

	// Save changes to disk if any were made
	if changesMade {
		if err := s.saveTuning(); err != nil {
			s.logger.Warn("failed to save autotuning changes", "error", err)
		} else {
			s.logger.Info("autotuning adjustments applied", "changes", adjustmentsMade)
		}
	}

	s.logger.Debug("autotuning analysis completed",
		"adjustments_made", len(adjustmentsMade),
		"changes", adjustmentsMade)
}

// tuneMemtableSize adjusts memtable size based on write patterns and flush frequency
func (s *storeImpl) tuneMemtableSize(stats Stats) int64 {
	const (
		minMemtableSize = 16 * 1024 * 1024  // 16MB minimum
		maxMemtableSize = 512 * 1024 * 1024 // 512MB maximum
	)

	current := s.opts.MemtableTargetBytes

	// High write rate with low latency -> increase memtable size
	if stats.WritesPerSecond > 1000 && stats.LatencyP99 < 100*time.Millisecond {
		newSize := int64(float64(current) * 1.5)
		if newSize <= maxMemtableSize {
			s.logger.Debug("increasing memtable size due to high write rate", "writes_per_sec", stats.WritesPerSecond)
			return newSize
		}
	}

	// High latency or memory pressure -> decrease memtable size
	if stats.LatencyP99 > 500*time.Millisecond || stats.TotalBytes > 10*1024*1024*1024 { // 10GB
		newSize := int64(float64(current) * 0.8)
		if newSize >= minMemtableSize {
			s.logger.Debug("decreasing memtable size due to high latency or memory pressure",
				"p99_latency_ms", stats.LatencyP99.Milliseconds(),
				"total_bytes", stats.TotalBytes)
			return newSize
		}
	}

	// Low write rate -> optimize for memory usage
	if stats.WritesPerSecond < 10 && current > minMemtableSize*2 {
		newSize := int64(float64(current) * 0.9)
		if newSize >= minMemtableSize {
			s.logger.Debug("decreasing memtable size due to low write rate", "writes_per_sec", stats.WritesPerSecond)
			return newSize
		}
	}

	return current
}

// tuneLabelAdvanceCache adjusts label advance cache size based on hit rates
func (s *storeImpl) tuneLabelAdvanceCache(stats Stats) int64 {
	const (
		minCacheSize = 1 * 1024 * 1024  // 1MB minimum
		maxCacheSize = 64 * 1024 * 1024 // 64MB maximum
	)

	current := s.opts.CacheLabelAdvanceBytes
	hitRate := stats.LabelAdvanceHitRate

	// Low hit rate -> increase cache size
	if hitRate < 0.7 && current < maxCacheSize {
		newSize := int64(float64(current) * 1.3)
		if newSize <= maxCacheSize {
			s.logger.Debug("increasing label advance cache due to low hit rate", "hit_rate", hitRate)
			return newSize
		}
	}

	// Very high hit rate -> can reduce cache size slightly
	if hitRate > 0.95 && current > minCacheSize*2 {
		newSize := int64(float64(current) * 0.9)
		if newSize >= minCacheSize {
			s.logger.Debug("decreasing label advance cache due to very high hit rate", "hit_rate", hitRate)
			return newSize
		}
	}

	return current
}

// tuneNFATransitionCache adjusts NFA transition cache size based on hit rates
func (s *storeImpl) tuneNFATransitionCache(stats Stats) int64 {
	const (
		minCacheSize = 2 * 1024 * 1024   // 2MB minimum
		maxCacheSize = 128 * 1024 * 1024 // 128MB maximum
	)

	current := s.opts.CacheNFATransitionBytes
	hitRate := stats.NFATransHitRate

	// Low hit rate and high query load -> increase cache
	if hitRate < 0.8 && stats.QueriesPerSecond > 100 && current < maxCacheSize {
		newSize := int64(float64(current) * 1.4)
		if newSize <= maxCacheSize {
			s.logger.Debug("increasing NFA transition cache due to low hit rate and high query load",
				"hit_rate", hitRate, "queries_per_sec", stats.QueriesPerSecond)
			return newSize
		}
	}

	// Very high hit rate with low query load -> can reduce cache
	if hitRate > 0.98 && stats.QueriesPerSecond < 10 && current > minCacheSize*2 {
		newSize := int64(float64(current) * 0.85)
		if newSize >= minCacheSize {
			s.logger.Debug("decreasing NFA transition cache due to very high hit rate and low query load",
				"hit_rate", hitRate, "queries_per_sec", stats.QueriesPerSecond)
			return newSize
		}
	}

	return current
}

// tuneCompactionBehavior adjusts compaction behavior based on level statistics
func (s *storeImpl) tuneCompactionBehavior(stats Stats) {
	if s.compactor == nil {
		return
	}

	// Check for excessive segments in L0
	l0Segments, hasL0 := stats.SegmentCounts[0]
	if hasL0 && l0Segments > 8 {
		s.logger.Info("detected high L0 segment count - compaction may need tuning",
			"l0_segments", l0Segments)
		// Note: In a more advanced implementation, we could adjust compactor thresholds here
	}

	// Check tombstone ratios
	for level, tombstoneRatio := range stats.TombstoneFractions {
		if tombstoneRatio > 0.3 { // More than 30% tombstones
			s.logger.Info("high tombstone ratio detected",
				"level", level,
				"tombstone_ratio", tombstoneRatio)
			// Note: Could trigger more aggressive compaction for this level
		}
	}

	// Check for size imbalances between levels
	if l0Size, hasL0 := stats.LevelSizes[0]; hasL0 {
		if l1Size, hasL1 := stats.LevelSizes[1]; hasL1 && l0Size > l1Size*2 {
			s.logger.Info("L0 size significantly larger than L1 - may need more aggressive L0->L1 compaction",
				"l0_size", l0Size, "l1_size", l1Size)
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

				// CRITICAL FIX: Check if segment is locked for compaction
				// If locked, another goroutine is actively using this segment
				if s.manifest.IsSegmentLockedForCompaction(segID) {
					s.logger.Debug("rcu skipping segment locked for compaction", "id", segID)
					continue
				}

				// CRITICAL: Check refcount before deletion to prevent race conditions
				// If a reader exists with refcount > 0, segment is still in use (e.g., during compaction)
				s.mu.RLock()
				reader, hasReader := s.readersMap[segID]
				s.mu.RUnlock()

				if hasReader && reader != nil {
					// Check refcount atomically
					refcount := reader.GetRefcount()
					if refcount > 0 {
						// Segment is still in use by active queries or operations
						s.logger.Debug("rcu skipping segment with active refcount", "id", segID, "refcount", refcount)
						continue
					}
					// Refcount is 0, safe to proceed with deletion
					// Remove from readersMap first
					s.mu.Lock()
					delete(s.readersMap, segID)
					s.mu.Unlock()
				}

				// Double-check compaction lock after refcount check (prevents TOCTOU race)
				if s.manifest.IsSegmentLockedForCompaction(segID) {
					s.logger.Debug("rcu skipping segment locked for compaction (second check)", "id", segID)
					continue
				}

				// Check dir mtime as proxy for age
				if info, err := os.Stat(segPath); err == nil {
					if info.ModTime().Unix() < cutoff {
						if err := os.RemoveAll(segPath); err != nil {
							s.logger.Warn("rcu failed to remove segment dir", "id", segID, "error", err)
							atomic.AddUint64(&s.stats.CleanupFailures, 1)
						} else {
							s.logger.Info("rcu removed obsolete segment dir", "id", segID)
							atomic.AddUint64(&s.stats.SegmentsDeleted, 1)
						}
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
		var missingSegmentIDs []uint64
		for _, seg := range active {
			segmentID := seg.ID
			// Load segment metadata
			segDir := filepath.Join(segmentsDir, fmt.Sprintf("%016d", segmentID))
			metadataPath := filepath.Join(segDir, "segment.json")
			metadata, err := segment.LoadFromFile(metadataPath)
			if err != nil {
				// Self-healing: if segment is missing, remove from manifest
				// Use errors.Is and fallback string check for wrapped errors
				isMissingError := errors.Is(err, os.ErrNotExist) ||
					strings.Contains(err.Error(), "no such file or directory")
				if isMissingError {
					s.logger.Warn("segment missing during initial load, will remove from manifest",
						"segment_id", segmentID,
						"path", metadataPath,
						"error", err,
					)
					missingSegmentIDs = append(missingSegmentIDs, segmentID)
					continue
				}
				s.logger.Warn("failed to load segment metadata", "id", segmentID, "error", err)
				continue
			}

			// Also verify index.louds exists to detect partially deleted segments
			loudsPath := filepath.Join(segDir, "index.louds")
			if _, err := os.Stat(loudsPath); os.IsNotExist(err) {
				s.logger.Warn("index.louds missing during initial load (partial segment), will remove from manifest",
					"segment_id", segmentID,
					"path", loudsPath,
				)
				missingSegmentIDs = append(missingSegmentIDs, segmentID)
				continue
			}
			reader, err := segment.NewReaderWithCache(segmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad, s.mmapCache)
			if err != nil {
				// Self-healing: if segment is missing, remove from manifest
				// Use errors.Is and fallback string check for wrapped errors
				isMissingError := errors.Is(err, os.ErrNotExist) ||
					strings.Contains(err.Error(), "no such file or directory")
				if isMissingError {
					s.logger.Warn("segment missing during initial load, will remove from manifest",
						"segment_id", segmentID,
						"error", err,
					)
					missingSegmentIDs = append(missingSegmentIDs, segmentID)
					continue
				}
				s.logger.Warn("failed to create segment reader", "id", segmentID, "error", err)
				continue
			}
			s.segments = append(s.segments, metadata)
			s.readers = append(s.readers, reader)
			s.readersMap[segmentID] = reader // Track for RCU refcount checks
			if segmentID >= s.nextSegmentID {
				s.nextSegmentID = segmentID + 1
			}
		}

		// Remove missing segments from manifest
		if len(missingSegmentIDs) > 0 {
			s.logger.Info("removing missing segments from manifest during initial load",
				"count", len(missingSegmentIDs),
				"segment_ids", missingSegmentIDs,
			)
			if err := s.manifest.RemoveSegments(missingSegmentIDs); err != nil {
				s.logger.Error("failed to remove missing segments from manifest during initial load",
					"error", err,
					"segment_ids", missingSegmentIDs,
				)
				// Don't fail the load - we can continue with loaded segments
			} else {
				s.logger.Info("successfully removed missing segments from manifest during initial load", "count", len(missingSegmentIDs))
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
		reader, err := segment.NewReaderWithCache(segmentID, segmentsDir, s.logger, s.opts.VerifyChecksumsOnLoad, s.mmapCache)
		if err != nil {
			s.logger.Warn("failed to create segment reader", "id", segmentID, "error", err)
			continue
		}

		s.segments = append(s.segments, metadata)
		s.readers = append(s.readers, reader)
		s.readersMap[segmentID] = reader // Track for RCU refcount checks

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

// detectOrphanedSegments detects segments on disk that are not tracked in the manifest.
// This is a safety mechanism to detect data that might have been lost due to manifest update failures.
func (s *storeImpl) detectOrphanedSegments() error {
	if s.manifest == nil {
		return nil // Can't detect orphans without manifest
	}

	segmentsDir := filepath.Join(s.dir, common.DirSegments)
	if _, err := os.Stat(segmentsDir); os.IsNotExist(err) {
		return nil // No segments directory
	}

	// Get segments from manifest
	manifestSegments := s.manifest.GetActiveSegments()
	manifestMap := make(map[uint64]bool)
	for _, seg := range manifestSegments {
		manifestMap[seg.ID] = true
	}

	// Scan filesystem for segment directories
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return fmt.Errorf("read segments directory: %w", err)
	}

	var orphans []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Parse segment ID
		var segmentID uint64
		if _, err := fmt.Sscanf(entry.Name(), "%d", &segmentID); err != nil {
			continue
		}

		// Skip in-progress builds
		segmentPath := filepath.Join(segmentsDir, entry.Name())
		if _, err := os.Stat(filepath.Join(segmentPath, ".building")); err == nil {
			continue
		}

		// Check if segment has metadata file (indicates it's a complete segment)
		if _, err := os.Stat(filepath.Join(segmentPath, "segment.json")); err != nil {
			continue
		}

		// Check if it's in manifest
		if !manifestMap[segmentID] {
			orphans = append(orphans, segmentID)
		}
	}

	if len(orphans) > 0 {
		s.logger.Warn("detected orphaned segments - attempting automatic recovery",
			"orphaned_segments", orphans,
			"count", len(orphans),
			"note", "This indicates a previous flush partially failed. Attempting to recover data.")

		// Attempt automatic recovery by adding orphaned segments to manifest
		recovered, failed := s.recoverOrphanedSegments(orphans, segmentsDir)

		if len(recovered) > 0 {
			s.logger.Info("successfully recovered orphaned segments",
				"recovered_segments", recovered,
				"count", len(recovered))

			// Reload segments to include recovered ones
			s.mu.Lock()
			if err := s.loadSegments(); err != nil {
				s.logger.Error("failed to reload segments after recovery", "error", err)
			} else {
				s.updateStatsFromManifest()
				s.logger.Info("segments reloaded after recovery")
			}
			s.mu.Unlock()
		}

		if len(failed) > 0 {
			s.logger.Error("failed to recover some orphaned segments",
				"failed_segments", failed,
				"count", len(failed),
				"note", "These segments may contain lost data - manual intervention required")
		}
	}

	return nil
}

// recoverOrphanedSegments attempts to recover orphaned segments by adding them to the manifest.
// Returns lists of successfully recovered and failed segment IDs.
func (s *storeImpl) recoverOrphanedSegments(orphanIDs []uint64, segmentsDir string) (recovered []uint64, failed []uint64) {
	var segmentInfos []manifest.SegmentInfo

	for _, segmentID := range orphanIDs {
		segmentPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", segmentID))

		// Load segment metadata to get key information
		metadataPath := filepath.Join(segmentPath, "segment.json")
		metadata, err := segment.LoadFromFile(metadataPath)
		if err != nil {
			s.logger.Warn("failed to load metadata for orphaned segment", "id", segmentID, "error", err)
			failed = append(failed, segmentID)
			continue
		}

		// Create segment info for manifest
		info := manifest.SegmentInfo{
			ID:      segmentID,
			Level:   common.LevelL0, // Assume L0 for recovered segments
			NumKeys: metadata.Counts.Accepted,
			Size:    s.computeSegmentSize(segmentID),
		}

		// Add min/max key info if available
		if minb, err := metadata.GetMinKey(); err == nil {
			info.MinKeyHex = fmt.Sprintf("%x", minb)
		}
		if maxb, err := metadata.GetMaxKey(); err == nil {
			info.MaxKeyHex = fmt.Sprintf("%x", maxb)
		}

		segmentInfos = append(segmentInfos, info)

		s.logger.Info("prepared orphaned segment for recovery",
			"id", segmentID,
			"keys", metadata.Counts.Accepted,
			"size", info.Size)
	}

	if len(segmentInfos) == 0 {
		s.logger.Warn("no orphaned segments could be prepared for recovery")
		return nil, orphanIDs
	}

	// Atomically add all recovered segments to manifest
	s.logger.Info("adding recovered segments to manifest", "count", len(segmentInfos))
	if err := s.manifest.AddSegments(segmentInfos); err != nil {
		s.logger.Error("failed to add recovered segments to manifest", "error", err)
		// If manifest update fails, all segments are considered failed
		return nil, orphanIDs
	}

	// All segments were recovered successfully
	for _, info := range segmentInfos {
		recovered = append(recovered, info.ID)
	}

	s.logger.Info("orphaned segments recovery completed",
		"recovered", len(recovered),
		"total_keys_recovered", func() uint64 {
			var total uint64
			for _, info := range segmentInfos {
				total += info.NumKeys
			}
			return total
		}())

	return recovered, failed
}

// selfHealMissingSegments scans manifest for segments that don't exist on disk
// and removes them from the manifest. This is a recovery mechanism for
// handling race conditions or unexpected segment deletions.
func (s *storeImpl) selfHealMissingSegments() error {
	if s.manifest == nil {
		return fmt.Errorf("manifest not initialized")
	}

	active := s.manifest.GetActiveSegments()
	segmentsDir := filepath.Join(s.dir, common.DirSegments)

	var missingIDs []uint64
	for _, seg := range active {
		segPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", seg.ID), "segment.json")
		if _, err := os.Stat(segPath); os.IsNotExist(err) {
			s.logger.Warn("self-healing: found segment in manifest but missing on disk",
				"segment_id", seg.ID,
				"level", seg.Level,
				"path", segPath,
			)
			missingIDs = append(missingIDs, seg.ID)
		}
	}

	if len(missingIDs) == 0 {
		s.logger.Info("self-healing: no missing segments found")
		return nil
	}

	s.logger.Info("self-healing: removing missing segments from manifest",
		"count", len(missingIDs),
		"segment_ids", missingIDs,
	)

	if err := s.manifest.RemoveSegments(missingIDs); err != nil {
		return fmt.Errorf("self-healing: failed to remove segments from manifest: %w", err)
	}

	s.logger.Info("self-healing: successfully removed missing segments",
		"count", len(missingIDs),
	)

	// Reload segments to update in-memory state
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove readers for missing segments
	newReaders := make([]*segment.Reader, 0, len(s.readers))
	newSegments := make([]*segment.Metadata, 0, len(s.segments))

	missingMap := make(map[uint64]bool, len(missingIDs))
	for _, id := range missingIDs {
		missingMap[id] = true
	}

	for i, r := range s.readers {
		if !missingMap[r.GetSegmentID()] {
			newReaders = append(newReaders, r)
			if i < len(s.segments) {
				newSegments = append(newSegments, s.segments[i])
			}
		} else {
			// Close and remove reader for missing segment
			r.Close()
			delete(s.readersMap, r.GetSegmentID())
		}
	}

	s.readers = newReaders
	s.segments = newSegments
	s.updateStatsFromManifest()

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

// PauseBackgroundCompaction pauses background compaction and waits for any ongoing
// Execute() call to return or until ctx is done.
func (s *storeImpl) PauseBackgroundCompaction(ctx context.Context) error {
	atomic.AddInt32(&s.compactionPauseCount, 1)
	// Best-effort: wait for compaction loop to observe pause and drain in-flight Execute.
	// We don't have a dedicated in-flight counter; poll briefly.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Heuristic sleep; compaction loop ticks every 10s, but Execute runs synchronously.
		time.Sleep(50 * time.Millisecond)
		// Nothing else to check synchronously.
	}
	s.logger.Info("background compaction paused")
	return nil
}

// ResumeBackgroundCompaction resumes background compaction if paused.
func (s *storeImpl) ResumeBackgroundCompaction() {
	if atomic.AddInt32(&s.compactionPauseCount, -1) < 0 {
		atomic.StoreInt32(&s.compactionPauseCount, 0)
	}
	s.logger.Info("background compaction resumed")
}

// SetAsyncFilterBuild toggles asynchronous filter building at runtime.
func (s *storeImpl) SetAsyncFilterBuild(enabled bool) {
	s.mu.Lock()
	s.opts.AsyncFilterBuild = enabled
	s.mu.Unlock()
	s.logger.Info("async filter build updated", "enabled", enabled)
}
