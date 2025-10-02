// Package srad implements a string storage engine with regex search support,
// LSM architecture, immutable segments, WAL durability, parallel queries,
// RCU without downtime, autotuning, and monitoring.
package srad

import (
	"context"
	"regexp"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
)

// Store is the main interface for the SRAD storage engine.
type Store interface {
	// Close closes the store and releases all resources.
	Close() error

	// Insert adds a string to the store.
	Insert(s []byte) error

	// Delete removes a string from the store.
	Delete(s []byte) error

	// InsertWithTTL inserts a string with expiration time
	InsertWithTTL(s []byte, ttl time.Duration) error

	// RegexSearch performs a regex search on the store.
	RegexSearch(ctx context.Context, re *regexp.Regexp, q *QueryOptions) (Iterator, error)

	// PrefixScan returns an iterator over keys starting with the given prefix.
	PrefixScan(ctx context.Context, prefix []byte, q *QueryOptions) (Iterator, error)

	// RangeScan returns an iterator over keys in [start, end) lexicographic range.
	// If end is nil or empty, iteration continues to the end.
	RangeScan(ctx context.Context, start, end []byte, q *QueryOptions) (Iterator, error)

	// Stats returns current statistics for the store.
	Stats() Stats

	// RefreshStats forces a refresh of statistics.
	RefreshStats()

	// Tune adjusts tuning parameters at runtime.
	Tune(params TuningParams)

	// CompactNow triggers immediate compaction.
	CompactNow(ctx context.Context) error

	// Flush forces a flush of the memtable to disk.
	Flush(ctx context.Context) error

	// RCUEnabled returns whether RCU is enabled.
	RCUEnabled() bool

	// AdvanceRCU advances the RCU epoch.
	AdvanceRCU(ctx context.Context) error

	// VacuumPrefix removes all keys with the specified prefix.
	VacuumPrefix(ctx context.Context, prefix []byte) error

	// SetAutotunerEnabled enables or disables the autotuner.
	SetAutotunerEnabled(enabled bool)

	// PruneWAL deletes obsolete WAL files that are older than the current WAL sequence.
	// Effective primarily when WAL rotation is enabled on flush; otherwise it only
	// removes fully older WAL files, never truncating the current one.
	PruneWAL() error

	// PurgeObsoleteSegments removes non-active segment directories immediately (dangerous).
	// This bypasses the RCU grace period and should be used only in maintenance tools
	// or tests when no readers are using old segments.
	PurgeObsoleteSegments() error

	// PauseBackgroundCompaction pauses background compaction and waits until any
	// in-progress compaction finishes or ctx is done.
	PauseBackgroundCompaction(ctx context.Context) error

	// ResumeBackgroundCompaction resumes background compaction if previously paused.
	ResumeBackgroundCompaction()

	// SetAsyncFilterBuild toggles asynchronous filter building at runtime.
	// When disabled, filters are built inline during Flush/Compact.
	SetAsyncFilterBuild(enabled bool)

	// RebuildMissingFilters synchronously rebuilds missing Bloom/Trigram filters
	// for all active segments. Blocks until completion.
	RebuildMissingFilters(ctx context.Context) error
}

// Options configures the store behavior.
type Options struct {
	// ReadOnly opens the store in read-only mode.
	ReadOnly bool

	// Parallelism sets the maximum number of parallel operations.
	Parallelism int

	// VerifyChecksumsOnLoad enables CRC verification when loading segments.
	VerifyChecksumsOnLoad bool

	// MemtableTargetBytes sets the target size for the memtable before flushing.
	MemtableTargetBytes int64

	// CacheLabelAdvanceBytes sets the cache size for label advance operations.
	CacheLabelAdvanceBytes int64

	// CacheNFATransitionBytes sets the cache size for NFA transitions.
	CacheNFATransitionBytes int64

	// EnableRCU enables Read-Copy-Update for lock-free reads.
	EnableRCU bool

	// RCUCleanupInterval sets the interval for RCU cleanup.
	RCUCleanupInterval time.Duration

	// Logger provides structured logging.
	Logger common.Logger

	// DisableAutotuner disables automatic tuning.
	DisableAutotuner bool

	// DisableAutoFlush disables automatic flushing.
	DisableAutoFlush bool

	// DefaultTTL sets the default TTL for keys without explicit TTL
	DefaultTTL time.Duration

	// DisableBackgroundCompaction disables background compaction tasks.
	DisableBackgroundCompaction bool

	// RotateWALOnFlush forces WAL rotation after each flush when true.
	RotateWALOnFlush bool

	// WALRotateSize overrides the default WAL rotation size in bytes (0 = default).
	WALRotateSize int64

	// WALMaxFileSize overrides the maximum WAL file size in bytes (0 = default).
	WALMaxFileSize int64

	// WALBufferSize overrides the WAL write buffer size in bytes (0 = default).
	WALBufferSize int

	// WALSyncOnEveryWrite forces fsync on each WAL write (safest, slowest).
	WALSyncOnEveryWrite bool
	// WALFlushOnEveryWrite forces buffer flush on each WAL write (safer, faster than sync).
	WALFlushOnEveryWrite bool
	// WALFlushEveryBytes triggers buffer flush after approximately this many bytes written (0 = disabled).
	WALFlushEveryBytes int

	// WALFlushEveryInterval triggers time-based flush at this interval (0 = disabled).
	// When >0, a background ticker flushes the WAL buffer periodically even if
	// WALFlushEveryBytes threshold is not reached. This reduces RPO at the cost of more flushes.
	WALFlushEveryInterval time.Duration

	// PrefixBloomFPR sets Bloom filter target false positive rate (0 => default).
	PrefixBloomFPR float64
	// PrefixBloomMaxPrefixLen limits prefix length added to Bloom (0 => default).
	PrefixBloomMaxPrefixLen int
	// EnableTrigramFilter toggles trigram filter generation (default: true).
	EnableTrigramFilter bool

	// Build parallelization knobs (0 => auto defaults)
	BuildMaxShards       int // cap parallel shards for builder tasks (Bloom/Trigram)
	BuildShardMinKeys    int // below this key count, disable sharding (use 1 shard)
	BloomAdaptiveMinKeys int // above this, reduce Bloom prefix work

	// Range-partitioned build (1 = disabled)
	BuildRangePartitions int
	// AsyncFilterBuild enables background filter building after flush/compact.
	// When true, filters (Bloom/Trigram) are built asynchronously in background,
	// shortening the flush critical path. Queries remain correct but may scan more
	// segments until filters are ready (typically < 1 second for most workloads).
	// When false, filters are built inline during flush/compact (deterministic, but slower).
	// Default: true (recommended for high-write workloads and bulk imports)
	AsyncFilterBuild bool

	// ForceTrieBuild forces building a trie even when input is sorted and
	// streaming LOUDS path is available. Useful for benchmarks and debugging.
	ForceTrieBuild bool

	// DisableLOUDSBuild disables building LOUDS index during flush. Readers will
	// fall back to keys.dat streaming and filters. Exact lookups will use keys.dat.
	DisableLOUDSBuild bool

	// AutoDisableLOUDSMinKeys: when >0 and number of keys >= this threshold, the
	// builder will skip LOUDS regardless of DisableLOUDSBuild.
	AutoDisableLOUDSMinKeys int

	// GCPercentDuringTrie sets temporary runtime GC percent during trie build (0 = unchanged).
	// Example: set to a large number (e.g., 500-1000) to reduce GC frequency while building huge tries.
	GCPercentDuringTrie int
}

// QueryOptions configures query execution.
type QueryOptions struct {
	// Limit sets the maximum number of results to return.
	Limit int

	// Mode determines what information is returned.
	Mode QueryMode

	// Order determines the traversal order.
	Order TraversalOrder

	// MaxParallelism sets the maximum parallel segments to query.
	MaxParallelism int

	// FilterThreshold sets the selectivity threshold for using filters.
	FilterThreshold float64

	// CachePolicy determines caching behavior during queries.
	CachePolicy CachePolicy
}

// QueryMode determines what information is returned by queries.
type QueryMode int

const (
	// CountOnly returns only the count of matches.
	CountOnly QueryMode = iota
	// EmitIDs returns match IDs.
	EmitIDs
	// EmitStrings returns the actual strings.
	EmitStrings
)

// TraversalOrder determines the order of traversal.
type TraversalOrder int

const (
	// DFS uses depth-first search.
	DFS TraversalOrder = iota
	// BFS uses breadth-first search.
	BFS
	// Auto automatically selects the best order.
	Auto
)

// CachePolicy determines caching behavior.
type CachePolicy int

const (
	// CachePolicyDefault uses default caching.
	CachePolicyDefault CachePolicy = iota
	// CachePolicyAggressive aggressively caches data.
	CachePolicyAggressive
	// CachePolicyMinimal minimizes caching.
	CachePolicyMinimal
	// CachePolicyNone disables caching.
	CachePolicyNone
)

// Iterator provides access to query results.
type Iterator interface {
	// Next advances to the next result.
	Next(ctx context.Context) bool

	// Err returns any error encountered during iteration.
	Err() error

	// ID returns the global ID of the current result.
	// Format: (segmentID << 32) | nodeID
	ID() uint64

	// String returns the current string value.
	String() []byte

	// Op returns the operation type (insert/delete).
	Op() uint8

	// Close releases resources associated with the iterator.
	Close() error
}

// Stats contains store statistics.
type Stats struct {
	// LevelSizes maps level number to total bytes.
	LevelSizes map[int]int64

	// SegmentCounts maps level number to segment count.
	SegmentCounts map[int]int

	// TombstoneFractions maps level to fraction of tombstones.
	TombstoneFractions map[int]float64

	// TotalBytes is the total size of all data.
	TotalBytes int64

	// LatencyP50 is the 50th percentile latency.
	LatencyP50 time.Duration

	// LatencyP95 is the 95th percentile latency.
	LatencyP95 time.Duration

	// LatencyP99 is the 99th percentile latency.
	LatencyP99 time.Duration

	// QueriesPerSecond is the current query rate.
	QueriesPerSecond float64

	// WritesPerSecond is the current write rate.
	WritesPerSecond float64

	// TotalInserts is the cumulative number of inserts since start.
	TotalInserts uint64

	// TotalDeletes is the cumulative number of deletes since start.
	TotalDeletes uint64

	// TotalSearches is the cumulative number of searches since start.
	TotalSearches uint64

	// OverallQueriesPerSecond is the average query rate since start.
	OverallQueriesPerSecond float64

	// OverallWritesPerSecond is the average write rate since start.
	OverallWritesPerSecond float64

	// LabelAdvanceHitRate is the cache hit rate for label advances.
	LabelAdvanceHitRate float64

	// NFATransHitRate is the cache hit rate for NFA transitions.
	NFATransHitRate float64

	// PrefixBloomFPR is the false positive rate for prefix bloom filters.
	PrefixBloomFPR float64

	// TrigramSkipRatio is the ratio of segments skipped by trigram filters.
	TrigramSkipRatio float64

	// ManifestGeneration is the current manifest generation.
	ManifestGeneration uint64

	// CleanupFailures is the number of failed cleanup operations.
	CleanupFailures uint64

	// SegmentsDeleted is the number of successfully deleted segments.
	SegmentsDeleted uint64

	// WALDeleteFailed is the number of failed WAL file deletions.
	WALDeleteFailed uint64
}

// TuningParams contains tunable parameters.
type TuningParams struct {
	// MemtableTargetBytes sets the target memtable size.
	MemtableTargetBytes *int64

	// CacheLabelAdvanceBytes sets the label advance cache size.
	CacheLabelAdvanceBytes *int64

	// CacheNFATransitionBytes sets the NFA transition cache size.
	CacheNFATransitionBytes *int64

	// CompactionPriority sets the compaction priority.
	CompactionPriority *int
}

// DefaultOptions returns default store options.
func DefaultOptions() *Options {
	return &Options{
		ReadOnly:                    false,
		Parallelism:                 4,
		VerifyChecksumsOnLoad:       false,
		MemtableTargetBytes:         common.DefaultMemtableTargetBytes,
		CacheLabelAdvanceBytes:      common.DefaultCacheLabelAdvanceBytes,
		CacheNFATransitionBytes:     common.DefaultCacheNFATransitionBytes,
		EnableRCU:                   true,
		RCUCleanupInterval:          30 * time.Second,
		Logger:                      NewDefaultLogger(),
		DisableAutotuner:            false,
		DisableAutoFlush:            false,
		DefaultTTL:                  common.DefaultTTL,
		DisableBackgroundCompaction: false,
		RotateWALOnFlush:            false,
		WALRotateSize:               0,
		WALMaxFileSize:              0,
		WALBufferSize:               0,
		WALSyncOnEveryWrite:         false,
		WALFlushOnEveryWrite:        false,
		WALFlushEveryBytes:          int(common.WALBufferSize),
		WALFlushEveryInterval:       0,
		PrefixBloomFPR:              common.DefaultBloomFPR,
		PrefixBloomMaxPrefixLen:     int(common.DefaultPrefixBloomLength),
		EnableTrigramFilter:         true,
		BuildMaxShards:              8,
		BuildShardMinKeys:           200000,
		BloomAdaptiveMinKeys:        10000000,
		BuildRangePartitions:        1,
		AsyncFilterBuild:            true, // Enable by default: faster flush, filters built in background
		ForceTrieBuild:              false,
		DisableLOUDSBuild:           false,
		AutoDisableLOUDSMinKeys:     0,
	}
}

// DefaultQueryOptions returns default query options.
func DefaultQueryOptions() *QueryOptions {
	return &QueryOptions{
		Limit:           0, // unlimited
		Mode:            EmitStrings,
		Order:           Auto,
		MaxParallelism:  4,
		FilterThreshold: 0.1,
		CachePolicy:     CachePolicyDefault,
	}
}
