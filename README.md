# SRAD - String Storage with Regex Search

SRAD is a high-performance string storage engine with regex search support, implementing LSM-tree architecture, immutable segments, WAL durability, parallel queries, RCU without downtime, tuning hooks, and lightweight statistics.

## Features

- **LSM-tree Architecture**: Efficient write performance with background compaction
- **Regex Search**: Full regex support using Go's regexp package
- **WAL Durability**: Write-ahead logging for crash recovery
- **Immutable Segments**: Once written, segments are never modified
- **Parallel Queries**: Concurrent segment searching with configurable parallelism
- **RCU Support**: Read-Copy-Update for lock-free reads during updates
- **Tuning**: Manual tuning via APIs with persisted `tuning.json`
- **Statistics**: Lightweight stats via API (no Prometheus export)

## Installation

```bash
go get github.com/CVDpl/go-live-srad
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "regexp"
    
    "github.com/CVDpl/go-live-srad/pkg/srad"
)

func main() {
    // Open a store
    opts := srad.DefaultOptions()
    store, err := srad.Open("./mystore", opts)
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()
    
    // Insert data
    store.Insert([]byte("apple"))
    store.Insert([]byte("application"))
    store.Insert([]byte("banana"))
    
    // Search with regex
    re := regexp.MustCompile("^app.*")
    iter, err := store.RegexSearch(context.Background(), re, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer iter.Close()
    
    // Iterate results
    for iter.Next(context.Background()) {
        fmt.Printf("Found: %s\n", iter.String())
    }
}
```

## API Reference

### Store Interface

```go
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
    // Bypasses the RCU grace period; only for maintenance when no readers use old segments.
    PurgeObsoleteSegments() error

    // PauseBackgroundCompaction pauses background compaction and waits for any in-flight
    // compaction cycle to drain (best-effort), or until ctx is done.
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
```

### Options

```go
type Options struct {
    // ReadOnly opens the store in read-only mode.
    // Default: false
    ReadOnly bool

    // Parallelism sets the maximum number of parallel operations.
    // Default: 4
    Parallelism int

    // VerifyChecksumsOnLoad enables CRC verification when loading segments.
    // Default: false (enable in maintenance tools)
    VerifyChecksumsOnLoad bool

    // MemtableTargetBytes sets the target size for the memtable before flushing.
    // Default: 512 MiB
    MemtableTargetBytes int64

    // Cache sizes for query engine caches.
    // Default: 32 MiB each
    CacheLabelAdvanceBytes  int64
    CacheNFATransitionBytes int64

    // EnableRCU enables Read‑Copy‑Update for lock‑free reads.
    // Default: true
    EnableRCU bool

    // RCUCleanupInterval sets the interval for RCU cleanup.
    // Default: 30s
    RCUCleanupInterval time.Duration

    // Logger provides structured logging.
    // Default: JSON logger to stderr (NewDefaultLogger)
    Logger Logger

    // DisableAutotuner disables automatic tuning.
    // Default: false
    DisableAutotuner bool

    // DisableAutoFlush disables periodic background flush.
    // Default: false
    DisableAutoFlush bool

    // DefaultTTL sets default time‑to‑live for inserted keys (0 = never expire).
    // Default: 0
    DefaultTTL time.Duration

    // DisableBackgroundCompaction disables background LSM compaction.
    // Default: false
    DisableBackgroundCompaction bool

    // RotateWALOnFlush rotates WAL after each flush.
    // Default: false
    RotateWALOnFlush bool

    // WALRotateSize overrides rotation size (bytes).
    // Default: 0 => use built‑in default (128 MiB)
    WALRotateSize int64

    // WALMaxFileSize overrides max single WAL size (bytes).
    // Default: 0 => use built‑in default (1 GiB)
    WALMaxFileSize int64

    // WALBufferSize overrides WAL write buffer size (bytes).
    // Default: 0 => use built‑in default (256 KiB)
    WALBufferSize int

    // Durability knobs:
    // WALSyncOnEveryWrite: fsync after each write (safest, slowest). Default: false
    // WALFlushOnEveryWrite: flush userspace buffer after each write. Default: false
    // WALFlushEveryBytes: flush after approx N bytes. Default: WALBufferSize
    WALSyncOnEveryWrite  bool
    WALFlushOnEveryWrite bool
    WALFlushEveryBytes   int
    WALFlushEveryInterval time.Duration

    // Prefix Bloom filter controls.
    // PrefixBloomFPR: target false‑positive rate. Default: 0.01
    // PrefixBloomMaxPrefixLen: max prefix length to add. Default: 16
    // EnableTrigramFilter: build trigram filter. Default: true
    PrefixBloomFPR          float64
    PrefixBloomMaxPrefixLen int
    EnableTrigramFilter     bool

    // Build parallelization (0 => auto defaults).
    // BuildMaxShards: cap internal shards per build task. Default: 8
    // BuildShardMinKeys: below this, use 1 shard. Default: 200_000
    // BloomAdaptiveMinKeys: above this, reduce Bloom prefix work. Default: 10_000_000
    BuildMaxShards       int
    BuildShardMinKeys    int
    BloomAdaptiveMinKeys int

    // Range‑partitioned build (1 = disabled). Default: 1
    BuildRangePartitions int

    // AsyncFilterBuild: when true, skip inline filter build; build later in background.
    // Default: false
    AsyncFilterBuild bool

    // ForceTrieBuild forces trie build even for sorted inputs (for benchmarks/tests).
    // Default: false
    ForceTrieBuild bool

    // DisableLOUDSBuild disables LOUDS index build (Readers use keys.dat + filters).
    // Default: false
    DisableLOUDSBuild bool

    // AutoDisableLOUDSMinKeys: when >0 and number of keys >= threshold, skip LOUDS.
    // Default: 0 (disabled)
    AutoDisableLOUDSMinKeys int

    // GCPercentDuringTrie sets temporary runtime GC percent during trie build (0 = unchanged).
    // Default: 0
    GCPercentDuringTrie int
}
```

### QueryOptions

```go
type QueryOptions struct {
    Limit           int           // max results (0 = unlimited)
    Mode            QueryMode     // CountOnly | EmitIDs | EmitStrings
    Order           TraversalOrder// DFS | BFS | Auto
    MaxParallelism  int           // max segments to search in parallel
    FilterThreshold float64       // selectivity threshold for using filters
    CachePolicy     CachePolicy   // Default | Aggressive | Minimal | None
}
```

### Examples

Example tuning for building large segments:
```go
opts := srad.DefaultOptions()
opts.BuildMaxShards = 8          // limit concurrent shards
opts.BuildShardMinKeys = 300000  // below threshold, do not shard
opts.BloomAdaptiveMinKeys = 8_000_000 // reduce Bloom prefixes earlier
```

Example toggling LOUDS for bulk imports:
```go
opts := srad.DefaultOptions()
opts.DisableLOUDSBuild = true              // skip LOUDS entirely
opts.AsyncFilterBuild = true               // filters in background
opts.EnableTrigramFilter = false           // optional
store, _ := srad.Open(dir, opts)
// ... bulk load ...
store.Flush(ctx)
// Optionally force filters to be present before shutdown/cleanup
_ = store.RebuildMissingFilters(ctx)
// Re-enable LOUDS for subsequent smaller batches if needed
```

Example auto skip example (skip LOUDS only for huge builds):
```go
opts := srad.DefaultOptions()
opts.AutoDisableLOUDSMinKeys = 50_000_000  // 50M+ keys => skip LOUDS
```

### Filters

- Prefix Bloom: controls FPR and the maximum prefix length added to the filter.
- Trigram: can be disabled for very large batches or when you do not use substring searches.

Example:
```go
opts := srad.DefaultOptions()
opts.PrefixBloomFPR = 0.02            // faster build, smaller filter
opts.PrefixBloomMaxPrefixLen = 8      // less work per key
opts.EnableTrigramFilter = false      // skip tri.bits
store, _ := srad.Open(dir, opts)
```

### Flush/Compaction parallelism (range partitions)

- `BuildRangePartitions` (>1) splits the key range into N partitions (heuristic by first byte) and builds N segments in parallel during Flush/Compact.
- Combined with `BuildMaxShards`, this fully utilizes the CPU (N partitions × internal shards).

Batch example:
```go
opts := srad.DefaultOptions()
opts.DisableAutotuner = true
opts.MemtableTargetBytes = 256 * 1024 * 1024
opts.EnableTrigramFilter = false
opts.PrefixBloomFPR = 0.03
opts.PrefixBloomMaxPrefixLen = 8
opts.BuildMaxShards = runtime.NumCPU()
opts.BuildRangePartitions = runtime.NumCPU()/2 // e.g., 8–16
```

- Fallback: when keys are highly clustered (e.g., many share a long common prefix) the range bucketing by first byte can collapse to a single non-empty bucket. When this happens and `BuildRangePartitions > 1`, SRAD automatically switches to hash-based partitioning (FNV-1a) across the same number of parts to preserve parallelism. You will see logs like:

```text
flush using hash-based partitioning
compaction using hash-based partitioning
```
This fallback only affects how work is split during Flush/Compaction. It does not change query behavior or on-disk key order within a segment.

### AsyncFilterBuild

- `AsyncFilterBuild = true` enables building missing filters (`filters/prefix.bf`, `filters/tri.bits`) in the background after flush/compaction.
- When enabled, the builder skips inline filter generation during Flush/Compact to shorten the critical path; filters are produced shortly after by a background task that scans active segments.
- You can also trigger a blocking rebuild at any time via `store.RebuildMissingFilters(ctx)` (e.g., right before `Close()` or `PurgeObsoleteSegments()`), which rebuilds any missing filters for active segments and returns when done. During this call, background compaction is temporarily paused and automatically resumed at the end to avoid churn on active segment sets while filters are being created.
- Queries are correct even without filters; filters only accelerate reads.

```go
opts := srad.DefaultOptions()
opts.AsyncFilterBuild = true // skip inline filter build, do it in background
```

Behavior and guidance:

- Inline filter build is entirely skipped during Flush/Compact when `AsyncFilterBuild` is true. Background filter build starts after segments are persisted and added to the manifest.
- If `EnableTrigramFilter` is false, only `prefix.bf` is produced (less CPU and disk).
- For bulk imports, combine with range partitions and higher shard counts to maximize CPU and reduce elapsed time of the critical path.

Bulk import example:

```go
opts := srad.DefaultOptions()
opts.DisableBackgroundCompaction = true // compact later, outside critical path
opts.AsyncFilterBuild = true             // filters in background, not inline
opts.EnableTrigramFilter = false         // optional: less CPU during filter build
opts.PrefixBloomFPR = 0.03
opts.PrefixBloomMaxPrefixLen = 8
opts.BuildRangePartitions = 12           // or runtime.NumCPU()/2
opts.BuildMaxShards = runtime.NumCPU()
store, _ := srad.Open(dir, opts)
// ... bulk load ...
store.Flush(ctx)       // writes segments fast; filters will follow asynchronously
// If you need filters now (before close/purge), run a blocking rebuild:
_ = store.RebuildMissingFilters(ctx)
// Optionally run compaction later, then allow background filter builder to finish
```

Pausing compaction explicitly for maintenance windows:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
defer cancel()
if err := store.PauseBackgroundCompaction(ctx); err != nil {
    // handle pause error (e.g., context deadline)
}
defer store.ResumeBackgroundCompaction()

// Perform maintenance: rebuild filters, vacuum prefixes, etc.
_ = store.RebuildMissingFilters(ctx)
// ... other tasks ...
```

Nested pauses: SRAD uses a pause counter; compaction is paused while the counter > 0. Calls to `RebuildMissingFilters` increment/decrement the counter around the rebuild but do not resume compaction if you paused it earlier (i.e., an outer pause keeps compaction paused until you call `ResumeBackgroundCompaction` the same number of times).

Runtime toggle for AsyncFilterBuild:

```go
// Build filters inline during maintenance
store.SetAsyncFilterBuild(false)
// ... Flush/Compact/RebuildMissingFilters ...
// Return to background filter build afterwards (optional)
store.SetAsyncFilterBuild(true)
```

Example maintenance/conversion flow:

```go
ctx := context.Background()
_ = store.PauseBackgroundCompaction(ctx)
defer store.ResumeBackgroundCompaction()

store.SetAsyncFilterBuild(false)              // build filters inline
_ = store.Flush(ctx)                          // persist
_ = store.CompactNow(ctx)                     // optional: compact immediately
_ = store.RebuildMissingFilters(ctx)          // ensure filters present
_ = store.PruneWAL()                          // drop old WAL files
_ = store.AdvanceRCU(ctx)                     // advance RCU epoch
_ = store.PurgeObsoleteSegments()             // remove obsolete segment dirs (dangerous)
store.SetAsyncFilterBuild(true)               // optional: back to async
_ = store.Close()
```

Pausing compaction without a deadline:

```go
// No deadline: Pause waits best-effort for in-flight cycles to drain
if err := store.PauseBackgroundCompaction(context.Background()); err != nil {
    // handle unexpected error
}
defer store.ResumeBackgroundCompaction()

// Maintenance work...
```

### Flush behavior

- Flush uses a freeze-and-swap strategy: first swap the `memtable` for a new one under a short lock, then build the segment from the frozen copy outside the lock. New inserts are minimally blocked and never lost from view.
- Artifact construction is parallelized: `keys.dat` and the LOUDS/edges/tails indexes are built in goroutines; filters are built inline only when `AsyncFilterBuild` is disabled (otherwise they are produced in the background after Flush/Compact).

### Query Options

```go
type QueryOptions struct {
    Limit           int
    Mode            QueryMode
    Order           TraversalOrder
    MaxParallelism  int
    FilterThreshold float64
    CachePolicy     CachePolicy
}
```

## Architecture

### Storage Layout

```
store_directory/
├── wal/                    # Write-ahead log files
│   ├── 1.log
│   └── 2.log
├── segments/               # Immutable segment files
│   ├── <segmentID>/
│   │   ├── index.louds    # LOUDS-encoded trie
│   │   ├── segment.json   # Segment metadata
│   │   └── filters/       # Bloom and trigram filters
│   │       ├── prefix.bf
│   │       └── tri.bits
├── manifest/              # Manifest files
│   └── <generation>
├── CURRENT               # Points to current manifest
└── tuning.json          # Tuning parameters
```

### Components

1. **WAL (Write-Ahead Log)**: Ensures durability by logging all operations before applying them
2. **Memtable**: In-memory Patricia tree for recent writes
3. **Segments**: Immutable on-disk structures with LOUDS-encoded tries
4. **Manifest**: Tracks all segments and their metadata
5. **Compactor**: Merges segments to maintain read performance
6. **Query Engine**: Regex via Go's regexp with literal-prefix and NFA prefix pruning; advanced NFA automaton product implemented

## Performance

- **Inserts**: 100k+ ops/sec on modern hardware
- **Queries**: P95 < 10ms for typical regex patterns
- **Memory**: Configurable cache sizes and memtable limits
- **Parallelism**: Automatic scaling based on CPU cores

## Configuration

### Default Values

- Memtable Target: 512MB
- Cache Label Advance: 32MB
- Cache NFA Transition: 32MB
- RCU Cleanup Interval: 30s
- Compaction Priority: 3
- WAL Rotation Size: 128MB
- WAL Max File Size: 1GB
- WAL Buffer Size: 256KB

Note on WAL lifecycle:
- WAL pruning is manual via `PruneWAL()` and does not run automatically after flush/compaction.
- Set `RotateWALOnFlush` to `true` to rotate WAL after each flush so older WAL files become immediately eligible for pruning.
- `PruneWAL()` only deletes WAL files with sequence number lower than the current active file; the active file is never truncated.

### Logging

By default, SRAD uses a JSON structured logger to stderr. To disable logging entirely, set a null logger in options before opening the store:

```go
opts := srad.DefaultOptions()
opts.Logger = srad.NewNullLogger() // disables all logs
store, err := srad.Open("./mystore", opts)
```

You can also provide your own implementation of the `Logger` interface if you prefer a different format.

### Tuning Parameters

```go
params := srad.TuningParams{
    MemtableTargetBytes:     &memSize,
    CacheLabelAdvanceBytes:  &cacheSize,
    CacheNFATransitionBytes: &nfaCacheSize,
    CompactionPriority:      &priority,
}
store.Tune(params)
```

## Development Status

### ✅ Core Features (Completed)
- **Storage Engine**: Full LSM-tree with immutable segments
- **WAL**: Write-Ahead Log with rotation and crash recovery
- **Memtable**: Canonical Patricia tree with regex support
- **Segments**: LOUDS encoding with bitvector and rank/select operations
- **Manifest**: Atomic updates and version history management
- **Compaction**: Background LSM compaction with leveled strategy
- **Concurrency**: RCU (Read-Copy-Update) for lock-free reads
- **Filters**: Bloom filters for prefix queries and trigram substring filter
- **Persistence**: Segment persistence and recovery
- **Performance**: Parallel search with NFA prefix pruning and trigram-based pruning

### ✅ API & Utilities (Completed)
- **Store Interface**: Complete public API with context support
- **Iterators**: Merged iterator for memtable + segments; Prefix and Range scans
- **Statistics**: Performance metrics and monitoring
- **Examples**: Basic, large dataset, and advanced usage examples
- **Testing**: Comprehensive test suite with benchmarks
- **TTL**: Expiration on read; permanent removal during compaction

### Architecture Highlights
- **LSM-tree**: Log-structured merge tree with multiple levels
- **Immutable segments**: Once written, segments are never modified
- **Atomic updates**: Manifest ensures consistency during compaction
- **Lock-free reads**: RCU allows concurrent reads without blocking
- **Crash recovery**: WAL ensures durability, manifest tracks segments

## Testing

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./pkg/srad

# Run specific test
go test -v -run TestStoreBasicOperations ./pkg/srad

# Run benchmarks
go test -bench=. ./pkg/srad
```

### Verify compaction size reduction
Use the helper at `cmd/compaction_check` to measure live keys and segment sizes before/after compaction.

```bash
# Default: n=10000 inserts, m=5000 deletes, k=0 overwrites
go run ./cmd/compaction_check

# Custom scenario: 10k inserts, 5k deletes, 2k overwrites among survivors
go run ./cmd/compaction_check -n 10000 -m 5000 -k 2000
```

- **n**: initial inserts
- **m**: deletes (from the first m keys)
- **k**: overwrites (re-insert among surviving keys)

The program prints live keys and active size from the manifest before and after compaction. Old segment directories may
still exist briefly due to RCU grace period; you can pass `-wait_rcu 40s` to wait, or `-purge` to delete obsolete
segment directories immediately (dangerous). Use `-verify` to do a full scan for live counts (slow), `-keep` to keep the
output directory, `-out` to provide a custom directory, `-timeout` per phase, and `-prune_wal` to delete older WAL files.

### Clean sweep (compact + WAL prune)

To fully compact, prune WAL files, and assert that no live entries remain:

```bash
go run ./cmd/compaction_check \
  -n 4000000 -m 4000000 -k 0 \
  -rotate_wal \
  -prune_wal \
  -wait_rcu 45s \
  -verify \
  -assert_empty
```

- **-rotate_wal**: rotate WAL on each flush to make older files eligible for pruning.
- **-prune_wal**: delete WAL files older than the current sequence.
- **-wait_rcu**: allow RCU cleanup to remove obsolete segment directories before measuring.
- **-verify**: perform a full scan to count live entries precisely.
- **-assert_empty**: exit non-zero if any live entries remain after compaction.

Note: `-purge` is available but dangerous; it physically removes non-active segment directories immediately, bypassing
RCU safety. Prefer `-wait_rcu` for production.

### Segment sanity check (segcheck)

Use the `segcheck` tool to validate the on-disk format of a single segment directory (magic + version headers and
optional BLAKE3 from `segment.json`).

```bash
go build ./cmd/segcheck
./segcheck -dir /path/to/store/segments/0000000000000003
```

It verifies `index.louds` (required), `filters/prefix.bf` (if present), `filters/tri.bits` (if present), `keys.dat` (if present), and `tombstones.dat` (if present). If `segment.json` contains BLAKE3 entries, it recomputes and compares them.

## Examples

See the `cmd/example` directory for usage examples:

```bash
go run ./cmd/example
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- Code follows Go conventions
- New features include tests
- Documentation is updated

## License

This project is licensed under the BSD 3-Clause License. See the `LICENSE` file for details.

## WAL Durability and Crash Recovery

SRAD uses a write-ahead log (WAL) for durability. All write operations (`Insert`, `Delete`) are first written to the WAL before being applied to the memtable, ensuring that data survives system crashes.

### Automatic Recovery

**WAL replay happens automatically on every `Open()`**. When you open a store after a crash:

1. SRAD reads all WAL files in sequence order
2. Replays all operations (Insert/Delete) into a fresh memtable
3. Restores the exact state as of the last confirmed write
4. Makes all recovered data immediately available for queries

**No manual intervention required** - crash recovery is completely automatic.

Example crash scenario:
```go
// Before crash
store, _ := srad.Open("./data", nil)
store.Insert([]byte("key1"))  // ✅ Confirmed - written to WAL
store.Insert([]byte("key2"))  // ✅ Confirmed - written to WAL
store.Insert([]byte("key3"))  // ✅ Confirmed - written to WAL
// 💥 System crash before flush to segments

// After restart
store, _ := srad.Open("./data", nil)
// ✅ All three keys automatically recovered from WAL
// ✅ Immediately available for search
iter, _ := store.RegexSearch(ctx, regexp.MustCompile(".*"), nil)
// Returns: key1, key2, key3
```

**Durability guarantee**: If `Insert()` or `Delete()` returns successfully (no error), the operation is durable and will survive crashes.

### Durability Configuration

By default, the WAL buffers writes in userspace and flushes them periodically. You can tune durability-vs-latency using the following options:

- `WALSyncOnEveryWrite` (bool): call `fsync` after each WAL write. Safest but slowest.
- `WALFlushOnEveryWrite` (bool): flush the userspace buffer after each write (no `fsync`). Faster than sync but still reduces loss on process crash.
- `WALFlushEveryBytes` (int): flush after approximately this many bytes are written. Defaults to the WAL buffer size (256KB).
- `WALFlushEveryInterval` (time.Duration): periodic time-based flush (0 = disabled). When >0, a background ticker flushes the WAL buffer at this interval even if the byte threshold is not reached.

Recommendations:
- **Production**: keep `WALFlushEveryBytes = 256KB` (default). Enable `RotateWALOnFlush` to make pruning effective. Periodically call `PruneWAL()`.
- **Maximum durability**: set `WALSyncOnEveryWrite = true` (expect higher latency, but zero data loss on system crash).
- **Lowest latency**: leave all off, at the cost of higher data-loss window on process crash (only in-memory buffer at risk).

Example:
```go
opts := srad.DefaultOptions()
opts.WALFlushEveryBytes = 512 * 1024 // 512KB
opts.RotateWALOnFlush = true         // rotate on each flush
opts.WALFlushEveryInterval = 2 * time.Second // also flush every 2s
store, _ := srad.Open(dir, opts)
```

### WAL Corruption Handling

If a WAL file is corrupted during replay:
- Corrupted files are automatically quarantined (renamed with `.corrupt` suffix)
- Partially written records at the end of a file are truncated to the last valid offset
- Recovery continues with remaining valid WAL files
- Only operations that were successfully confirmed before the corruption point are lost

### Search concurrency and correctness

Regex search scans memtable first, then segments from newest to oldest, seeding a `seen` set with memtable tombstones and per-segment tombstones to enforce "newest wins". The engine holds internal references to segment readers during scans to avoid use-after-close during compaction. Use contexts to cancel long-running searches.

### Trie and LOUDS build behavior

- **Streaming LOUDS (default when possible)**: for lexicographically sorted inputs (flush/compaction), LOUDS is built directly from the sorted keys in two BFS passes with preallocation. This avoids building an intermediate trie and drastically reduces wall‑clock time.
- **Trie from sorted data**: when requested, the trie is built without per‑key inserts; it is path‑compressed and can be parallelized per level for large groups (threshold based on `BuildShardMinKeys`).
- **Unsorted data**: falls back to incremental insert into a compressed trie.
- **Accept bitvector**: the newer `index.louds` format stores a leaf/accept bitvector and does not serialize per‑leaf value payloads, which reduces I/O.

Settings:
- `DisableLOUDSBuild` (bool): skip LOUDS (readers use `keys.dat` + filters). Good for very large bulk loads.
- `AutoDisableLOUDSMinKeys` (int): automatically skip LOUDS when the number of keys ≥ this threshold.
- `ForceTrieBuild` (bool): force trie building even for sorted inputs (useful for tests/benchmarks).

Example:
```go
opts := srad.DefaultOptions()
opts.ForceTrieBuild = true       // always build the trie (for benchmarks)
opts.DisableLOUDSBuild = false   // build LOUDS normally
opts.AutoDisableLOUDSMinKeys = 0 // no auto-skip for LOUDS
```
