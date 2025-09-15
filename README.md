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
    Close() error
    Insert(s []byte) error
    InsertWithTTL(s []byte, ttl time.Duration) error
    Delete(s []byte) error
    RegexSearch(ctx context.Context, re *regexp.Regexp, q *QueryOptions) (Iterator, error)
    PrefixScan(ctx context.Context, prefix []byte, q *QueryOptions) (Iterator, error)
    RangeScan(ctx context.Context, start, end []byte, q *QueryOptions) (Iterator, error)
    Stats() Stats
    RefreshStats()
    Tune(params TuningParams)
    CompactNow(ctx context.Context) error
    Flush(ctx context.Context) error
    RCUEnabled() bool
    AdvanceRCU(ctx context.Context) error
    VacuumPrefix(ctx context.Context, prefix []byte) error
    SetAutotunerEnabled(enabled bool)
    // Delete obsolete WAL files older than the current WAL sequence.
    // Note: pruning is not automatic; call this explicitly. Pruning removes only
    // files with sequence < current and never truncates the active WAL. Enabling
    // RotateWALOnFlush makes older files eligible for pruning immediately.
    PruneWAL() error
}
```

### Options

```go
type Options struct {
    // Core
    ReadOnly                  bool
    Parallelism               int
    VerifyChecksumsOnLoad     bool
    MemtableTargetBytes       int64
    CacheLabelAdvanceBytes    int64
    CacheNFATransitionBytes   int64
    EnableRCU                 bool
    RCUCleanupInterval        time.Duration
    Logger                    Logger
    DisableAutotuner          bool
    DisableAutoFlush          bool
    DefaultTTL                time.Duration
    DisableBackgroundCompaction bool

    // WAL / durability
    RotateWALOnFlush          bool
    WALRotateSize             int64
    WALMaxFileSize            int64
    WALBufferSize             int
    WALSyncOnEveryWrite       bool
    WALFlushOnEveryWrite      bool
    WALFlushEveryBytes        int

    // Filters
    PrefixBloomFPR            float64 // default 0.01
    PrefixBloomMaxPrefixLen   int     // default 16
    EnableTrigramFilter       bool    // default true

    // Build parallelization (0 => auto defaults)
    BuildMaxShards            int
    BuildShardMinKeys         int
    BloomAdaptiveMinKeys      int

    // Range partitioning and async filters
    BuildRangePartitions      int
    AsyncFilterBuild          bool

    // LOUDS build controls
    // DisableLOUDSBuild: when true, LOUDS index is not built during Flush/Compact.
    // Readers will fall back to keys.dat streaming and filters. Exact lookups
    // are still correct but may be slower without LOUDS.
    DisableLOUDSBuild         bool
    // AutoDisableLOUDSMinKeys: when >0 and a build processes at least this many
    // keys, LOUDS build is skipped regardless of DisableLOUDSBuild.
    AutoDisableLOUDSMinKeys   int
    // ForceTrieBuild: when true, always build the trie even for sorted inputs
    // (overrides the streaming LOUDS fast-path). Useful for benchmarking.
    ForceTrieBuild            bool
}
```

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
// Optionally run compaction later, then allow background filter builder to finish
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
│   │   ├── index.edges    # Edge labels and topology
│   │   ├── index.accept   # Accept states
│   │   ├── index.tmap     # Tail mapping
│   │   ├── tails.dat      # Tail data
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

It verifies `index.louds` (required) and, if present, `index.edges`, `index.accept`, `index.tmap`, `filters/prefix.bf`,
`filters/tri.bits`, `keys.dat`, and `tombstones.dat`. If `segment.json` contains BLAKE3 entries, it recomputes and
compares them.

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

## WAL Durability

SRAD uses a write-ahead log (WAL) for durability. By default, the WAL buffers writes in userspace and flushes them periodically. You can tune durability-vs-latency using the following options:

- `WALSyncOnEveryWrite` (bool): call `fsync` after each WAL write. Safest but slowest.
- `WALFlushOnEveryWrite` (bool): flush the userspace buffer after each write (no `fsync`). Faster than sync but still reduces loss on process crash.
- `WALFlushEveryBytes` (int): flush after approximately this many bytes are written. Defaults to the WAL buffer size (256KB).

Recommendations:
- Production: keep `WALFlushEveryBytes = 256KB` (default). Enable `RotateWALOnFlush` to make pruning effective. Periodically call `PruneWAL()`.
- Maximum durability: set `WALSyncOnEveryWrite = true` (expect higher latency).
- Lowest latency: leave all off, at the cost of higher data-loss window on process crash.

Example:
```go
opts := srad.DefaultOptions()
opts.WALFlushEveryBytes = 512 * 1024 // 512KB
opts.RotateWALOnFlush = true         // rotate on each flush
store, _ := srad.Open(dir, opts)
```

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
