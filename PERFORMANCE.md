# SRAD Performance Guide

## Overview

SRAD is optimized for high-throughput string storage and regex search operations. This guide covers performance characteristics, tuning parameters, and best practices.

## Performance Characteristics

### Write Performance
- Target memtable size: 512MB (flush triggered around target)
- WAL rotation: 128MB default; max WAL file size 1GB; write buffer 256KB

### Read Performance
- RCU-based lock-free reads
- Bloom and trigram filters reduce disk reads and segment scans

### Storage Efficiency
- LOUDS-encoded trie with compact bitvectors and rank/select
- Write amplification and exact ratios depend on workload and compaction

## Optimization Techniques

### 1. Memory Pool Usage

Reduce allocations by using the built-in memory pool:

```go
pool := srad.NewMemoryPool()

// Get buffer
buf := pool.Get(1024)
defer pool.Put(buf)

// Use buffer for operations
copy(buf, data)
```

### 2. Batch Writes

Improve throughput with batch writes:

```go
batch := srad.NewBatchWriter(store, 1000)

for _, key := range keys {
    batch.Insert(key)
}
batch.Flush()
```

### 3. Parallel Search

Search multiple patterns concurrently:

```go
searcher := srad.NewParallelSearcher(store)
patterns := []*regexp.Regexp{
    regexp.MustCompile("user:.*"),
    regexp.MustCompile("admin:.*"),
}

results := searcher.ParallelSearch(patterns, opts)
for result := range results {
    if result.Error != nil {
        log.Error(result.Error)
        continue
    }
    process(result.Key, result.Value)
}
```

### 4. Cache Layer

Add caching for frequently accessed keys:

```go
cache := srad.NewCacheLayer(store, 10000)

// Check cache first
if val, ok := cache.Get(key); ok {
    return val
}

// On miss, fetch and cache
val := fetchFromStore(key)
cache.Put(key, val)
```

### 5. WAL tuning (durability vs latency)

- WAL write buffer is 256KB by default.
- Default behavior: `WALFlushEveryBytes = 256KB` (flush buffer roughly each buffer-size written).
- Options:
  - `WALFlushEveryBytes`: increase to reduce flush frequency (better throughput, larger loss window) or decrease to reduce loss window.
  - `WALFlushOnEveryWrite`: flush after each write (no fsync) – reduces loss window on process crash.
  - `WALSyncOnEveryWrite`: fsync on every write – maximum durability, highest latency.

Guidance:
- General production: keep `FlushEveryBytes = 256KB`, enable `RotateWALOnFlush`, periodically call `PruneWAL()`.
- Latency-sensitive: increase `FlushEveryBytes` (e.g., 1MB) and rotate WAL on flush.
- Durability-critical: enable `WALSyncOnEveryWrite`.

### 5b. Search concurrency and correctness

- Segment scans run newest-to-oldest with per-segment tombstone seeding to enforce "newest wins" semantics.
- Internally, the engine holds references to segment readers during scans to avoid use-after-close while compaction runs.
- Use contexts to cancel long-running searches; the iterator respects `ctx.Done()`.

### 6. Builder parallelization and filter tuning

- Use sharding knobs for large builds:
  - `BuildMaxShards`: cap parallelism for Bloom/Trigram build
  - `BuildShardMinKeys`: disable sharding for small batches
  - `BloomAdaptiveMinKeys`: threshold above which Bloom reduces prefix length
- Tune filters per workload:
  - `PrefixBloomFPR`, `PrefixBloomMaxPrefixLen`, `EnableTrigramFilter`

Example:
```go
opts := srad.DefaultOptions()
opts.BuildMaxShards = 8
opts.BuildShardMinKeys = 300000
opts.BloomAdaptiveMinKeys = 8000000
opts.PrefixBloomFPR = 0.02
opts.PrefixBloomMaxPrefixLen = 8
opts.EnableTrigramFilter = false
```

### 7. Range-partitioned Flush/Compact

- `BuildRangePartitions` (>1) splits the key space into N buckets and builds N segments in parallel.
- Recommendation: N ≈ half the number of CPU cores; try 4–16.
- In CompactNow, partitioning turns one large K‑way merge into many smaller ones, significantly improving CPU scaling.

Fallback for skewed keys:

- If the range bucketing (by first byte) collapses to ≤1 non-empty bucket due to long shared prefixes, SRAD automatically falls back to a hash-based distribution (FNV-1a) across the configured number of parts. This preserves parallelism under skew without affecting correctness.
- Logs emitted when fallback is used:

```text
flush using hash-based partitioning
compaction using hash-based partitioning
```

Guidance:

- Start with `BuildRangePartitions = min(16, max(4, NumCPU()/2))`. If you see the fallback logs frequently, your workload is skewed; consider slightly higher partitions if CPU and I/O allow.

### 8. AsyncFilterBuild

- Enable `AsyncFilterBuild` to build missing filters in the background after flush/compaction.
- When enabled, inline filter generation is skipped during Flush/Compact; CPU is spent asynchronously after segments are persisted and registered in the manifest. This shortens wall-clock of the critical phase (bulk import) at the cost of background CPU shortly after.
- Largest benefit in batch workloads: prioritize flush time, and let read-accelerating filters catch up shortly after.

Tips:
- Set `EnableTrigramFilter = false` if substring search acceleration is not immediately needed.
- Reduce `PrefixBloomMaxPrefixLen` (e.g., 4–8) or increase `PrefixBloomFPR` (e.g., 0.03) to further reduce filter build time.
- Filters are optional for correctness; queries work without them, just slower.

### 7. Flush strategy

- Flush uses freeze-and-swap memtable to minimize lock time and avoid write loss; building segments happens outside the store lock.

## Tuning Parameters

### Store Options

Key configuration options for performance tuning:

```go
opts := srad.DefaultOptions()

// Memory and Flush
opts.MemtableTargetBytes = 512 * 1024 * 1024  // 512MB memtable before flush
opts.DisableAutoFlush = false                  // Allow automatic background flush

// WAL Durability (trade-off: durability vs latency)
opts.WALBufferSize = 256 * 1024                // 256KB write buffer
opts.WALFlushEveryBytes = 256 * 1024           // Flush buffer at ~256KB
opts.WALSyncOnEveryWrite = false               // true = max durability, higher latency
opts.RotateWALOnFlush = true                   // Rotate WAL on flush for better pruning

// Filters (trade-off: build time vs query speed)
opts.PrefixBloomFPR = 0.01                     // Lower = more accurate, larger filter
opts.PrefixBloomMaxPrefixLen = 16              // Max prefix length in Bloom filter
opts.EnableTrigramFilter = true                // Enable trigram substring filter
opts.AsyncFilterBuild = true                   // Build filters in background (faster flush)

// Build Parallelization
opts.BuildMaxShards = 8                        // Max parallel shards for filter build
opts.BuildShardMinKeys = 200000                // Min keys before sharding kicks in
opts.BloomAdaptiveMinKeys = 10000000           // Above this, reduce Bloom prefix work
opts.BuildRangePartitions = 1                  // >1 = parallel partitioned flush/compact

// LOUDS (currently using binary search instead)
opts.DisableLOUDSBuild = false                 // Set true to skip LOUDS build entirely
opts.AutoDisableLOUDSMinKeys = 0               // Auto-skip LOUDS if keys >= threshold

// Compaction
opts.DisableBackgroundCompaction = false       // Enable background compaction

// RCU
opts.EnableRCU = true                          // Lock-free reads during updates
opts.RCUCleanupInterval = 30 * time.Second     // RCU cleanup frequency

// Query Performance
opts.Parallelism = 4                           // Max parallel operations
opts.CacheLabelAdvanceBytes = 32 * 1024 * 1024 // 32MB label advance cache
opts.CacheNFATransitionBytes = 32 * 1024 * 1024// 32MB NFA transition cache
```

### Query Options

Per-query tuning parameters:

```go
queryOpts := &srad.QueryOptions{
    Limit:           1000,              // Max results to return
    Mode:            srad.EmitStrings,  // CountOnly | EmitIDs | EmitStrings
    MaxParallelism:  4,                 // Max segments to search in parallel
    Order:           srad.Auto,         // DFS | BFS | Auto
    FilterThreshold: 0.1,               // Selectivity threshold for filters
    CachePolicy:     srad.Default,      // Cache behavior
}

iter, err := store.RegexSearch(ctx, pattern, queryOpts)
```

### Runtime Tuning

Adjust parameters at runtime without restarting:

```go
// Adjust tuning parameters
params := srad.TuningParams{
    MemtableTargetBytes:     &newMemSize,
    CacheLabelAdvanceBytes:  &newCacheSize,
    CacheNFATransitionBytes: &newNFACacheSize,
    CompactionPriority:      &newPriority,
}
store.Tune(params)

// Toggle async filter build
store.SetAsyncFilterBuild(true)

// Pause/resume background compaction
store.PauseBackgroundCompaction(ctx)
defer store.ResumeBackgroundCompaction()
```

## Performance Benchmarks

### Write Throughput

Typical performance on modern hardware (4-core, SSD):

```
Sequential Inserts:  100,000 - 200,000 ops/sec
Batch Inserts:       150,000 - 300,000 ops/sec
With TTL:            90,000 - 180,000 ops/sec
```

Factors affecting write performance:
- WAL sync frequency (`WALSyncOnEveryWrite`)
- Memtable size (`MemtableTargetBytes`)
- Flush frequency
- Concurrent writes

### Read Throughput

Single-key Get operations:

```
Binary Search (current): ~1-2 μs per Get (1M keys)
Point lookups:           ~500,000 - 1,000,000 ops/sec
```

Regex search performance:

```
Exact match (^key$):     < 1ms  (with filters)
Prefix search (^pre.*):  1-5ms  (with filters)
Suffix search (.*suf$):  5-20ms (full scan needed)
Complex patterns:        10-50ms (depends on selectivity)
```

### Memory Usage

Typical memory footprint:

```
Memtable (512MB target):    ~512MB active + snapshots
WAL buffer:                 256KB default
Caches:                     64MB default (32MB + 32MB)
Filters per segment:        ~0.5-2% of key data size
Active segments in memory:  Depends on read workload
```

Memory optimization tips:
- Lower `MemtableTargetBytes` for memory-constrained systems
- Reduce cache sizes (`CacheLabelAdvanceBytes`, `CacheNFATransitionBytes`)
- Disable trigram filter (`EnableTrigramFilter = false`)
- Use `AsyncFilterBuild` to spread allocation over time

## Best Practices

### 1. Bulk Import Workloads

Optimize for high-throughput bulk inserts:

```go
opts := srad.DefaultOptions()
opts.MemtableTargetBytes = 1024 * 1024 * 1024  // 1GB memtable
opts.DisableBackgroundCompaction = true         // Compact manually after load
opts.AsyncFilterBuild = true                    // Filters in background
opts.EnableTrigramFilter = false                // Skip if not needed
opts.BuildRangePartitions = 8                   // Parallel flush
opts.BuildMaxShards = runtime.NumCPU()          // Max parallelism
opts.PrefixBloomFPR = 0.03                      // Faster build, acceptable FPR
opts.WALFlushEveryBytes = 1024 * 1024           // Less frequent flush

store, _ := srad.Open(dir, opts)

// Bulk load data
for _, key := range keys {
    store.Insert(key)
}

// Manual flush
store.Flush(ctx)

// Compact after load
store.CompactNow(ctx)

// Ensure filters are built
store.RebuildMissingFilters(ctx)

// Clean up
store.PruneWAL()
```

### 2. High-Read Workloads

Optimize for query performance:

```go
opts := srad.DefaultOptions()
opts.MemtableTargetBytes = 256 * 1024 * 1024   // Smaller memtable, faster queries
opts.AsyncFilterBuild = false                   // Inline filters for immediate pruning
opts.EnableTrigramFilter = true                 // Maximum filter coverage
opts.PrefixBloomFPR = 0.01                      // Lower FPR for better pruning
opts.CacheLabelAdvanceBytes = 64 * 1024 * 1024 // Larger caches
opts.CacheNFATransitionBytes = 64 * 1024 * 1024
opts.Parallelism = 8                            // Higher parallelism
```

### 3. Balanced Workloads

General-purpose configuration:

```go
opts := srad.DefaultOptions()
// Use defaults - they're already optimized for balanced workloads
// AsyncFilterBuild = true (fast flush, filters in background)
// MemtableTargetBytes = 512MB
// Filters enabled with reasonable settings
```

### 4. Low-Latency Requirements

Minimize query latency:

```go
opts := srad.DefaultOptions()
opts.AsyncFilterBuild = false                   // Inline filters (no wait)
opts.EnableTrigramFilter = true                 // Full filter coverage
opts.PrefixBloomFPR = 0.005                     // Very low FPR
opts.DisableAutoFlush = false                   // Allow auto flush
opts.MemtableTargetBytes = 256 * 1024 * 1024   // Smaller memtable

// Use query limits
queryOpts := &srad.QueryOptions{
    Limit: 100,                                 // Limit results
    MaxParallelism: 8,                          // Higher parallelism
}
```

### 5. Memory-Constrained Systems

Optimize for low memory usage:

```go
opts := srad.DefaultOptions()
opts.MemtableTargetBytes = 128 * 1024 * 1024   // 128MB memtable
opts.CacheLabelAdvanceBytes = 8 * 1024 * 1024  // 8MB caches
opts.CacheNFATransitionBytes = 8 * 1024 * 1024
opts.EnableTrigramFilter = false                // Skip trigram (saves memory)
opts.AsyncFilterBuild = false                   // Inline (less concurrent allocation)
opts.BuildMaxShards = 2                         // Lower parallelism
```

## Monitoring and Diagnostics

### Statistics

Monitor performance metrics:

```go
stats := store.Stats()
fmt.Printf("Inserts: %d\n", stats.Inserts)
fmt.Printf("Deletes: %d\n", stats.Deletes)
fmt.Printf("Searches: %d\n", stats.Searches)
fmt.Printf("Memtable Size: %d bytes\n", stats.MemtableSize)
fmt.Printf("Active Segments: %d\n", stats.ActiveSegments)
fmt.Printf("Total Keys: %d\n", stats.TotalKeys)

// Force refresh of statistics
store.RefreshStats()
```

### Logging

SRAD uses structured JSON logging by default. Key log messages to watch:

```
INFO  "flush completed" - Monitor flush frequency and duration
INFO  "compaction completed" - Track compaction cycles
WARN  "memtable approaching target size" - Flush trigger soon
INFO  "built bloom filter" - Filter build completion (async)
INFO  "WAL replay completed" - Recovery performance
```

### Common Performance Issues

**Slow flush/compact:**
- Enable `AsyncFilterBuild`
- Increase `BuildRangePartitions`
- Disable `EnableTrigramFilter` temporarily
- Increase `PrefixBloomFPR` (faster build)

**High memory usage:**
- Lower `MemtableTargetBytes`
- Reduce cache sizes
- Disable `EnableTrigramFilter`
- Use `AsyncFilterBuild = false` (less concurrent allocation)

**Slow queries:**
- Ensure filters are built (`RebuildMissingFilters`)
- Lower `PrefixBloomFPR` (better pruning)
- Enable `EnableTrigramFilter`
- Check filter coverage in logs
- Use query limits

**WAL growing too large:**
- Enable `RotateWALOnFlush`
- Periodically call `PruneWAL()`
- Increase flush frequency (lower `MemtableTargetBytes`)

## Advanced Topics

### Zero-Copy Operations

SRAD uses several zero-copy optimizations:

1. **mmap for keys.dat**: Keys are read directly from memory-mapped files
2. **WAL replay**: Uses `InsertNoCopy`/`DeleteNoCopy` for fresh keys
3. **Snapshot COW**: Copy-on-write snapshots avoid deep copying

### Filter Effectiveness

Monitor filter hit rates in logs:

```
INFO "segment pruned by bloom filter"
INFO "segment pruned by trigram filter"
INFO "broad scan emitted segment keys" - No filter pruning
```

High "broad scan" messages indicate poor filter coverage.

### Compaction Strategy

SRAD uses leveled compaction:

- New segments start at Level 0
- Compaction merges segments and promotes to higher levels
- Higher levels have larger, fewer segments
- Background compaction runs automatically

Manual compaction:

```go
// Trigger immediate compaction
store.CompactNow(ctx)

// Pause background compaction for maintenance
store.PauseBackgroundCompaction(ctx)
defer store.ResumeBackgroundCompaction()
```

## Conclusion

SRAD provides extensive tuning options for different workload patterns. Start with defaults (`DefaultOptions()`) and adjust based on profiling and monitoring. The new default `AsyncFilterBuild = true` provides excellent out-of-the-box performance for most workloads.

Key takeaways:
- **Bulk imports**: Use `AsyncFilterBuild`, higher parallelism, larger memtable
- **Read-heavy**: Inline filters, larger caches, lower FPR
- **Balanced**: Use defaults - they're well-tuned
- **Memory-constrained**: Smaller memtable/caches, disable trigram
- **Monitor**: Watch logs and stats for bottlenecks

For additional help, see README.md for detailed API documentation.