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

Use `