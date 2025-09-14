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

### 7. Flush strategy

- Flush uses freeze-and-swap memtable to minimize lock time and avoid write loss; building segments happens outside the store lock.

## Tuning Parameters

### Store Options

Use `