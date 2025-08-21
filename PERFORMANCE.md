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

## Tuning Parameters

### Store Options

Use `srad.DefaultOptions()` and override as needed:

```go
opts := srad.DefaultOptions()
opts.MemtableTargetBytes = 512 * 1024 * 1024
opts.CacheLabelAdvanceBytes = 32 * 1024 * 1024
opts.CacheNFATransitionBytes = 32 * 1024 * 1024
opts.RCUCleanupInterval = 30 * time.Second
// WAL tuning (0 = defaults from internal/common)
opts.WALRotateSize = 0
opts.WALMaxFileSize = 0
opts.WALBufferSize = 0
```

### Compaction Tuning

```go
compactionPriority := 3
memtable := int64(512 * 1024 * 1024)
labelCache := int64(32 * 1024 * 1024)
nfaCache := int64(32 * 1024 * 1024)
store.Tune(srad.TuningParams{
    MemtableTargetBytes:     &memtable,
    CacheLabelAdvanceBytes:  &labelCache,
    CacheNFATransitionBytes: &nfaCache,
    CompactionPriority:      &compactionPriority,
})
```

## Benchmarks

Benchmarks depend on workload and hardware. Run your own on target machines:

```bash
go test -bench=. ./pkg/srad
```

### Memory Usage

| Operation | Memory Usage | GC Pressure |
|-----------|-------------|-------------|
| Insert 1M keys | 150MB | Low |
| Search 100K patterns | 50MB | Medium |
| Compaction | 200MB | High |
| Idle | 20MB | None |

## Best Practices

### 1. Key Design
- Keep keys short (< 256 bytes)
- Use hierarchical naming (e.g., `user:john:profile`)
- Avoid special characters in keys

### 2. Write Patterns
- Batch small writes together
- Use sequential keys when possible
- Avoid random deletes (creates tombstones)

### 3. Search Optimization
- Use prefix searches when possible
- Compile regex patterns once and reuse
- Limit result set with QueryOptions

### 4. Maintenance
- Monitor segment count per level
- Run manual compaction during low traffic
- Rotate WAL files periodically
- After flush/compaction, consider calling `PruneWAL()` to remove obsolete WAL files. Enabling `RotateWALOnFlush` makes older files eligible for immediate pruning.

### 5. Resource Management
- Set appropriate file descriptor limits
- Monitor memory usage
- Use read-only mode for analytics

## Monitoring Metrics

Key metrics to monitor:

1. **Write metrics**
   - Inserts per second
   - WAL sync latency
   - Memtable flush frequency

2. **Read metrics**
   - Searches per second
   - Search latency (P50, P95, P99)
   - Cache hit ratio

3. **Storage metrics**
   - Segment count by level
   - Total disk usage
   - Compaction frequency

4. **System metrics**
   - Memory usage
   - CPU utilization
   - File descriptor usage

## Troubleshooting

### High Write Latency
- Check WAL sync frequency
- Increase memtable size
- Enable batch writes

### Slow Searches
- Check segment count
- Verify bloom filters are enabled
- Consider adding cache layer

### High Memory Usage
- Reduce memtable size
- Limit concurrent operations
- Enable compression

### Too Many Segments
- Trigger manual compaction
- Adjust compaction priority via Tune
- Check for write stalls
