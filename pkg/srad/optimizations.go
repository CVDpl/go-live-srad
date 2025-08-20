package srad

import (
	"context"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/CVDpl/go-live-srad/pkg/srad/segment"
)

// MemoryPool manages reusable memory buffers to reduce allocations.
type MemoryPool struct {
	small  sync.Pool // For buffers up to 4KB
	medium sync.Pool // For buffers up to 64KB
	large  sync.Pool // For buffers up to 1MB

	stats struct {
		allocations uint64
		reuses      uint64
	}
}

// NewMemoryPool creates a new memory pool.
func NewMemoryPool() *MemoryPool {
	mp := &MemoryPool{}

	mp.small.New = func() interface{} {
		atomic.AddUint64(&mp.stats.allocations, 1)
		buf := make([]byte, 4096)
		return &buf
	}

	mp.medium.New = func() interface{} {
		atomic.AddUint64(&mp.stats.allocations, 1)
		buf := make([]byte, 65536)
		return &buf
	}

	mp.large.New = func() interface{} {
		atomic.AddUint64(&mp.stats.allocations, 1)
		buf := make([]byte, 1048576)
		return &buf
	}

	return mp
}

// Get retrieves a buffer of at least the requested size.
func (mp *MemoryPool) Get(size int) []byte {
	atomic.AddUint64(&mp.stats.reuses, 1)

	switch {
	case size <= 4096:
		buf := mp.small.Get().(*[]byte)
		return (*buf)[:size]
	case size <= 65536:
		buf := mp.medium.Get().(*[]byte)
		return (*buf)[:size]
	case size <= 1048576:
		buf := mp.large.Get().(*[]byte)
		return (*buf)[:size]
	default:
		// For very large buffers, allocate directly
		return make([]byte, size)
	}
}

// Put returns a buffer to the pool.
func (mp *MemoryPool) Put(buf []byte) {
	// Clear sensitive data
	for i := range buf {
		buf[i] = 0
	}

	cap := cap(buf)
	switch {
	case cap == 4096:
		mp.small.Put(&buf)
	case cap == 65536:
		mp.medium.Put(&buf)
	case cap == 1048576:
		mp.large.Put(&buf)
	default:
		// Let GC handle non-standard sizes
	}
}

// GetStats returns pool statistics.
func (mp *MemoryPool) GetStats() (allocations, reuses uint64) {
	return atomic.LoadUint64(&mp.stats.allocations),
		atomic.LoadUint64(&mp.stats.reuses)
}

// BatchWriter accumulates multiple writes before flushing.
type BatchWriter struct {
	store     Store
	mu        sync.Mutex
	batch     [][]byte
	batchSize int
	maxBatch  int
}

// NewBatchWriter creates a new batch writer.
func NewBatchWriter(store Store, maxBatch int) *BatchWriter {
	return &BatchWriter{
		store:    store,
		batch:    make([][]byte, 0, maxBatch),
		maxBatch: maxBatch,
	}
}

// Insert adds a key to the batch.
func (bw *BatchWriter) Insert(key []byte) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Make a copy of the key
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	bw.batch = append(bw.batch, keyCopy)
	bw.batchSize += len(keyCopy)

	// Auto-flush if batch is full
	if len(bw.batch) >= bw.maxBatch {
		return bw.flushLocked()
	}

	return nil
}

// Flush writes all pending keys to the store.
func (bw *BatchWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flushLocked()
}

func (bw *BatchWriter) flushLocked() error {
	if len(bw.batch) == 0 {
		return nil
	}

	// Write all keys
	for _, key := range bw.batch {
		if err := bw.store.Insert(key); err != nil {
			return err
		}
	}

	// Clear batch
	bw.batch = bw.batch[:0]
	bw.batchSize = 0

	return nil
}

// ParallelSearcher performs parallel regex searches.
type ParallelSearcher struct {
	store   Store
	workers int
}

// NewParallelSearcher creates a new parallel searcher.
func NewParallelSearcher(store Store) *ParallelSearcher {
	return &ParallelSearcher{
		store:   store,
		workers: runtime.NumCPU(),
	}
}

// SearchResult holds a search result.
type SearchResult struct {
	Key   []byte
	Value []byte
	Error error
}

// ParallelSearch performs a parallel search across segments.
func (ps *ParallelSearcher) ParallelSearch(patterns []*regexp.Regexp, opts *QueryOptions) <-chan SearchResult {
	results := make(chan SearchResult, 100)

	var wg sync.WaitGroup

	// Launch workers for each pattern
	for _, pattern := range patterns {
		wg.Add(1)
		go func(p *regexp.Regexp) {
			defer wg.Done()

			ctx := context.Background()
			iter, err := ps.store.RegexSearch(ctx, p, opts)
			if err != nil {
				results <- SearchResult{Error: err}
				return
			}
			defer iter.Close()

			for iter.Next(ctx) {
				// Send result
				results <- SearchResult{
					Key:   iter.String(),
					Value: iter.String(), // Use key as value for now
				}
			}

			if err := iter.Err(); err != nil {
				results <- SearchResult{Error: err}
			}
		}(pattern)
	}

	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

// CacheLayer provides a simple LRU cache for frequently accessed keys.
type CacheLayer struct {
	store   Store
	cache   *sync.Map
	maxSize int
	size    int32
}

// NewCacheLayer creates a new cache layer.
func NewCacheLayer(store Store, maxSize int) *CacheLayer {
	return &CacheLayer{
		store:   store,
		cache:   &sync.Map{},
		maxSize: maxSize,
	}
}

// Get retrieves a value, checking cache first.
func (cl *CacheLayer) Get(key []byte) ([]byte, bool) {
	// Check cache
	if val, ok := cl.cache.Load(string(key)); ok {
		return val.([]byte), true
	}

	// Not in cache, would need to search store
	// This is simplified - real implementation would need full search
	return nil, false
}

// Put adds a key-value pair to the cache.
func (cl *CacheLayer) Put(key, value []byte) {
	size := atomic.LoadInt32(&cl.size)
	if int(size) >= cl.maxSize {
		// Simple eviction - clear entire cache
		// Real implementation would use LRU
		cl.cache.Range(func(k, v interface{}) bool {
			cl.cache.Delete(k)
			return true
		})
		atomic.StoreInt32(&cl.size, 0)
	}

	cl.cache.Store(string(key), value)
	atomic.AddInt32(&cl.size, 1)
}

// Prefetcher pre-reads segments to warm up the cache.
type Prefetcher struct {
	readers []*segment.Reader
	mu      sync.RWMutex
}

// NewPrefetcher creates a new prefetcher.
func NewPrefetcher() *Prefetcher {
	return &Prefetcher{
		readers: make([]*segment.Reader, 0),
	}
}

// AddSegment adds a segment for prefetching.
func (pf *Prefetcher) AddSegment(reader *segment.Reader) {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	pf.readers = append(pf.readers, reader)
}

// Prefetch loads segment data into memory.
func (pf *Prefetcher) Prefetch(pattern string) {
	pf.mu.RLock()
	readers := make([]*segment.Reader, len(pf.readers))
	copy(readers, pf.readers)
	pf.mu.RUnlock()

	// Parallel prefetch
	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func(r *segment.Reader) {
			defer wg.Done()
			// This would trigger OS page cache warming
			// by reading segment headers and bloom filters
			_ = r.GetMetadata()
		}(reader)
	}
	wg.Wait()
}
