package segment

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	"golang.org/x/sys/unix"
)

// DefaultMaxMmapCacheSize is the default maximum number of mmapped keys.dat files.
const DefaultMaxMmapCacheSize = 128

// DefaultMmapCacheIdleTimeout is the default idle timeout for cached mmaps.
// Entries not accessed within this duration may be evicted even if cache is not full.
// Set to 0 to disable time-based eviction.
const DefaultMmapCacheIdleTimeout = 5 * time.Minute

// MmapCache provides LRU-based lazy mmap management for keys.dat files.
// It limits the number of concurrently mmapped files to reduce virtual memory usage.
// Optionally, entries can be evicted after an idle timeout even if cache is not full.
type MmapCache struct {
	mu sync.Mutex

	// Maximum number of concurrent mmaps
	maxSize int

	// Idle timeout: entries not accessed within this duration may be evicted
	// Set to 0 to disable time-based eviction
	idleTimeout time.Duration

	// LRU list: front = most recently used, back = least recently used
	lruList *list.List

	// Map from segmentID to cache entry
	entries map[uint64]*mmapEntry

	// Logger for warnings
	logger common.Logger

	// Stats
	hits       uint64
	misses     uint64
	evicts     uint64
	idleEvicts uint64

	// Background cleanup
	stopCleanup chan struct{}
	cleanupWg   sync.WaitGroup
}

// mmapEntry represents a cached mmap for a segment's keys.dat
type mmapEntry struct {
	segmentID uint64
	data      []byte // mmapped data
	size      int64  // file size (for stats)

	// Reference count: entry can't be evicted while refcount > 0
	refcount int32

	// Last access time for idle timeout eviction
	lastAccess time.Time

	// LRU list element for O(1) removal
	lruElement *list.Element

	// Path to keys.dat (for lazy mmap)
	path string
}

// NewMmapCache creates a new mmap cache with the specified maximum size.
// Use NewMmapCacheWithTimeout for time-based eviction.
func NewMmapCache(maxSize int, logger common.Logger) *MmapCache {
	return NewMmapCacheWithTimeout(maxSize, DefaultMmapCacheIdleTimeout, logger)
}

// NewMmapCacheWithTimeout creates a new mmap cache with specified size and idle timeout.
// Set idleTimeout to 0 to disable time-based eviction.
func NewMmapCacheWithTimeout(maxSize int, idleTimeout time.Duration, logger common.Logger) *MmapCache {
	if maxSize <= 0 {
		maxSize = DefaultMaxMmapCacheSize
	}
	if logger == nil {
		logger = common.NewNullLogger()
	}

	c := &MmapCache{
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
		lruList:     list.New(),
		entries:     make(map[uint64]*mmapEntry),
		logger:      logger,
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup if idle timeout is enabled
	if idleTimeout > 0 {
		c.cleanupWg.Add(1)
		go c.cleanupLoop()
	}

	return c
}

// Acquire returns the mmapped data for the given segment, mmapping if necessary.
// The caller MUST call Release when done with the data.
// Returns nil if mmap fails or is not available.
func (c *MmapCache) Acquire(segmentID uint64, path string, fileSize int64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Check if already cached
	if entry, ok := c.entries[segmentID]; ok {
		// Move to front of LRU and update access time
		c.lruList.MoveToFront(entry.lruElement)
		entry.lastAccess = now
		atomic.AddInt32(&entry.refcount, 1)
		atomic.AddUint64(&c.hits, 1)
		return entry.data
	}

	atomic.AddUint64(&c.misses, 1)

	// Need to mmap - first evict if at capacity
	c.evictIfNeededLocked()

	// Perform mmap
	data, err := c.mmapFile(path, fileSize)
	if err != nil {
		c.logger.Debug("mmap failed for keys.dat", "segment_id", segmentID, "path", path, "error", err)
		return nil
	}

	// Create entry
	entry := &mmapEntry{
		segmentID:  segmentID,
		data:       data,
		size:       fileSize,
		refcount:   1, // Caller holds one reference
		lastAccess: now,
		path:       path,
	}
	entry.lruElement = c.lruList.PushFront(entry)
	c.entries[segmentID] = entry

	return data
}

// Release decrements the reference count for a segment's mmap.
// The mmap may be evicted after release if cache is under pressure.
func (c *MmapCache) Release(segmentID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[segmentID]
	if !ok {
		return
	}

	newCount := atomic.AddInt32(&entry.refcount, -1)
	if newCount < 0 {
		// Shouldn't happen, but fix it
		atomic.StoreInt32(&entry.refcount, 0)
		c.logger.Warn("mmap cache refcount went negative", "segment_id", segmentID)
	}
}

// Remove explicitly removes a segment from the cache (e.g., when segment is deleted).
// Blocks until refcount reaches 0 or returns immediately if not cached.
func (c *MmapCache) Remove(segmentID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[segmentID]
	if !ok {
		return
	}

	// Wait for refcount to reach 0 (busy wait with unlock/lock)
	// In practice, this should be very brief as queries are short-lived
	for atomic.LoadInt32(&entry.refcount) > 0 {
		c.mu.Unlock()
		// Brief yield to allow Release calls
		c.mu.Lock()
	}

	c.removeLocked(entry)
}

// Close unmaps all cached entries and releases resources.
func (c *MmapCache) Close() error {
	// Stop background cleanup goroutine
	if c.idleTimeout > 0 {
		close(c.stopCleanup)
		c.cleanupWg.Wait()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.entries {
		if entry.data != nil {
			if err := unix.Munmap(entry.data); err != nil {
				c.logger.Warn("failed to munmap on cache close", "segment_id", entry.segmentID, "error", err)
			}
		}
	}

	c.entries = make(map[uint64]*mmapEntry)
	c.lruList = list.New()
	return nil
}

// Stats returns cache statistics.
func (c *MmapCache) Stats() (hits, misses, evicts, idleEvicts uint64, currentSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return atomic.LoadUint64(&c.hits), atomic.LoadUint64(&c.misses), atomic.LoadUint64(&c.evicts), atomic.LoadUint64(&c.idleEvicts), len(c.entries)
}

// evictIfNeededLocked evicts LRU entries until cache is under maxSize.
// Must be called with c.mu held.
func (c *MmapCache) evictIfNeededLocked() {
	for len(c.entries) >= c.maxSize {
		// Find LRU entry with refcount == 0
		evicted := false
		for elem := c.lruList.Back(); elem != nil; elem = elem.Prev() {
			entry := elem.Value.(*mmapEntry)
			if atomic.LoadInt32(&entry.refcount) == 0 {
				c.removeLocked(entry)
				atomic.AddUint64(&c.evicts, 1)
				evicted = true
				break
			}
		}

		if !evicted {
			// All entries are in use - can't evict, allow over capacity temporarily
			c.logger.Warn("mmap cache at capacity with all entries in use",
				"size", len(c.entries),
				"max_size", c.maxSize,
			)
			break
		}
	}
}

// removeLocked removes an entry from the cache.
// Must be called with c.mu held.
func (c *MmapCache) removeLocked(entry *mmapEntry) {
	if entry.data != nil {
		if err := unix.Munmap(entry.data); err != nil {
			c.logger.Warn("failed to munmap", "segment_id", entry.segmentID, "error", err)
		}
		entry.data = nil
	}

	if entry.lruElement != nil {
		c.lruList.Remove(entry.lruElement)
		entry.lruElement = nil
	}

	delete(c.entries, entry.segmentID)
}

// mmapFile performs the actual mmap syscall.
func (c *MmapCache) mmapFile(path string, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}

	// Open file
	fd, err := unix.Open(path, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	// Mmap
	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Advise kernel about access pattern
	_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)

	return data, nil
}

// PreloadHint hints that a segment may be accessed soon.
// This is a no-op hint that could be used for prefetching in the future.
func (c *MmapCache) PreloadHint(segmentID uint64, path string, fileSize int64) {
	// Currently a no-op - could implement background prefetch in the future
}

// cleanupLoop runs in background and periodically evicts idle entries.
func (c *MmapCache) cleanupLoop() {
	defer c.cleanupWg.Done()

	// Check every 1/4 of idle timeout, but at least every minute
	interval := c.idleTimeout / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	if interval > c.idleTimeout {
		interval = c.idleTimeout
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCleanup:
			return
		case <-ticker.C:
			c.evictIdleEntries()
		}
	}
}

// evictIdleEntries removes entries that haven't been accessed within idleTimeout.
func (c *MmapCache) evictIdleEntries() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.idleTimeout <= 0 {
		return
	}

	now := time.Now()
	cutoff := now.Add(-c.idleTimeout)

	// Iterate from LRU (back) to MRU (front)
	var toEvict []*mmapEntry
	for elem := c.lruList.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*mmapEntry)

		// Stop if we reach entries that are still fresh
		if entry.lastAccess.After(cutoff) {
			break
		}

		// Only evict if not in use
		if atomic.LoadInt32(&entry.refcount) == 0 {
			toEvict = append(toEvict, entry)
		}
	}

	// Evict collected entries
	for _, entry := range toEvict {
		c.removeLocked(entry)
		atomic.AddUint64(&c.idleEvicts, 1)
	}

	if len(toEvict) > 0 {
		c.logger.Debug("evicted idle mmap entries",
			"count", len(toEvict),
			"idle_timeout", c.idleTimeout,
			"remaining", len(c.entries),
		)
	}
}

// GetIdleTimeout returns the configured idle timeout.
func (c *MmapCache) GetIdleTimeout() time.Duration {
	return c.idleTimeout
}
