package srad

import (
	"sync"
	"sync/atomic"
	"time"
)

// StatsCollector collects and maintains statistics for the store.
type StatsCollector struct {
	mu sync.RWMutex

	// Operation counts
	inserts     uint64
	deletes     uint64
	searches    uint64
	flushes     uint64
	compactions uint64

	// Timing statistics
	latencies    []time.Duration
	maxLatencies int

	// Rate calculation
	lastRateCalc time.Time
	lastInserts  uint64
	lastDeletes  uint64
	lastSearches uint64
	insertRate   float64
	deleteRate   float64
	searchRate   float64

	// Cache statistics
	labelAdvanceHits   uint64
	labelAdvanceMisses uint64
	nfaTransHits       uint64
	nfaTransMisses     uint64

	// Filter statistics
	prefixBloomChecks uint64
	prefixBloomHits   uint64
	trigramChecks     uint64
	trigramSkips      uint64

	// Level statistics
	levelSizes    map[int]int64
	segmentCounts map[int]int
	tombstones    map[int]int64

	// Current manifest generation
	manifestGen uint64

	// Totals window start
	startTime time.Time
}

// NewStatsCollector creates a new statistics collector.
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		maxLatencies:  10000,
		latencies:     make([]time.Duration, 0, 10000),
		lastRateCalc:  time.Now(),
		startTime:     time.Now(),
		levelSizes:    make(map[int]int64),
		segmentCounts: make(map[int]int),
		tombstones:    make(map[int]int64),
	}
}

// RecordInsert records an insert operation.
func (sc *StatsCollector) RecordInsert() { atomic.AddUint64(&sc.inserts, 1) }

// RecordDelete records a delete operation.
func (sc *StatsCollector) RecordDelete() { atomic.AddUint64(&sc.deletes, 1) }

// RecordSearch records a search operation.
func (sc *StatsCollector) RecordSearch() { atomic.AddUint64(&sc.searches, 1) }

// RecordFlush records a flush operation.
func (sc *StatsCollector) RecordFlush(duration time.Duration) {
	atomic.AddUint64(&sc.flushes, 1)
	sc.recordLatency(duration)
}

// RecordCompaction records a compaction operation.
func (sc *StatsCollector) RecordCompaction(duration time.Duration) {
	atomic.AddUint64(&sc.compactions, 1)
	sc.recordLatency(duration)
}

// recordLatency records an operation latency.
func (sc *StatsCollector) recordLatency(d time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.latencies = append(sc.latencies, d)

	// Keep only the most recent latencies
	if len(sc.latencies) > sc.maxLatencies {
		copy(sc.latencies, sc.latencies[len(sc.latencies)-sc.maxLatencies:])
		sc.latencies = sc.latencies[:sc.maxLatencies]
	}
}

// RecordCacheHit records a cache hit.
func (sc *StatsCollector) RecordCacheHit(cacheType string) {
	switch cacheType {
	case "label_advance":
		atomic.AddUint64(&sc.labelAdvanceHits, 1)
	case "nfa_trans":
		atomic.AddUint64(&sc.nfaTransHits, 1)
	}
}

// RecordCacheMiss records a cache miss.
func (sc *StatsCollector) RecordCacheMiss(cacheType string) {
	switch cacheType {
	case "label_advance":
		atomic.AddUint64(&sc.labelAdvanceMisses, 1)
	case "nfa_trans":
		atomic.AddUint64(&sc.nfaTransMisses, 1)
	}
}

// RecordFilterCheck records a filter check.
func (sc *StatsCollector) RecordFilterCheck(filterType string, hit bool) {
	switch filterType {
	case "prefix_bloom":
		atomic.AddUint64(&sc.prefixBloomChecks, 1)
		if hit {
			atomic.AddUint64(&sc.prefixBloomHits, 1)
		}
	case "trigram":
		atomic.AddUint64(&sc.trigramChecks, 1)
		if !hit {
			atomic.AddUint64(&sc.trigramSkips, 1)
		}
	}
}

// UpdateLevelStats updates level statistics.
func (sc *StatsCollector) UpdateLevelStats(level int, size int64, segments int, tombstones int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.levelSizes[level] = size
	sc.segmentCounts[level] = segments
	sc.tombstones[level] = tombstones
}

// SetManifestGeneration sets the current manifest generation.
func (sc *StatsCollector) SetManifestGeneration(gen uint64) { atomic.StoreUint64(&sc.manifestGen, gen) }

// GetStats returns the current statistics.
func (sc *StatsCollector) GetStats() Stats {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Calculate rates
	sc.calculateRates()

	// Calculate latency percentiles
	p50, p95, p99 := sc.calculatePercentiles()

	// Calculate cache hit rates
	labelHitRate := sc.calculateHitRate(sc.labelAdvanceHits, sc.labelAdvanceMisses)
	nfaHitRate := sc.calculateHitRate(sc.nfaTransHits, sc.nfaTransMisses)

	// Calculate filter rates
	bloomFPR := sc.calculateFPR(sc.prefixBloomHits, sc.prefixBloomChecks)
	trigramSkipRatio := sc.calculateSkipRatio(sc.trigramSkips, sc.trigramChecks)

	// Calculate total bytes
	var totalBytes int64
	for _, size := range sc.levelSizes {
		totalBytes += size
	}

	// Calculate tombstone fractions
	tombstoneFractions := make(map[int]float64)
	for level, tombstones := range sc.tombstones {
		if size := sc.levelSizes[level]; size > 0 {
			tombstoneFractions[level] = float64(tombstones) / float64(size)
		}
	}

	// Overall averages since start
	elapsedTotal := time.Since(sc.startTime).Seconds()
	if elapsedTotal < 1.0 {
		elapsedTotal = 1.0
	}
	totalInserts := atomic.LoadUint64(&sc.inserts)
	totalDeletes := atomic.LoadUint64(&sc.deletes)
	totalSearches := atomic.LoadUint64(&sc.searches)

	return Stats{
		LevelSizes:              copyIntInt64Map(sc.levelSizes),
		SegmentCounts:           copyIntIntMap(sc.segmentCounts),
		TombstoneFractions:      tombstoneFractions,
		TotalBytes:              totalBytes,
		LatencyP50:              p50,
		LatencyP95:              p95,
		LatencyP99:              p99,
		QueriesPerSecond:        sc.searchRate,
		WritesPerSecond:         sc.insertRate + sc.deleteRate,
		TotalInserts:            totalInserts,
		TotalDeletes:            totalDeletes,
		TotalSearches:           totalSearches,
		OverallQueriesPerSecond: float64(totalSearches) / elapsedTotal,
		OverallWritesPerSecond:  float64(totalInserts+totalDeletes) / elapsedTotal,
		LabelAdvanceHitRate:     labelHitRate,
		NFATransHitRate:         nfaHitRate,
		PrefixBloomFPR:          bloomFPR,
		TrigramSkipRatio:        trigramSkipRatio,
		ManifestGeneration:      atomic.LoadUint64(&sc.manifestGen),
	}
}

// Refresh forces a refresh of rate calculations.
func (sc *StatsCollector) Refresh() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.calculateRates()
}

// calculateRates calculates operation rates.
func (sc *StatsCollector) calculateRates() {
	now := time.Now()
	elapsed := now.Sub(sc.lastRateCalc).Seconds()
	if elapsed < 1.0 {
		elapsed = 1.0
	}

	currentInserts := atomic.LoadUint64(&sc.inserts)
	currentDeletes := atomic.LoadUint64(&sc.deletes)
	currentSearches := atomic.LoadUint64(&sc.searches)

	sc.insertRate = float64(currentInserts-sc.lastInserts) / elapsed
	sc.deleteRate = float64(currentDeletes-sc.lastDeletes) / elapsed
	sc.searchRate = float64(currentSearches-sc.lastSearches) / elapsed

	sc.lastInserts = currentInserts
	sc.lastDeletes = currentDeletes
	sc.lastSearches = currentSearches
	sc.lastRateCalc = now
}

// calculatePercentiles calculates latency percentiles.
func (sc *StatsCollector) calculatePercentiles() (p50, p95, p99 time.Duration) {
	if len(sc.latencies) == 0 {
		return
	}
	// Make a copy and sort
	sorted := make([]time.Duration, len(sc.latencies))
	copy(sorted, sc.latencies)
	// Simple bubble sort for small arrays
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	// Calculate percentiles
	n := len(sorted)
	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	if n > 0 {
		p99 = sorted[n*99/100]
		if n*99/100 >= n {
			p99 = sorted[n-1]
		}
	}
	return
}

// calculateHitRate calculates a cache hit rate.
func (sc *StatsCollector) calculateHitRate(hits, misses uint64) float64 {
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// calculateFPR calculates false positive rate.
func (sc *StatsCollector) calculateFPR(hits, checks uint64) float64 {
	if checks == 0 {
		return 0
	}
	return float64(hits) / float64(checks)
}

// calculateSkipRatio calculates skip ratio.
func (sc *StatsCollector) calculateSkipRatio(skips, checks uint64) float64 {
	if checks == 0 {
		return 0
	}
	return float64(skips) / float64(checks)
}

// copyIntInt64Map creates a copy of a map[int]int64.
func copyIntInt64Map(m map[int]int64) map[int]int64 {
	result := make(map[int]int64, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// copyIntIntMap creates a copy of a map[int]int.
func copyIntIntMap(m map[int]int) map[int]int {
	result := make(map[int]int, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
