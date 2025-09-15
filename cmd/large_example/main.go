package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CVDpl/go-live-srad/pkg/srad"
	"github.com/CVDpl/go-live-srad/pkg/srad/monitoring"
)

func main() {
	// Flags
	var (
		storeDir       = flag.String("store", "", "Directory for the store (default: create ./srad-large-<ts>)")
		numEntriesFlag = flag.Int64("n", 100_000, "Number of entries to generate (default: 100k)")
		sampleCount    = flag.Int("sample", 1000, "How many entries to remember for searches")
		parallelism    = flag.Int("parallelism", runtime.NumCPU(), "Store parallelism")
		memtableMB     = flag.Int("memtable_mb", 128, "Memtable target size in MB (default: 128)")
		cacheMB        = flag.Int("cache_mb", 32, "Cache sizes (label advance / NFA) in MB each (default: 32)")
		progressEvery  = flag.Int64("progress_every", 10_000, "Print progress every N inserts (default: 10k)")
		flushEvery     = flag.Int64("flush_every", 50_000, "Force flush every N inserts (default: 50k)")

		benchSearch    = flag.Bool("search_bench", true, "Run short parallel search benchmark after inserts")
		benchWorkers   = flag.Int("search_workers", runtime.NumCPU(), "Parallel search workers")
		benchDuration  = flag.Duration("search_duration", 3*time.Second, "Parallel search benchmark duration")
		benchMaxPar    = flag.Int("search_max_parallel", runtime.NumCPU(), "MaxParallelism per query")
		benchPrefixLen = flag.Int("search_prefix_len", 8, "Prefix length for anchored prefix patterns (^prefix.*)")
		seed           = flag.Int64("seed", time.Now().UnixNano(), "RNG seed")
		keepDir        = flag.Bool("keep", false, "Keep store directory after completion")
		rcuCleanup     = flag.Bool("rcu_cleanup", false, "Enable RCU cleanup during run (can affect I/O and benchmarks)")
	)
	flag.Parse()

	// Optional pprof: enable by flag or env SRAD_PPROF_ADDR (e.g., ":6060")
	pprofAddr := os.Getenv("SRAD_PPROF_ADDR")
	if pprofAddr != "" {
		srv, err := monitoring.StartPprofServer(pprofAddr)
		if err == nil {
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = monitoring.StopPprofServer(ctx, srv)
				cancel()
			}()
			fmt.Printf("pprof listening on %s (set SRAD_PPROF_ADDR to disable/enable)\n", pprofAddr)
		} else {
			fmt.Printf("failed to start pprof on %s: %v\n", pprofAddr, err)
		}
	}

	// Prepare store directory
	if *storeDir == "" {
		*storeDir = fmt.Sprintf("./srad-large-%d", time.Now().Unix())
	}
	if err := os.MkdirAll(*storeDir, 0o755); err != nil {
		fmt.Printf("Failed to create store dir: %v\n", err)
		return
	}

	if !*keepDir {
		defer func() {
			fmt.Printf("\nRemoving store directory: %s\n", *storeDir)
			os.RemoveAll(*storeDir)
		}()
	}

	// Open store
	opts := srad.DefaultOptions()
	opts.Logger = srad.NewNullLogger()
	opts.ReadOnly = false
	opts.Parallelism = *parallelism
	opts.VerifyChecksumsOnLoad = false
	opts.MemtableTargetBytes = int64(*memtableMB) * 1024 * 1024
	opts.CacheLabelAdvanceBytes = int64(*cacheMB) * 1024 * 1024
	opts.CacheNFATransitionBytes = int64(*cacheMB) * 1024 * 1024
	opts.EnableRCU = *rcuCleanup
	opts.RCUCleanupInterval = 60 * time.Second
	// WAL tuning
	opts.RotateWALOnFlush = true
	opts.WALRotateSize = 1 << 30
	opts.WALMaxFileSize = 1 << 30
	opts.WALBufferSize = 1 << 20
	opts.WALFlushEveryBytes = 8 << 20
	// Filters and build
	opts.PrefixBloomFPR = 0.005
	opts.PrefixBloomMaxPrefixLen = 24
	opts.EnableTrigramFilter = true
	opts.BuildMaxShards = runtime.NumCPU() * 2
	opts.BuildShardMinKeys = 200000
	opts.BuildRangePartitions = 16
	opts.AsyncFilterBuild = true
	opts.AutoDisableLOUDSMinKeys = 5_000_000

	store, err := srad.Open(*storeDir, opts)
	if err != nil {
		fmt.Printf("Failed to open store: %v\n", err)
		return
	}
	defer store.Close()

	// Background WAL pruning: every 30 minutes
	stopPrune := make(chan struct{})
	go func() {
		t := time.NewTicker(30 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = store.PruneWAL()
			case <-stopPrune:
				return
			}
		}
	}()
	defer close(stopPrune)

	// Memory before
	runtime.GC()
	memBefore := readMem()
	fmt.Printf("SRAD Large Example\n")
	fmt.Printf("==================\n")
	fmt.Printf("Store dir: %s\n", *storeDir)
	fmt.Printf("Entries: %d, Sample: %d, Parallelism: %d, Memtable: %d MB, Cache: %d MB\n",
		*numEntriesFlag, *sampleCount, *parallelism, *memtableMB, *cacheMB)
	fmt.Printf("Memory BEFORE: alloc=%s, heapAlloc=%s, sys=%s, numGC=%d\n",
		formatBytes(int64(memBefore.Alloc)), formatBytes(int64(memBefore.HeapAlloc)),
		formatBytes(int64(memBefore.Sys)), memBefore.NumGC)

	// Generate and insert
	words := defaultWords()
	rnd := rand.New(rand.NewSource(*seed))
	// Track total and progress window separately to avoid skew
	totalStart := time.Now()
	windowStart := totalStart
	samples := make([]string, 0, *sampleCount)
	var seen int64

	ctx := context.Background()

	fmt.Printf("\nInserting %d entries...\n", *numEntriesFlag)

	for i := int64(0); i < *numEntriesFlag; i++ {
		entry := generateEntry(i, words, rnd)

		if err := store.Insert([]byte(entry)); err != nil {
			fmt.Printf("Warning: insert failed at %d: %v\n", i, err)
		}

		// Reservoir sampling for search queries
		seen++
		if len(samples) < *sampleCount {
			samples = append(samples, entry)
		} else if rnd.Int63n(seen) < int64(*sampleCount) {
			idx := rnd.Intn(*sampleCount)
			samples[idx] = entry
		}

		// Optional forced flush
		if *flushEvery > 0 && i > 0 && i%*flushEvery == 0 {
			flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = store.Flush(flushCtx)
			cancel()
		}

		// Progress
		if *progressEvery > 0 && i > 0 && i%*progressEvery == 0 {
			elapsed := time.Since(windowStart)
			ips := float64(*progressEvery) / elapsed.Seconds()
			fmt.Printf("Progress: %d/%d (%.1f%%) | last %d in %v (%.0f inserts/s)\n",
				i, *numEntriesFlag, float64(i)/float64(*numEntriesFlag)*100.0,
				*progressEvery, elapsed, ips)
			windowStart = time.Now()
		}
	}

	totalTime := time.Since(totalStart)
	fmt.Printf("\nInserted %d entries in %v (%.0f inserts/s)\n",
		*numEntriesFlag, totalTime, float64(*numEntriesFlag)/totalTime.Seconds())

	// Final flush
	fmt.Printf("\nFinal flush...\n")
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_ = store.Flush(flushCtx)
	cancel()
	store.RefreshStats()

	// Optional compaction before benchmark to reduce segment count
	fmt.Printf("\nCompacting...\n")
	if *benchSearch {
		cmpCtx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = store.CompactNow(cmpCtx)
		ccancel()
		store.RefreshStats()
	}

	// Memory after
	runtime.GC()
	memAfter := readMem()
	fmt.Printf("\nMemory AFTER: alloc=%s, heapAlloc=%s, sys=%s, numGC=%d\n",
		formatBytes(int64(memAfter.Alloc)), formatBytes(int64(memAfter.HeapAlloc)),
		formatBytes(int64(memAfter.Sys)), memAfter.NumGC)

	// Disk usage breakdown
	fmt.Printf("\nDisk usage by structure:\n")
	byCat, total := diskUsageByStructure(*storeDir)
	keys := make([]string, 0, len(byCat))
	for k := range byCat {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-32s %12s\n", k+":", formatBytes(byCat[k]))
	}
	fmt.Printf("  %-32s %12s\n", "TOTAL:", formatBytes(total))

	// Verification searches
	if len(samples) > *sampleCount {
		samples = samples[:*sampleCount]
	}
	if len(samples) == 0 {
		fmt.Printf("No samples collected; skipping searches.\n")
		return
	}

	fmt.Printf("\nRunning %d verification searches...\n", min(100, len(samples)))
	var (
		minDur time.Duration
		maxDur time.Duration
		sumDur time.Duration
		count  int
	)
	minDur = time.Hour

	for i, s := range samples {
		if i >= 100 {
			break
		}

		pattern := "^" + regexp.QuoteMeta(s) + "$"
		re := regexp.MustCompile(pattern)
		searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		startQ := time.Now()

		iter, err := store.RegexSearch(searchCtx, re, &srad.QueryOptions{
			Limit: 1,
			Mode:  srad.CountOnly,
		})
		if err != nil {
			cancel()
			continue
		}

		found := false
		for iter.Next(searchCtx) {
			found = true
			break
		}
		iter.Close()
		cancel()

		dur := time.Since(startQ)
		if dur < minDur {
			minDur = dur
		}
		if dur > maxDur {
			maxDur = dur
		}
		sumDur += dur
		count++

		if !found {
			fmt.Printf("  ⚠ Sample not found: %s\n", s)
		}

		if (i+1)%20 == 0 {
			fmt.Printf("  verified %d/%d\n", i+1, min(100, len(samples)))
		}
	}

	if count > 0 {
		avg := time.Duration(int64(sumDur) / int64(count))
		fmt.Printf("Search timings: fastest=%v, slowest=%v, average=%v\n", minDur, maxDur, avg)
	}

	// Optional: short parallel search benchmark
	if *benchSearch && len(samples) > 0 {
		fmt.Printf("\nRunning parallel search benchmark (%d workers, %v)...\n",
			*benchWorkers, *benchDuration)

		// Build a list of anchored prefix patterns from samples
		prefixes := buildPrefixes(samples, *benchPrefixLen)
		if len(prefixes) == 0 {
			fmt.Println("No prefixes to benchmark.")
			return
		}

		// Precompile regexps to avoid heavy compile cost in the hot loop
		patterns := make([]*regexp.Regexp, len(prefixes))
		for i, p := range prefixes {
			patterns[i] = regexp.MustCompile("^" + regexp.QuoteMeta(p) + ".*")
		}

		var stop int32
		var totalOps uint64
		wg := &sync.WaitGroup{}
		ctxBench, cancelBench := context.WithTimeout(context.Background(), *benchDuration)
		startBench := time.Now()

		for i := 0; i < *benchWorkers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(*seed + int64(id)))
				for atomic.LoadInt32(&stop) == 0 {
					select {
					case <-ctxBench.Done():
						return
					default:
					}

					re := patterns[rnd.Intn(len(patterns))]
					q := &srad.QueryOptions{
						Mode:           srad.CountOnly,
						Limit:          256,
						MaxParallelism: *benchMaxPar,
					}

					// Per-query timeout increased to 1s
					qctx, qcancel := context.WithTimeout(ctxBench, 1*time.Second)
					iter, err := store.RegexSearch(qctx, re, q)
					if err != nil {
						qcancel()
						continue
					}

					for iter.Next(qctx) {
						// drain
					}
					iter.Close()
					qcancel()
					atomic.AddUint64(&totalOps, 1)
				}
			}(i)
		}

		<-ctxBench.Done()
		atomic.StoreInt32(&stop, 1)
		wg.Wait()
		cancelBench()

		elapsed := time.Since(startBench)
		opRate := float64(totalOps) / elapsed.Seconds()
		stats := store.Stats()
		fmt.Printf("Benchmark done: ops=%d in %v (%.1f ops/s) | QPS: %.1f, P95: %v, P99: %v\n",
			totalOps, elapsed, opRate, stats.QueriesPerSecond, stats.LatencyP95, stats.LatencyP99)
	}

	// Final stats (overall averages and totals)
	fmt.Printf("\nFinal Store Statistics:\n")
	finalStats := store.Stats()
	fmt.Printf("  Manifest generation: %d\n", finalStats.ManifestGeneration)
	fmt.Printf("  Total bytes: %s\n", formatBytes(finalStats.TotalBytes))
	fmt.Printf("  Overall queries per second: %.2f\n", finalStats.OverallQueriesPerSecond)
	fmt.Printf("  Overall writes per second: %.2f\n", finalStats.OverallWritesPerSecond)
	fmt.Printf("  Total inserts: %d, deletes: %d, searches: %d\n", finalStats.TotalInserts, finalStats.TotalDeletes, finalStats.TotalSearches)
	fmt.Printf("  Level 0 size: %s, Level 0 segments: %d\n", formatBytes(finalStats.LevelSizes[0]), finalStats.SegmentCounts[0])

	if *keepDir {
		fmt.Printf("\n✅ Store data persisted in: %s\n", *storeDir)
	}
}

// generateEntry produces a realistic-looking entry composed from dictionary words.
func generateEntry(i int64, words []string, rnd *rand.Rand) string {
	// Helper to pick a word
	word := func() string { return words[rnd.Intn(len(words))] }

	switch i % 10 {
	case 0:
		// email
		return fmt.Sprintf("%s.%s+%d@%s.com", word(), word(), i, word())
	case 1:
		// url
		return fmt.Sprintf("https://%s.com/%s/%s/%d", word(), word(), word(), i)
	case 2:
		// key style
		return fmt.Sprintf("%s:%s:%s:%d", word(), word(), word(), i)
	case 3:
		// log-like
		return fmt.Sprintf("level=INFO ts=%d msg=\"%s %s %s\" id=%d",
			time.Now().Unix(), word(), word(), word(), i)
	case 4:
		// phrase
		return fmt.Sprintf("%s %s %s %s", word(), word(), word(), word())
	case 5:
		// urn-like
		return fmt.Sprintf("urn:%s:%d:%s:%s", word(), i, word(), word())
	case 6:
		// csv-like
		return fmt.Sprintf("%s,%s,%s,%d", word(), word(), word(), i)
	case 7:
		// phone-ish
		area := 100 + int(i%900)
		pre := 100 + int((i/3)%900)
		suf := 1000 + int((i/7)%9000)
		return fmt.Sprintf("+1-%03d-%03d-%04d", area, pre, suf)
	case 8:
		// path
		return fmt.Sprintf("/var/log/%s/%s/%d.log", word(), word(), i)
	default:
		// json-like
		return fmt.Sprintf("{\"type\":\"%s\",\"msg\":\"%s %s\",\"id\":%d}",
			word(), word(), word(), i)
	}
}

func defaultWords() []string {
	return []string{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
		"kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee", "zulu",
		"orange", "apple", "banana", "grape", "mango", "peach", "pear", "plum", "berry", "lemon",
		"lion", "tiger", "bear", "eagle", "shark", "whale", "wolf", "fox", "owl", "falcon",
		"red", "blue", "green", "yellow", "purple", "black", "white", "gray", "brown", "pink",
		"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
		"north", "south", "east", "west", "center", "left", "right", "up", "down", "middle",
	}
}

// diskUsageByStructure walks the store directory and sums sizes per logical category.
func diskUsageByStructure(root string) (map[string]int64, int64) {
	byCat := make(map[string]int64)
	var total int64

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		sz := info.Size()
		total += sz
		cat := categorizeFile(path)
		byCat[cat] += sz
		return nil
	})

	return byCat, total
}

func categorizeFile(path string) string {
	p := filepath.ToSlash(path)
	base := filepath.Base(p)

	if strings.Contains(p, "/wal/") {
		return "wal"
	}
	if strings.Contains(p, "/segments/") {
		return "segments"
	}
	if strings.Contains(p, "/manifest/") {
		return "manifest"
	}
	if base == "CURRENT" {
		return "manifest/CURRENT"
	}
	if base == "tuning.json" {
		return "tuning"
	}
	return "other"
}

// Memory helpers
func readMem() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// Formatting helpers
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * KB
		GB = MB * KB
		TB = GB * KB
	)
	if bytes < KB {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < MB {
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	}
	if bytes < GB {
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	}
	if bytes < TB {
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	}
	return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
}

// buildPrefixes builds distinct prefixes of given length from provided strings.
func buildPrefixes(samples []string, n int) []string {
	seen := make(map[string]struct{}, len(samples))
	out := make([]string, 0, len(samples))
	for _, s := range samples {
		if len(s) < n {
			continue
		}
		p := s[:n]
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
		if len(out) >= 1000 { // cap to avoid huge prefix set
			break
		}
	}
	if len(out) == 0 && len(samples) > 0 {
		// fallback: use full strings
		for i, s := range samples {
			out = append(out, s)
			if i >= 1000 {
				break
			}
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
