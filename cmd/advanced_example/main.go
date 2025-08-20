package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/CVDpl/go-live-srad/pkg/srad"
	"github.com/CVDpl/go-live-srad/pkg/srad/monitoring"
)

const (
	// Dataset sizes for performance testing
	SmallDataset  = 1_000     // 1K entries
	MediumDataset = 10_000    // 10K entries
	LargeDataset  = 100_000   // 100K entries
	HugeDataset   = 1_000_000 // 1M entries
)

func main() {
	// Track program start time
	startTime := time.Now()

	// Set up context
	ctx := context.Background()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Monitor for signals in background
	go func() {
		sig := <-sigChan
		fmt.Printf("\n🚨 Program interrupted: Received signal %v\n", sig)
		fmt.Printf("📊 Program running time: %v\n", time.Since(startTime))
		showMemoryStatus()
		os.Exit(1)
	}()

	// Optional pprof: enable by setting SRAD_PPROF_ADDR (e.g., ":6060")
	if addr := os.Getenv("SRAD_PPROF_ADDR"); addr != "" {
		srv, err := monitoring.StartPprofServer(addr)
		if err == nil {
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = monitoring.StopPprofServer(ctx, srv)
				cancel()
			}()
			fmt.Printf("pprof listening on %s (set SRAD_PPROF_ADDR to disable/enable)\n", addr)
		} else {
			fmt.Printf("failed to start pprof on %s: %v\n", addr, err)
		}
	}

	// Create temporary directory
	tempDir, err := os.MkdirTemp(".", "srad-advanced-*")
	if err != nil {
		fmt.Printf("❌ Failed to create temp directory: %v\n", err)
		return
	}
	//	defer func() {
	//		fmt.Printf("\nCleaning up: %s\n", tempDir)
	//		os.RemoveAll(tempDir)
	//	}()

	fmt.Printf("SRAD Advanced Example\n")
	fmt.Printf("====================\n")
	fmt.Printf("📁 Store directory: %s\n", tempDir)
	fmt.Printf("🖥️  CPU cores: %d\n", runtime.NumCPU())
	fmt.Printf("🧮 Go version: %s\n", runtime.Version())

	// Show initial memory status
	fmt.Printf("\n📊 Initial memory status:\n")
	showMemoryStatus()

	// Run different scenarios
	scenarios := []struct {
		name string
		size int
		fn   func(context.Context, string, int) error
	}{
		{"Small Dataset Test", SmallDataset, runScenario},
		{"Medium Dataset Test", MediumDataset, runScenario},
		{"Large Dataset Test", LargeDataset, runScenario},
		{"Concurrent Operations", MediumDataset, runConcurrentScenario},
		{"Complex Regex Patterns", SmallDataset, runComplexRegexScenario},
		{"Memory Pressure Test", MediumDataset, runMemoryPressureScenario},
	}

	for i, scenario := range scenarios {
		fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
		fmt.Printf("Scenario %d/%d: %s (size: %d)\n", i+1, len(scenarios), scenario.name, scenario.size)
		fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

		scenarioDir := fmt.Sprintf("%s/scenario_%d", tempDir, i+1)
		if err := os.MkdirAll(scenarioDir, 0755); err != nil {
			fmt.Printf("❌ Failed to create scenario directory: %v\n", err)
			continue
		}

		start := time.Now()
		err := scenario.fn(ctx, scenarioDir, scenario.size)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("❌ Scenario failed: %v\n", err)
		} else {
			fmt.Printf("✅ Scenario completed in %v\n", duration)
		}

		// Show memory after each scenario
		fmt.Printf("\n📊 Memory after scenario:\n")
		showMemoryStatus()

		// Force GC between scenarios
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("✅ All scenarios completed successfully!\n")
	fmt.Printf("📊 Total runtime: %v\n", time.Since(startTime))
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
}

// runScenario runs a basic insert/search/delete scenario
func runScenario(ctx context.Context, dir string, size int) error {
	// Open store
	opts := &srad.Options{
		ReadOnly:                false,
		Parallelism:             runtime.NumCPU(),
		MemtableTargetBytes:     16 * 1024 * 1024, // 16MB
		CacheLabelAdvanceBytes:  4 * 1024 * 1024,  // 4MB
		CacheNFATransitionBytes: 4 * 1024 * 1024,  // 4MB
	}

	store, err := srad.Open(dir, opts)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer store.Close()

	// Insert phase
	fmt.Printf("  📝 Inserting %d entries...\n", size)
	insertStart := time.Now()
	samples := make([]string, 0, min(1000, size))

	for i := 0; i < size; i++ {
		entry := generateTestEntry(i)
		if err := store.Insert([]byte(entry)); err != nil {
			return fmt.Errorf("insert failed at %d: %w", i, err)
		}

		// Sample some entries for searching
		if i < 1000 || rand.Float32() < 0.01 {
			samples = append(samples, entry)
		}

		// Progress
		if i > 0 && i%(size/10) == 0 {
			fmt.Printf("    %d%% complete\n", (i*100)/size)
		}
	}

	insertDuration := time.Since(insertStart)
	insertRate := float64(size) / insertDuration.Seconds()
	fmt.Printf("  ✅ Inserted %d entries in %v (%.0f ops/s)\n", size, insertDuration, insertRate)

	// Flush to disk
	fmt.Printf("  💾 Flushing to disk...\n")
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := store.Flush(flushCtx); err != nil {
		fmt.Printf("  ⚠️  Flush failed: %v\n", err)
	}
	cancel()

	// Search phase
	fmt.Printf("  🔍 Running searches...\n")
	searchStart := time.Now()
	searchCount := min(100, len(samples))

	for i := 0; i < searchCount; i++ {
		sample := samples[i%len(samples)]
		pattern := "^" + regexp.QuoteMeta(sample) + "$"
		re := regexp.MustCompile(pattern)

		searchCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
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

		if !found {
			fmt.Printf("  ⚠️  Sample not found: %s\n", sample)
		}
	}

	searchDuration := time.Since(searchStart)
	searchRate := float64(searchCount) / searchDuration.Seconds()
	fmt.Printf("  ✅ Completed %d searches in %v (%.0f ops/s)\n", searchCount, searchDuration, searchRate)

	// Delete phase
	fmt.Printf("  🗑️  Deleting entries...\n")
	deleteStart := time.Now()
	deleteCount := min(100, len(samples))

	for i := 0; i < deleteCount; i++ {
		sample := samples[i%len(samples)]
		if err := store.Delete([]byte(sample)); err != nil {
			fmt.Printf("  ⚠️  Delete failed: %v\n", err)
		}
	}

	deleteDuration := time.Since(deleteStart)
	deleteRate := float64(deleteCount) / deleteDuration.Seconds()
	fmt.Printf("  ✅ Deleted %d entries in %v (%.0f ops/s)\n", deleteCount, deleteDuration, deleteRate)

	// Show stats
	stats := store.Stats()
	fmt.Printf("\n  📈 Store Statistics:\n")
	fmt.Printf("    • Total bytes: %d\n", stats.TotalBytes)
	fmt.Printf("    • Queries/sec: %.2f\n", stats.QueriesPerSecond)
	fmt.Printf("    • Writes/sec: %.2f\n", stats.WritesPerSecond)

	return nil
}

// runConcurrentScenario tests concurrent operations
func runConcurrentScenario(ctx context.Context, dir string, size int) error {
	opts := &srad.Options{
		ReadOnly:                false,
		Parallelism:             runtime.NumCPU(),
		MemtableTargetBytes:     32 * 1024 * 1024, // 32MB
		CacheLabelAdvanceBytes:  8 * 1024 * 1024,  // 8MB
		CacheNFATransitionBytes: 8 * 1024 * 1024,  // 8MB
	}

	store, err := srad.Open(dir, opts)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer store.Close()

	fmt.Printf("  🔀 Running %d concurrent workers...\n", runtime.NumCPU())

	var wg sync.WaitGroup
	var insertCount, searchCount, deleteCount uint64
	workers := runtime.NumCPU()
	itemsPerWorker := size / workers

	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			base := workerID * itemsPerWorker
			for i := 0; i < itemsPerWorker; i++ {
				entry := generateTestEntry(base + i)

				// Insert
				if err := store.Insert([]byte(entry)); err == nil {
					atomic.AddUint64(&insertCount, 1)
				}

				// Search
				if i%10 == 0 {
					re := regexp.MustCompile("^" + regexp.QuoteMeta(entry) + "$")
					searchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
					iter, err := store.RegexSearch(searchCtx, re, &srad.QueryOptions{
						Limit: 1,
						Mode:  srad.CountOnly,
					})
					if err == nil {
						for iter.Next(searchCtx) {
							// drain
						}
						iter.Close()
						atomic.AddUint64(&searchCount, 1)
					}
					cancel()
				}

				// Delete some
				if i%20 == 0 {
					if err := store.Delete([]byte(entry)); err == nil {
						atomic.AddUint64(&deleteCount, 1)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("  ✅ Concurrent operations completed in %v\n", duration)
	fmt.Printf("    • Inserts: %d (%.0f ops/s)\n", insertCount, float64(insertCount)/duration.Seconds())
	fmt.Printf("    • Searches: %d (%.0f ops/s)\n", searchCount, float64(searchCount)/duration.Seconds())
	fmt.Printf("    • Deletes: %d (%.0f ops/s)\n", deleteCount, float64(deleteCount)/duration.Seconds())

	return nil
}

// runComplexRegexScenario tests various regex patterns
func runComplexRegexScenario(ctx context.Context, dir string, size int) error {
	opts := srad.DefaultOptions()
	store, err := srad.Open(dir, opts)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer store.Close()

	// Insert test data
	fmt.Printf("  📝 Inserting test data...\n")
	testData := []string{
		"user:john:admin",
		"user:jane:moderator",
		"user:bob:user",
		"product:laptop:electronics:1299",
		"product:phone:electronics:899",
		"product:book:literature:29",
		"order:2024:01:12345:pending",
		"order:2024:01:12346:shipped",
		"order:2024:02:12347:delivered",
		"log:info:2024-01-01:application started",
		"log:error:2024-01-01:database connection failed",
		"log:warn:2024-01-01:high memory usage",
		"email:john@example.com:verified",
		"email:jane@test.org:pending",
		"email:bob@demo.net:verified",
	}

	for _, item := range testData {
		if err := store.Insert([]byte(item)); err != nil {
			return fmt.Errorf("insert failed: %w", err)
		}
	}

	// Test various regex patterns
	patterns := []struct {
		name    string
		pattern string
	}{
		{"Prefix match", "^user:.*"},
		{"Suffix match", ".*:admin$"},
		{"Contains", ".*electronics.*"},
		{"Alternation", "^(user|product):.*"},
		{"Character class", "^log:[iew].*"},
		{"Quantifiers", "^order:2024:0[12]:.*"},
		{"Email pattern", "^email:.*@.*\\..*:.*"},
		{"Complex", "^(user|product):.*:(admin|electronics|literature).*"},
	}

	fmt.Printf("  🔍 Testing regex patterns...\n")
	for _, p := range patterns {
		re, err := regexp.Compile(p.pattern)
		if err != nil {
			fmt.Printf("    ❌ Invalid pattern %s: %v\n", p.name, err)
			continue
		}

		searchCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		iter, err := store.RegexSearch(searchCtx, re, nil)
		if err != nil {
			fmt.Printf("    ❌ Search failed for %s: %v\n", p.name, err)
			cancel()
			continue
		}

		var matches []string
		for iter.Next(searchCtx) {
			matches = append(matches, string(iter.String()))
		}
		iter.Close()
		cancel()

		fmt.Printf("    ✅ %s: found %d matches\n", p.name, len(matches))
		if len(matches) > 0 && len(matches) <= 3 {
			for _, m := range matches {
				fmt.Printf("       • %s\n", m)
			}
		}
	}

	return nil
}

// runMemoryPressureScenario tests behavior under memory pressure
func runMemoryPressureScenario(ctx context.Context, dir string, size int) error {
	// Use smaller memory limits to test pressure scenarios
	opts := &srad.Options{
		ReadOnly:                false,
		Parallelism:             2,               // Reduce parallelism
		MemtableTargetBytes:     4 * 1024 * 1024, // 4MB - small memtable
		CacheLabelAdvanceBytes:  1 * 1024 * 1024, // 1MB
		CacheNFATransitionBytes: 1 * 1024 * 1024, // 1MB
	}

	store, err := srad.Open(dir, opts)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer store.Close()

	fmt.Printf("  💾 Testing with limited memory (4MB memtable)...\n")

	// Insert data that will force multiple flushes
	flushCount := 0
	lastStats := store.Stats()

	for i := 0; i < size; i++ {
		// Generate larger entries to fill memtable faster
		entry := generateLargeEntry(i)
		if err := store.Insert([]byte(entry)); err != nil {
			return fmt.Errorf("insert failed at %d: %w", i, err)
		}

		// Check if a flush occurred
		if i%100 == 0 {
			stats := store.Stats()
			if stats.ManifestGeneration > lastStats.ManifestGeneration {
				flushCount++
				fmt.Printf("    💾 Flush #%d triggered at entry %d\n", flushCount, i)
				lastStats = stats
			}
		}

		// Progress
		if i > 0 && i%(size/10) == 0 {
			fmt.Printf("    %d%% complete (flushes: %d)\n", (i*100)/size, flushCount)
		}
	}

	// Force final flush
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	store.Flush(flushCtx)
	cancel()

	fmt.Printf("  ✅ Completed with %d automatic flushes\n", flushCount)

	// Show memory stats
	fmt.Printf("\n  📊 Final memory pressure test stats:\n")
	showMemoryStatus()

	return nil
}

// Helper functions

func generateTestEntry(i int) string {
	types := []string{"user", "product", "order", "log", "email"}
	categories := []string{"admin", "user", "moderator", "electronics", "books", "info", "error", "warn"}

	t := types[i%len(types)]
	c := categories[i%len(categories)]

	return fmt.Sprintf("%s:%d:%s:%d", t, i, c, time.Now().Unix())
}

func generateLargeEntry(i int) string {
	// Generate entries with more data to trigger flushes
	base := generateTestEntry(i)
	padding := make([]byte, 1024) // 1KB padding
	for j := range padding {
		padding[j] = byte('a' + (j % 26))
	}
	return fmt.Sprintf("%s:data:%s", base, string(padding))
}

func showMemoryStatus() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("    • Alloc: %s\n", formatBytes(int64(m.Alloc)))
	fmt.Printf("    • HeapAlloc: %s\n", formatBytes(int64(m.HeapAlloc)))
	fmt.Printf("    • Sys: %s\n", formatBytes(int64(m.Sys)))
	fmt.Printf("    • NumGC: %d\n", m.NumGC)
	fmt.Printf("    • Goroutines: %d\n", runtime.NumGoroutine())
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * KB
		GB = MB * KB
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
	return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
