package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/CVDpl/go-live-srad/pkg/srad"
	"github.com/CVDpl/go-live-srad/pkg/srad/monitoring"
)

func main() {
	// Create a temporary directory for the example
	tempDir, err := os.MkdirTemp(".", "srad-example-*")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func() {
		fmt.Printf("\nStore data persisted in: %s\n", tempDir)
		fmt.Println("Remove with: rm -rf", tempDir)
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

	fmt.Printf("SRAD Example\n")
	fmt.Printf("============\n")
	fmt.Printf("Using temporary directory: %s\n\n", tempDir)

	// Create store options
	opts := &srad.Options{
		ReadOnly:                false,
		Parallelism:             4,
		VerifyChecksumsOnLoad:   true,
		MemtableTargetBytes:     1024 * 1024, // 1MB
		CacheLabelAdvanceBytes:  64 * 1024,   // 64KB
		CacheNFATransitionBytes: 64 * 1024,   // 64KB
	}

	// Open the store
	fmt.Println("1. Opening store...")
	store, err := srad.Open(tempDir, opts)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()
	fmt.Println("   ✓ Store opened successfully")

	// Insert sample data
	fmt.Println("\n2. Inserting sample data...")
	sampleData := []string{
		"user:john:admin",
		"user:jane:moderator",
		"user:bob:user",
		"user:alice:admin",
		"user:charlie:user",
		"product:laptop:electronics",
		"product:phone:electronics",
		"product:book:literature",
		"product:chair:furniture",
		"product:table:furniture",
		"order:12345:pending",
		"order:12346:shipped",
		"order:12347:delivered",
		"order:12348:cancelled",
		"order:12349:pending",
	}

	for _, item := range sampleData {
		if err := store.Insert([]byte(item)); err != nil {
			log.Printf("Warning: Failed to insert %q: %v", item, err)
		} else {
			fmt.Printf("   ✓ Inserted: %s\n", item)
		}
	}

	// Delete some items
	fmt.Println("\n3. Deleting some items...")
	itemsToDelete := []string{"user:charlie:user", "order:12348:cancelled"}
	for _, item := range itemsToDelete {
		if err := store.Delete([]byte(item)); err != nil {
			log.Printf("Warning: Failed to delete %q: %v", item, err)
		} else {
			fmt.Printf("   ✓ Deleted: %s\n", item)
		}
	}

	// Perform regex searches
	fmt.Println("\n4. Performing regex searches...")
	ctx := context.Background()
	queryOpts := &srad.QueryOptions{
		Limit: 100,
		Mode:  srad.EmitStrings,
		Order: srad.DFS,
	}

	// Search for admin users
	fmt.Println("\n   Searching for admin users...")
	adminRegex := regexp.MustCompile(`^user:.*:admin$`)
	adminIter, err := store.RegexSearch(ctx, adminRegex, queryOpts)
	if err != nil {
		log.Printf("Warning: Failed to search for admin users: %v", err)
	} else {
		var adminUsers []string
		for adminIter.Next(ctx) {
			adminUsers = append(adminUsers, string(adminIter.String()))
		}
		if err := adminIter.Err(); err != nil {
			log.Printf("Warning: Iterator error: %v", err)
		}
		adminIter.Close()
		fmt.Printf("   ✓ Found %d admin users: %v\n", len(adminUsers), adminUsers)
	}

	// Search for electronics products
	fmt.Println("\n   Searching for electronics products...")
	electronicsRegex := regexp.MustCompile(`^product:.*:electronics$`)
	electronicsIter, err := store.RegexSearch(ctx, electronicsRegex, queryOpts)
	if err != nil {
		log.Printf("Warning: Failed to search for electronics: %v", err)
	} else {
		var electronics []string
		for electronicsIter.Next(ctx) {
			electronics = append(electronics, string(electronicsIter.String()))
		}
		if err := electronicsIter.Err(); err != nil {
			log.Printf("Warning: Iterator error: %v", err)
		}
		electronicsIter.Close()
		fmt.Printf("   ✓ Found %d electronics: %v\n", len(electronics), electronics)
	}

	// Search for pending orders
	fmt.Println("\n   Searching for pending orders...")
	pendingRegex := regexp.MustCompile(`^order:.*:pending$`)
	pendingIter, err := store.RegexSearch(ctx, pendingRegex, queryOpts)
	if err != nil {
		log.Printf("Warning: Failed to search for pending orders: %v", err)
	} else {
		var pendingOrders []string
		for pendingIter.Next(ctx) {
			pendingOrders = append(pendingOrders, string(pendingIter.String()))
		}
		if err := pendingIter.Err(); err != nil {
			log.Printf("Warning: Iterator error: %v", err)
		}
		pendingIter.Close()
		fmt.Printf("   ✓ Found %d pending orders: %v\n", len(pendingOrders), pendingOrders)
	}

	// Test persistence
	fmt.Println("\n5. Testing persistence...")
	fmt.Println("   Flushing to disk...")
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := store.Flush(flushCtx); err != nil {
		log.Printf("Warning: Failed to flush: %v", err)
	} else {
		fmt.Println("   ✓ Data flushed to disk")
	}

	// Get store statistics
	fmt.Println("\n6. Store statistics...")
	stats := store.Stats()
	fmt.Printf("   Total bytes: %d\n", stats.TotalBytes)
	fmt.Printf("   Manifest generation: %d\n", stats.ManifestGeneration)
	fmt.Printf("   Queries per second: %.2f\n", stats.QueriesPerSecond)
	fmt.Printf("   Writes per second: %.2f\n", stats.WritesPerSecond)

	// Print concise level 0 stats
	fmt.Printf("   Level 0 size: %d\n", stats.LevelSizes[0])
	fmt.Printf("   Level 0 segments: %d\n", stats.SegmentCounts[0])

	// Tune store parameters
	fmt.Println("\n7. Tuning store parameters...")
	memtableTarget := int64(2 * 1024 * 1024) // 2MB
	cacheLabelAdvance := int64(128 * 1024)   // 128KB
	cacheNFATransition := int64(128 * 1024)  // 128KB

	tuningParams := srad.TuningParams{
		MemtableTargetBytes:     &memtableTarget,
		CacheLabelAdvanceBytes:  &cacheLabelAdvance,
		CacheNFATransitionBytes: &cacheNFATransition,
	}

	store.Tune(tuningParams)
	fmt.Println("   ✓ Store parameters tuned")

	// Force compaction (stub for now)
	fmt.Println("\n8. Triggering compaction...")
	compactCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := store.CompactNow(compactCtx); err != nil {
		log.Printf("Warning: Failed to trigger compaction: %v", err)
	} else {
		fmt.Println("   ✓ Compaction triggered")
	}

	// Wait a bit for background processes
	time.Sleep(500 * time.Millisecond)

	// Post-compaction stats (overall)
	post := store.Stats()
	fmt.Printf("   Post-compaction overall QPS: %.2f\n", post.OverallQueriesPerSecond)
	fmt.Printf("   Post-compaction overall WPS: %.2f\n", post.OverallWritesPerSecond)

	// Check if segments were created
	segmentsDir := filepath.Join(tempDir, "segments")
	if _, err := os.Stat(segmentsDir); err == nil {
		fmt.Printf("   ✓ Segments directory created: %s\n", segmentsDir)
	} else {
		fmt.Println("   ℹ Segments directory not yet created (normal for small datasets)")
	}

	// Final statistics (overall averages since start)
	fmt.Println("\n9. Final statistics...")
	finalStats := store.Stats()
	fmt.Printf("   Final manifest generation: %d\n", finalStats.ManifestGeneration)
	fmt.Printf("   Overall queries per second: %.2f\n", finalStats.OverallQueriesPerSecond)
	fmt.Printf("   Overall writes per second: %.2f\n", finalStats.OverallWritesPerSecond)
	fmt.Printf("   Total inserts: %d, deletes: %d, searches: %d\n", finalStats.TotalInserts, finalStats.TotalDeletes, finalStats.TotalSearches)

	// Test reopening store
	fmt.Println("\n10. Testing store persistence...")
	store.Close()

	// Reopen store
	store2, err := srad.Open(tempDir, opts)
	if err != nil {
		log.Printf("Warning: Failed to reopen store: %v", err)
	} else {
		fmt.Println("   ✓ Store reopened successfully")

		// Verify data persisted
		verifyRegex := regexp.MustCompile(`^user:john:admin$`)
		verifyIter, err := store2.RegexSearch(ctx, verifyRegex, queryOpts)
		if err != nil {
			log.Printf("Warning: Failed to verify data: %v", err)
		} else {
			found := false
			for verifyIter.Next(ctx) {
				found = true
				break
			}
			verifyIter.Close()

			if found {
				fmt.Println("   ✓ Data successfully persisted and recovered")
			} else {
				fmt.Println("   ⚠ Data not found after reopening")
			}
		}

		store2.Close()
	}

	fmt.Println("\n✅ Example completed successfully!")
}
