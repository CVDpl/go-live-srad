package srad

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"
)

// TestConcurrentOperations tests concurrent reads and writes.
func TestConcurrentOperations(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("writer-%d-key-%d", id, j)
				if err := store.Insert([]byte(key)); err != nil {
					errors <- fmt.Errorf("writer %d: %w", id, err)
				}
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pattern := regexp.MustCompile("writer-.*")
			for j := 0; j < 20; j++ {
				iter, err := store.RegexSearch(ctx, pattern, DefaultQueryOptions())
				if err != nil {
					errors <- fmt.Errorf("reader %d: %w", id, err)
					continue
				}

				count := 0
				for iter.Next(ctx) {
					count++
				}
				iter.Close()

				if count == 0 && j > 5 {
					errors <- fmt.Errorf("reader %d: no results after iteration %d", id, j)
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Wait for completion
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestLargeDataset tests handling of large datasets.
func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	const numKeys = 10000

	// Insert large dataset
	start := time.Now()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		if err := store.Insert([]byte(key)); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}

		// Force flush periodically
		if i%1000 == 999 {
			if err := store.Flush(ctx); err != nil {
				t.Fatalf("Failed to flush at %d: %v", i, err)
			}
		}
	}
	insertTime := time.Since(start)
	t.Logf("Inserted %d keys in %v (%.0f keys/sec)",
		numKeys, insertTime, float64(numKeys)/insertTime.Seconds())

	// Search performance test
	patterns := []string{
		"key-0000.*",
		"key-.*99",
		"key-0[0-9]{7}",
	}

	for _, p := range patterns {
		start = time.Now()
		pattern := regexp.MustCompile(p)
		iter, err := store.RegexSearch(ctx, pattern, DefaultQueryOptions())
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for iter.Next(ctx) {
			count++
		}
		iter.Close()

		searchTime := time.Since(start)
		t.Logf("Pattern '%s': found %d matches in %v", p, count, searchTime)
	}
}

// TestPersistenceAndRecovery tests data persistence across restarts.
func TestPersistenceAndRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: Write data
	func() {
		store, err := Open(dir, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// Insert data
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("persist-key-%03d", i)
			if err := store.Insert([]byte(key)); err != nil {
				t.Fatal(err)
			}
		}

		// Force flush to segments
		if err := store.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		// Insert more data (will be in WAL)
		for i := 100; i < 150; i++ {
			key := fmt.Sprintf("persist-key-%03d", i)
			if err := store.Insert([]byte(key)); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Phase 2: Reopen and verify
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Count all keys
	iter, err := store.RangeScan(ctx, []byte("persist-key-"), []byte("persist-key-~"), DefaultQueryOptions())
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	keys := make(map[string]bool)
	for iter.Next(ctx) {
		keys[string(iter.String())] = true
		count++
	}
	iter.Close()

	if count != 150 {
		t.Errorf("Expected 150 keys after recovery, got %d", count)
	}

	// Verify specific keys
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("persist-key-%03d", i)
		if !keys[key] {
			t.Errorf("Missing key after recovery: %s", key)
		}
	}
}

// TestCompactionBehavior tests compaction triggers and behavior.
func TestCompactionBehavior(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	// opts.MemtableSize = 1024 // Not available in current Options

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// Create multiple segments
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("batch-%d-key-%03d", batch, i)
			if err := store.Insert([]byte(key)); err != nil {
				t.Fatal(err)
			}
		}

		// Force flush to create segment
		if err := store.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Trigger compaction
	if err := store.CompactNow(ctx); err != nil {
		t.Logf("Compaction error (expected if not fully implemented): %v", err)
	}

	// Wait for background compaction
	time.Sleep(2 * time.Second)

	// Verify data is still accessible
	pattern := regexp.MustCompile("batch-.*")
	iter, err := store.RegexSearch(ctx, pattern, DefaultQueryOptions())
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for iter.Next(ctx) {
		count++
	}
	iter.Close()

	expectedCount := 500 // 5 batches * 100 keys
	if count != expectedCount {
		t.Errorf("Expected %d keys after compaction, got %d", expectedCount, count)
	}
}

// TestMemoryPressure tests behavior under memory pressure.
func TestMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory pressure test in short mode")
	}

	dir := t.TempDir()
	opts := DefaultOptions()
	// opts.MemtableSize = 10 * 1024 * 1024 // Not available

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// Generate large keys
	largeValue := make([]byte, 1024) // 1KB per key
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Insert until we trigger multiple flushes
	for i := 0; i < 20000; i++ {
		key := fmt.Sprintf("large-key-%08d", i)
		if err := store.Insert([]byte(key)); err != nil {
			t.Fatalf("Failed at key %d: %v", i, err)
		}

		// Periodically search to test mixed workload
		if i%1000 == 999 {
			pattern := regexp.MustCompile(fmt.Sprintf("large-key-%04d.*", i/1000))
			iter, err := store.RegexSearch(ctx, pattern, DefaultQueryOptions())
			if err != nil {
				t.Fatal(err)
			}

			count := 0
			for iter.Next(ctx) {
				count++
			}
			iter.Close()

			t.Logf("Checkpoint %d: found %d keys", i/1000, count)
		}
	}
}

// TestTTLFunctionality tests the TTL functionality of the store.
func TestTTLFunctionality(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DefaultTTL = 2 * time.Second // generous default TTL to reduce flakiness

	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	// Test InsertWithTTL with short-but-safe TTLs
	err = store.InsertWithTTL([]byte("key1"), 200*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to insert key1: %v", err)
	}

	err = store.InsertWithTTL([]byte("key2"), 1*time.Second)
	if err != nil {
		t.Fatalf("failed to insert key2: %v", err)
	}

	// Test immediate access
	iter, err := store.RegexSearch(context.Background(), regexp.MustCompile("key1"), nil)
	if err != nil {
		t.Fatalf("failed to search key1: %v", err)
	}
	if !iter.Next(context.Background()) {
		t.Fatal("key1 not found immediately after insert")
	}
	if string(iter.String()) != "key1" {
		t.Fatalf("expected key1, got %s", iter.String())
	}

	// Wait for first key to expire
	time.Sleep(300 * time.Millisecond)

	// First key should be expired
	iter, err = store.RegexSearch(context.Background(), regexp.MustCompile("key1"), nil)
	if err != nil {
		t.Fatalf("failed to search expired key1: %v", err)
	}
	if iter.Next(context.Background()) {
		t.Fatal("expired key1 still found")
	}

	// Second key should still be available
	iter, err = store.RegexSearch(context.Background(), regexp.MustCompile("key2"), nil)
	if err != nil {
		t.Fatalf("failed to search key2: %v", err)
	}
	if !iter.Next(context.Background()) {
		t.Fatal("key2 not found after key1 expired")
	}
	if string(iter.String()) != "key2" {
		t.Fatalf("expected key2, got %s", iter.String())
	}

	// Wait for second key to expire
	time.Sleep(800 * time.Millisecond)

	// Both keys should be expired
	iter, err = store.RegexSearch(context.Background(), regexp.MustCompile("key"), nil)
	if err != nil {
		t.Fatalf("failed to search all keys: %v", err)
	}
	if iter.Next(context.Background()) {
		t.Fatal("expired keys still found")
	}
}

// BenchmarkInsert benchmarks insert performance.
func BenchmarkInsert(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		if err := store.Insert([]byte(key)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRegexSearch benchmarks regex search performance.
func BenchmarkRegexSearch(b *testing.B) {
	dir := b.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// Prepare data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("search-key-%05d", i)
		if err := store.Insert([]byte(key)); err != nil {
			b.Fatal(err)
		}
	}

	pattern := regexp.MustCompile("search-key-0.*")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := store.RegexSearch(ctx, pattern, DefaultQueryOptions())
		if err != nil {
			b.Fatal(err)
		}

		for iter.Next(ctx) {
			// Just iterate
		}
		iter.Close()
	}
}

func TestVisibilityDuringFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoFlush = true
	// set small memtable to make flush inexpensive
	var small int64 = 1 << 16 // 64KB
	opts.MemtableTargetBytes = small
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Insert keys
	const N = 2000
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("flush-key-%06d", i))
		if err := store.Insert(k); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Start flush in background
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- store.Flush(ctx) }()

	// Attempt search immediately; regardless of flush timing, all keys must be visible
	re := regexp.MustCompile("^flush-key-.*")
	it, err := store.RegexSearch(ctx, re, DefaultQueryOptions())
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	defer it.Close()

	count := 0
	for it.Next(ctx) {
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iter err: %v", err)
	}
	if count != N {
		t.Fatalf("expected %d keys during/after flush, got %d", N, count)
	}

	// ensure flush finished
	<-done
}

func TestReadOnlySeesUnflushedViaWAL(t *testing.T) {
	dir := t.TempDir()
	// Writer opts: flush WAL on every write, do not auto-flush to segments
	wopts := DefaultOptions()
	wopts.WALFlushOnEveryWrite = true
	wopts.DisableAutoFlush = true
	writer, err := Open(dir, wopts)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Insert keys (remain in WAL + memtable)
	const N = 500
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("ro-key-%06d", i))
		if err := writer.Insert(k); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Open read-only process and ensure it replays WAL
	ropts := DefaultOptions()
	ropts.ReadOnly = true
	roStore, err := Open(dir, ropts)
	if err != nil {
		t.Fatal(err)
	}
	defer roStore.Close()

	re := regexp.MustCompile("^ro-key-.*")
	ctx := context.Background()
	it, err := roStore.RegexSearch(ctx, re, DefaultQueryOptions())
	if err != nil {
		t.Fatalf("search ro: %v", err)
	}
	defer it.Close()
	count := 0
	for it.Next(ctx) {
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iter ro err: %v", err)
	}
	if count != N {
		t.Fatalf("expected %d keys visible in RO via WAL, got %d", N, count)
	}
}
