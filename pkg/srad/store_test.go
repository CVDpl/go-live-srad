package srad

import (
	"context"
	"regexp"
	"testing"
	"time"
)

func TestStoreBasicOperations(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Test Insert
	testData := []string{
		"apple",
		"application",
		"banana",
		"cat",
		"dog",
	}

	for _, s := range testData {
		if err := store.Insert([]byte(s)); err != nil {
			t.Errorf("Failed to insert '%s': %v", s, err)
		}
	}

	// Test Regex Search
	ctx := context.Background()
	re, err := regexp.Compile("^app.*")
	if err != nil {
		t.Fatalf("Failed to compile regex: %v", err)
	}

	iter, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer iter.Close()

	var results []string
	for iter.Next(ctx) {
		results = append(results, string(iter.String()))
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	expectedCount := 2 // "apple" and "application"
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d: %v", expectedCount, len(results), results)
	}

	// Test Delete
	if err := store.Delete([]byte("apple")); err != nil {
		t.Errorf("Failed to delete 'apple': %v", err)
	}

	// Verify deletion
	iter2, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search after delete: %v", err)
	}
	defer iter2.Close()

	var resultsAfterDelete []string
	for iter2.Next(ctx) {
		resultsAfterDelete = append(resultsAfterDelete, string(iter2.String()))
	}

	expectedCountAfterDelete := 1 // only "application"
	if len(resultsAfterDelete) != expectedCountAfterDelete {
		t.Errorf("Expected %d results after delete, got %d: %v",
			expectedCountAfterDelete, len(resultsAfterDelete), resultsAfterDelete)
	}
}

func TestStoreReadOnly(t *testing.T) {
	dir := t.TempDir()

	// First, create a store with some data
	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	if err := store.Insert([]byte("test")); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}

	store.Close()

	// Now open in read-only mode
	opts.ReadOnly = true
	roStore, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open read-only store: %v", err)
	}
	defer roStore.Close()

	// Try to insert - should fail
	err = roStore.Insert([]byte("should fail"))
	if err == nil {
		t.Error("Expected error when inserting to read-only store")
	}

	// Try to delete - should fail
	err = roStore.Delete([]byte("test"))
	if err == nil {
		t.Error("Expected error when deleting from read-only store")
	}

	// Search should work
	ctx := context.Background()
	re, _ := regexp.Compile("test")
	iter, err := roStore.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Errorf("Search failed on read-only store: %v", err)
	}
	iter.Close()
}

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()

	// Create store and insert data
	opts := DefaultOptions()
	store1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	testData := []string{"persist1", "persist2", "persist3"}
	for _, s := range testData {
		if err := store1.Insert([]byte(s)); err != nil {
			t.Errorf("Failed to insert '%s': %v", s, err)
		}
	}

	// Flush to ensure data is persisted
	ctx := context.Background()
	if err := store1.Flush(ctx); err != nil {
		t.Errorf("Failed to flush: %v", err)
	}

	store1.Close()

	// Reopen store and verify data
	store2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	re, _ := regexp.Compile("persist.*")
	iter, err := store2.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search after reopen: %v", err)
	}
	defer iter.Close()

	var results []string
	for iter.Next(ctx) {
		results = append(results, string(iter.String()))
	}

	if len(results) != len(testData) {
		t.Errorf("Expected %d persisted items, got %d: %v", len(testData), len(results), results)
	}
}

func TestStoreStats(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Insert some data
	for i := 0; i < 10; i++ {
		if err := store.Insert([]byte(string(rune('a' + i)))); err != nil {
			t.Errorf("Failed to insert: %v", err)
		}
	}

	// Get stats
	stats := store.Stats()

	// Basic validation: initial manifest version starts at 1
	if stats.ManifestGeneration != 1 {
		t.Errorf("Expected manifest generation 1, got %d", stats.ManifestGeneration)
	}

	// Refresh stats
	store.RefreshStats()

	// Stats should be accessible without error
	_ = stats.TotalBytes
	_ = stats.QueriesPerSecond
	_ = stats.WritesPerSecond
}

// Tombstone should not hide a newer reinsert (newest wins)
func TestTombstoneThenReinsertVisible(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	key := []byte("foo")
	if err := store.Insert(key); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := store.Delete(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := store.Insert(key); err != nil {
		t.Fatalf("reinsert: %v", err)
	}

	ctx := context.Background()
	re := regexp.MustCompile("^foo$")
	it, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	defer it.Close()
	if !it.Next(ctx) {
		t.Fatalf("expected to find reinserted key")
	}
}

// Expired entries should not be returned via LOUDS path
func TestExpiryHiddenInLOUDS(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	key := []byte("bar")
	if err := store.InsertWithTTL(key, 50*time.Millisecond); err != nil {
		t.Fatalf("insert ttl: %v", err)
	}
	if err := store.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	time.Sleep(80 * time.Millisecond)

	ctx := context.Background()
	re := regexp.MustCompile("^bar$")
	it, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	defer it.Close()
	if it.Next(ctx) {
		t.Fatalf("expired key should not be returned")
	}
}

func TestStoreLimits(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Test empty key
	err = store.Insert([]byte{})
	if err == nil {
		t.Error("Expected error when inserting empty key")
	}

	// Test very large key (over 1MB)
	largeKey := make([]byte, 1024*1024+1)
	for i := range largeKey {
		largeKey[i] = 'a'
	}

	err = store.Insert(largeKey)
	if err == nil {
		t.Error("Expected error when inserting key larger than 1MB")
	}
}

func TestStoreQueryOptions(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Insert test data
	for i := 0; i < 10; i++ {
		key := []byte("test" + string(rune('0'+i)))
		if err := store.Insert(key); err != nil {
			t.Errorf("Failed to insert: %v", err)
		}
	}

	// Test with limit
	ctx := context.Background()
	re, _ := regexp.Compile("test.*")

	queryOpts := &QueryOptions{
		Limit: 3,
		Mode:  EmitStrings,
	}

	iter, err := store.RegexSearch(ctx, re, queryOpts)
	if err != nil {
		t.Fatalf("Failed to search with options: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next(ctx) {
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 results with limit, got %d", count)
	}
}

func TestStoreConcurrency(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Concurrent inserts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			key := []byte("concurrent" + string(rune('0'+n)))
			if err := store.Insert(key); err != nil {
				t.Errorf("Failed to insert: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all inserts
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all data was inserted
	ctx := context.Background()
	re, _ := regexp.Compile("concurrent.*")
	iter, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next(ctx) {
		count++
	}

	if count != 10 {
		t.Errorf("Expected 10 concurrent inserts, got %d", count)
	}
}

func TestStoreClose(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	// Close store
	if err := store.Close(); err != nil {
		t.Errorf("Failed to close store: %v", err)
	}

	// Operations after close should fail
	err = store.Insert([]byte("should fail"))
	if err == nil {
		t.Error("Expected error when inserting to closed store")
	}

	// Double close should be safe
	if err := store.Close(); err != nil {
		t.Errorf("Double close failed: %v", err)
	}
}

func TestStoreTimeout(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	store, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Insert some data
	if err := store.Insert([]byte("timeout test")); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}

	// Search with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond) // Ensure timeout

	re, _ := regexp.Compile(".*")
	iter, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		// This is expected due to timeout
		t.Logf("Search failed as expected: %v", err)
		return
	}
	defer iter.Close()

	// Try to iterate - should fail due to context cancellation
	if iter.Next(ctx) {
		if err := iter.Err(); err == nil {
			t.Error("Expected error due to context cancellation")
		}
	}
}
