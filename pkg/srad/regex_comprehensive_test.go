package srad

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"
)

// TestRegexPatternsComprehensive tests a comprehensive set of regex patterns
func TestRegexPatternsComprehensive(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	// Insert diverse test data
	testData := []string{
		// Simple patterns
		"apple",
		"application",
		"apply",
		"banana",
		"bandana",
		"cat",
		"catalog",
		"dog",
		"doggy",

		// Numeric patterns
		"test123",
		"test456",
		"test789",
		"abc123def",
		"xyz789",

		// Special characters
		"hello-world",
		"hello_world",
		"hello.world",
		"hello@world",

		// Mixed case (stored as-is, but regex should be case-sensitive)
		"Test",
		"TEST",
		"TeSt",

		// Unicode
		"café",
		"naïve",
		"日本語",

		// Long strings
		"very-long-key-with-multiple-segments-separated-by-dashes",
		"another_long_key_with_underscores_instead_of_dashes",

		// Edge cases
		"a",
		"z",
		"aaa",
		"zzz",
	}

	ctx := context.Background()

	for _, s := range testData {
		if err := store.Insert([]byte(s)); err != nil {
			t.Fatalf("Failed to insert '%s': %v", s, err)
		}
	}

	// Flush to ensure data is in segments
	if err := store.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Test patterns
	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{
			name:     "Prefix anchor",
			pattern:  "^app.*",
			expected: []string{"apple", "application", "apply"},
		},
		{
			name:     "Suffix anchor",
			pattern:  ".*log$",
			expected: []string{"catalog"},
		},
		{
			name:     "Both anchors (exact match)",
			pattern:  "^cat$",
			expected: []string{"cat"},
		},
		{
			name:     "Character class",
			pattern:  "^test[0-9]+$",
			expected: []string{"test123", "test456", "test789"},
		},
		{
			name:     "Alternation",
			pattern:  "^(dog|cat)$",
			expected: []string{"cat", "dog"},
		},
		{
			name:     "Wildcard in middle",
			pattern:  "^hello.world$",
			expected: []string{"hello-world", "hello.world", "hello@world", "hello_world"},
		},
		{
			name:     "Optional character",
			pattern:  "^dog(gy)?$", // Fixed: doggy? means "dogg" + optional "y", not "dog" + optional "gy"
			expected: []string{"dog", "doggy"},
		},
		{
			name:     "One or more",
			pattern:  "^a+$",
			expected: []string{"a", "aaa"},
		},
		{
			name:     "Zero or more",
			pattern:  "^z*$",
			expected: []string{"z", "zzz"},
		},
		{
			name:     "Specific count",
			pattern:  "^test[0-9]{3}$",
			expected: []string{"test123", "test456", "test789"},
		},
		{
			name:     "Range count",
			pattern:  "^[a-z]{3,5}$",
			expected: []string{"aaa", "apple", "apply", "cat", "dog", "doggy", "zzz"}, // Fixed: aaa and zzz also match (3 letters)
		},
		{
			name:    "Negated character class",
			pattern: "^[^0-9]+$", // No digits
			expected: []string{
				"a", "aaa", "apple", "application", "apply",
				"banana", "bandana", "cat", "catalog", "café",
				"dog", "doggy", "hello-world", "hello.world",
				"hello@world", "hello_world", "naïve", "z", "zzz",
				"Test", "TEST", "TeSt", "日本語",
				"very-long-key-with-multiple-segments-separated-by-dashes",
				"another_long_key_with_underscores_instead_of_dashes",
			},
		},
		{
			name:     "Unicode",
			pattern:  "^café$",
			expected: []string{"café"},
		},
		{
			name:     "Case sensitive",
			pattern:  "^Test$",
			expected: []string{"Test"},
		},
		{
			name:     "Mixed alphanumeric",
			pattern:  "^[a-z]+[0-9]+[a-z]+$",
			expected: []string{"abc123def"},
		},
		{
			name:     "Hyphen separated",
			pattern:  "^[a-z]+-[a-z]+$",
			expected: []string{"hello-world"},
		},
		{
			name:     "Underscore separated",
			pattern:  "^[a-z]+_[a-z]+$",
			expected: []string{"hello_world"},
		},
		{
			name:     "Complex pattern",
			pattern:  "^(test|xyz)[0-9]{3}(def)?$",
			expected: []string{"test123", "test456", "test789", "xyz789"}, // Fixed: abc123def doesn't match (starts with "abc" not "test"/"xyz")
		},
		{
			name:     "Match all",
			pattern:  ".*",
			expected: testData, // All data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re, err := regexp.Compile(tt.pattern)
			if err != nil {
				t.Fatalf("Failed to compile regex '%s': %v", tt.pattern, err)
			}

			iter, err := store.RegexSearch(ctx, re, nil)
			if err != nil {
				t.Fatalf("Failed to search with pattern '%s': %v", tt.pattern, err)
			}
			defer iter.Close()

			var results []string
			for iter.Next(ctx) {
				results = append(results, string(iter.String()))
			}

			if err := iter.Err(); err != nil {
				t.Fatalf("Iterator error: %v", err)
			}

			// Sort for comparison
			sort.Strings(results)
			expectedSorted := make([]string, len(tt.expected))
			copy(expectedSorted, tt.expected)
			sort.Strings(expectedSorted)

			if len(results) != len(expectedSorted) {
				t.Errorf("Pattern '%s': expected %d results, got %d",
					tt.pattern, len(expectedSorted), len(results))
				t.Errorf("Expected: %v", expectedSorted)
				t.Errorf("Got:      %v", results)
				return
			}

			for i := range results {
				if results[i] != expectedSorted[i] {
					t.Errorf("Pattern '%s': result mismatch at index %d",
						tt.pattern, i)
					t.Errorf("Expected: %v", expectedSorted)
					t.Errorf("Got:      %v", results)
					break
				}
			}
		})
	}
}

// TestRegexWithDeletions tests regex search with deletions
func TestRegexWithDeletions(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert data
	keys := []string{"apple", "application", "apply", "banana", "bandana"}
	for _, k := range keys {
		if err := store.Insert([]byte(k)); err != nil {
			t.Fatalf("Failed to insert '%s': %v", k, err)
		}
	}

	// Delete some keys
	if err := store.Delete([]byte("apple")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if err := store.Delete([]byte("banana")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Search should not return deleted keys
	re := regexp.MustCompile("^(app|ban).*")
	iter, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer iter.Close()

	var results []string
	for iter.Next(ctx) {
		results = append(results, string(iter.String()))
	}

	expected := []string{"application", "apply", "bandana"}
	sort.Strings(results)
	sort.Strings(expected)

	if len(results) != len(expected) {
		t.Errorf("Expected %d results, got %d", len(expected), len(results))
		t.Errorf("Expected: %v", expected)
		t.Errorf("Got:      %v", results)
	}

	for i := range results {
		if i >= len(expected) || results[i] != expected[i] {
			t.Errorf("Result mismatch at index %d", i)
			break
		}
	}
}

// TestRegexWithTTL tests regex search with expired keys
func TestRegexWithTTL(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert with short TTL
	if err := store.InsertWithTTL([]byte("temp1"), 100*time.Millisecond); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if err := store.InsertWithTTL([]byte("temp2"), 100*time.Millisecond); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Insert without TTL
	if err := store.Insert([]byte("permanent")); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Search immediately - should find all
	re := regexp.MustCompile(".*")
	iter, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}

	count := 0
	for iter.Next(ctx) {
		count++
	}
	iter.Close()

	if count != 3 {
		t.Errorf("Expected 3 results initially, got %d", count)
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Search after expiry - should find only permanent
	iter2, err := store.RegexSearch(ctx, re, nil)
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer iter2.Close()

	var results []string
	for iter2.Next(ctx) {
		results = append(results, string(iter2.String()))
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result after expiry, got %d: %v", len(results), results)
	}

	if len(results) > 0 && results[0] != "permanent" {
		t.Errorf("Expected 'permanent', got '%s'", results[0])
	}
}

// TestRegexPerformance benchmarks regex search performance
func TestRegexPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert large dataset
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d-%s", i, []string{"alpha", "beta", "gamma", "delta"}[i%4])
		if err := store.Insert([]byte(key)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Flush to segments
	if err := store.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Test different pattern complexities
	patterns := []struct {
		name    string
		pattern string
	}{
		{"Exact", "^key-00000000-alpha$"},
		{"Prefix", "^key-0000.*"},
		{"Suffix", ".*-alpha$"},
		{"Wildcard", "key-.*-beta"},
		{"Complex", "^key-[0-9]{8}-(alpha|gamma)$"},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			re := regexp.MustCompile(p.pattern)

			start := time.Now()
			iter, err := store.RegexSearch(ctx, re, nil)
			if err != nil {
				t.Fatalf("Failed to search: %v", err)
			}
			defer iter.Close()

			count := 0
			for iter.Next(ctx) {
				count++
			}

			elapsed := time.Since(start)
			t.Logf("Pattern '%s': found %d matches in %v (%.0f matches/sec)",
				p.pattern, count, elapsed, float64(count)/elapsed.Seconds())

			if count == 0 {
				t.Errorf("Pattern '%s' should have found some matches", p.pattern)
			}
		})
	}
}

// TestRegexEmptyResults tests patterns that should return no results
func TestRegexEmptyResults(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert some data
	keys := []string{"apple", "banana", "cherry"}
	for _, k := range keys {
		if err := store.Insert([]byte(k)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Patterns that should match nothing
	patterns := []string{
		"^xyz.*",   // No keys start with xyz
		".*9999$",  // No keys end with 9999
		"^[0-9]+$", // No numeric-only keys
		"^UPPER$",  // No uppercase (case-sensitive)
	}

	for _, pattern := range patterns {
		t.Run(pattern, func(t *testing.T) {
			re := regexp.MustCompile(pattern)
			iter, err := store.RegexSearch(ctx, re, nil)
			if err != nil {
				t.Fatalf("Failed to search: %v", err)
			}
			defer iter.Close()

			count := 0
			for iter.Next(ctx) {
				count++
			}

			if count != 0 {
				t.Errorf("Pattern '%s' should return no results, got %d", pattern, count)
			}
		})
	}
}
