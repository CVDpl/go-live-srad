package memtable

import (
	"context"
	"regexp"
	"testing"
)

// BenchmarkIteratorOptimizations benchmarks the optimized iterator performance
func BenchmarkIteratorOptimizations(b *testing.B) {
	// Create a memtable with test data
	m := New()

	// Insert test data with various patterns
	for i := 0; i < 10000; i++ {
		key := []byte("test-pattern-" + string(rune('a'+(i%26))) + "-" + string(rune('0'+(i%10))))
		m.Insert(key)
	}

	// Test patterns that will benefit from optimizations
	testPatterns := []string{
		"^test-pattern-a-0$",          // Exact match - should benefit from anchor optimization
		"^test-pattern-[a-z]-[0-9]$",  // Character classes - should benefit from NFA optimization
		"test-pattern-a.*",            // Prefix + wildcard - should benefit from literal prefix
		"test-pattern-[a-z]{1}-[0-9]", // Complex pattern - should benefit from all optimizations
	}

	for _, patternStr := range testPatterns {
		pattern := regexp.MustCompile(patternStr)

		b.Run("Pattern_"+patternStr, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter := m.RegexSearch(pattern)
				ctx := context.Background()

				count := 0
				for iter.Next(ctx) {
					count++
				}

				// Get optimization statistics
				stats := iter.GetStats()
				b.ReportMetric(float64(stats["pathsSkipped"]), "paths_skipped")
				b.ReportMetric(float64(stats["cacheHits"]), "cache_hits")
				b.ReportMetric(float64(stats["nfaChecks"]), "nfa_checks")
				b.ReportMetric(float64(stats["literalChecks"]), "literal_checks")

				iter.Close()
			}
		})
	}
}

// BenchmarkIteratorCacheEffectiveness benchmarks cache hit rates
func BenchmarkIteratorCacheEffectiveness(b *testing.B) {
	m := New()

	// Insert data with repeating patterns
	for i := 0; i < 1000; i++ {
		key := []byte("cache-test-" + string(rune('a'+(i%26))) + "-" + string(rune('0'+(i%10))))
		m.Insert(key)
	}

	// Test pattern that will generate many cache hits
	pattern := regexp.MustCompile("^cache-test-[a-z]-[0-9]$")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := m.RegexSearch(pattern)
		ctx := context.Background()

		// Reset stats before iteration
		iter.ResetStats()

		count := 0
		for iter.Next(ctx) {
			count++
		}

		// Report cache effectiveness
		stats := iter.GetStats()
		cacheHitRate := float64(stats["cacheHits"]) / float64(stats["pathsChecked"])
		b.ReportMetric(cacheHitRate, "cache_hit_rate")

		iter.Close()
	}
}

// BenchmarkIteratorNFAOptimization benchmarks NFA-based pruning
func BenchmarkIteratorNFAOptimization(b *testing.B) {
	m := New()

	// Insert data with complex patterns
	for i := 0; i < 5000; i++ {
		key := []byte("nfa-test-" + string(rune('a'+(i%26))) + "-" + string(rune('0'+(i%10))) + "-complex")
		m.Insert(key)
	}

	// Test patterns that heavily use NFA optimization
	testPatterns := []string{
		"^nfa-test-[a-z]-[0-9]-complex$", // Character classes + anchors
		"nfa-test-[a-z]{1}-[0-9]{1}-.*",  // Quantified character classes
		"^nfa-test-[a-z]+-[0-9]+-.*$",    // One or more quantifiers
	}

	for _, patternStr := range testPatterns {
		pattern := regexp.MustCompile(patternStr)

		b.Run("NFA_"+patternStr, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter := m.RegexSearch(pattern)
				ctx := context.Background()

				iter.ResetStats()

				count := 0
				for iter.Next(ctx) {
					count++
				}

				// Report NFA optimization effectiveness
				stats := iter.GetStats()
				pathsSkipped := stats["pathsSkipped"]
				totalPaths := stats["pathsChecked"]
				skipRate := float64(pathsSkipped) / float64(totalPaths)

				b.ReportMetric(skipRate, "skip_rate")
				b.ReportMetric(float64(pathsSkipped), "paths_skipped")

				iter.Close()
			}
		})
	}
}

// BenchmarkIteratorLiteralPrefixOptimization benchmarks literal prefix optimization
func BenchmarkIteratorLiteralPrefixOptimization(b *testing.B) {
	m := New()

	// Insert data with common prefixes
	for i := 0; i < 3000; i++ {
		key := []byte("literal-prefix-" + string(rune('a'+(i%26))) + "-" + string(rune('0'+(i%10))))
		m.Insert(key)
	}

	// Test patterns with literal prefixes
	testPatterns := []string{
		"^literal-prefix-a-0$",       // Exact literal match
		"literal-prefix-[a-z]-[0-9]", // Literal prefix + character classes
		"literal-prefix-a.*",         // Literal prefix + wildcard
		"literal-prefix-[a-z]+.*",    // Literal prefix + quantifier + wildcard
	}

	for _, patternStr := range testPatterns {
		pattern := regexp.MustCompile(patternStr)

		b.Run("Literal_"+patternStr, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter := m.RegexSearch(pattern)
				ctx := context.Background()

				iter.ResetStats()

				count := 0
				for iter.Next(ctx) {
					count++
				}

				// Report literal optimization effectiveness
				stats := iter.GetStats()
				literalChecks := stats["literalChecks"]
				totalPaths := stats["pathsChecked"]
				literalRate := float64(literalChecks) / float64(totalPaths)

				b.ReportMetric(literalRate, "literal_check_rate")
				b.ReportMetric(float64(literalChecks), "literal_checks")

				iter.Close()
			}
		})
	}
}
