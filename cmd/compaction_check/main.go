package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"io/fs"
	"regexp"
	"time"

	"github.com/CVDpl/go-live-srad/pkg/srad"
	"github.com/CVDpl/go-live-srad/pkg/srad/manifest"
)

func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func countLive(ctx context.Context, store srad.Store) (int, error) {
	re := regexp.MustCompile(".*")
	iter, err := store.RegexSearch(ctx, re, srad.DefaultQueryOptions())
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	cnt := 0
	for iter.Next(ctx) {
		cnt++
	}
	return cnt, iter.Err()
}

func main() {
	n := flag.Int("n", 10000, "number of inserts")
	m := flag.Int("m", 5000, "number of deletes from the first N keys")
	k := flag.Int("k", 0, "number of overwrites among surviving keys")
	keep := flag.Bool("keep", false, "keep the output directory and print its path")
	outDir := flag.String("out", "", "output directory to use; if empty a temp dir will be created")
	phaseTimeout := flag.Duration("timeout", 10*time.Minute, "timeout per phase (flush/scan/compact)")
	waitRCU := flag.Duration("wait_rcu", 30*time.Second, "wait this long after compaction before listing files (to allow RCU cleanup)")
	verifyScan := flag.Bool("verify", false, "perform a full scan to count live keys (slow)")
	pruneWAL := flag.Bool("prune_wal", true, "prune old WAL files after flush/compaction")
	purge := flag.Bool("purge", false, "physically remove non-active segment dirs after compaction (dangerous)")
	assertEmpty := flag.Bool("assert_empty", true, "exit with non-zero status if any live entries remain after compaction")
	rotateWAL := flag.Bool("rotate_wal", true, "rotate WAL on flush to enable immediate pruning of older WAL files")
	flag.Parse()

	var dir string
	if *outDir != "" {
		// Use explicit output directory
		dir = *outDir
		if err := os.MkdirAll(dir, 0755); err != nil {
			panic(err)
		}
	} else {
		// Create a temporary working directory
		d, err := os.MkdirTemp(".", "srad-compact-*")
		if err != nil {
			panic(err)
		}
		dir = d
	}
	// Remove the directory only if it was temporary and user did not request keeping it
	if !*keep && *outDir == "" {
		defer os.RemoveAll(dir)
	}
	fmt.Printf("output dir: %s\n", dir)

	opts := srad.DefaultOptions()
	// Stabilize environment for measurement: avoid background interference
	opts.DisableBackgroundCompaction = true
	opts.DisableAutotuner = true
	opts.RotateWALOnFlush = *rotateWAL
	store, err := srad.Open(dir, opts)
	if err != nil {
		panic(err)
	}

	// 1) Inserts
	for i := 0; i < *n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		if err := store.Insert(key); err != nil {
			panic(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *phaseTimeout)
	if err := store.Flush(ctx); err != nil {
		panic(err)
	}
	cancel()

	// 2) Deletes: delete first m keys (bounded)
	if *m > *n {
		*m = *n
	}
	for i := 0; i < *m; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		if err := store.Delete(key); err != nil {
			panic(err)
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), *phaseTimeout)
	if err := store.Flush(ctx); err != nil {
		panic(err)
	}
	cancel()

	// 3) Overwrites among surviving keys
	survivors := *n - *m
	if *k > survivors {
		*k = survivors
	}
	// overwrite last k survivors: indices [n-k, n) skipping deleted range
	oLeft := *k
	for i := *n - 1; i >= 0 && oLeft > 0; i-- {
		if i < *m { // deleted
			continue
		}
		key := []byte(fmt.Sprintf("key-%05d", i))
		if err := store.Insert(key); err != nil { // overwrite same key
			panic(err)
		}
		oLeft--
	}
	if *k > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *phaseTimeout)
		if err := store.Flush(ctx); err != nil {
			panic(err)
		}
		cancel()
	}

	// Optionally prune WAL after finishing writes and flushes
	if *pruneWAL {
		if err := store.PruneWAL(); err != nil {
			fmt.Println("PruneWAL error:", err)
		}
	}

	// Measure before compaction
	segDir := filepath.Join(dir, "segments")
	// Active size from manifest (ignores obsolete dirs pending RCU)
	beforeActive := activeBytes(dir)
	// Optional verification scan for live keys
	liveBefore := *n - *m
	if *verifyScan {
		ctx, cancel = context.WithTimeout(context.Background(), *phaseTimeout)
		lb, err := countLive(ctx, store)
		if err != nil {
			panic(err)
		}
		cancel()
		liveBefore = lb
	}
	tombBefore := *n - liveBefore

	// Debug: list files before compaction
	fmt.Println("-- segments before --")
	listDirRecursive(segDir)

	// Compact
	ctx, cancel = context.WithTimeout(context.Background(), *phaseTimeout)
	if err := store.CompactNow(ctx); err != nil {
		panic(err)
	}
	cancel()

	// Optionally prune WAL after compaction
	if *pruneWAL {
		if err := store.PruneWAL(); err != nil {
			fmt.Println("PruneWAL error:", err)
		}
	}

	// Optionally wait for RCU cleanup to run
	if *waitRCU > 0 {
		time.Sleep(*waitRCU)
	}

	// Compute sizes after compaction
	afterActive := activeBytes(dir)
	// Optional purge of obsolete segment directories for clarity
	if *purge {
		purgeObsoleteSegments(dir)
	}

	// Optional verification scan after compaction
	liveAfter := *n - *m
	scannedAfter := -1
	if *verifyScan || *assertEmpty {
		ctx, cancel = context.WithTimeout(context.Background(), *phaseTimeout)
		la, err := countLive(ctx, store)
		if err != nil {
			panic(err)
		}
		cancel()
		liveAfter = la
		scannedAfter = la
	}
	tombAfter := *n - liveAfter

	// Debug: list files after compaction
	fmt.Println("-- segments after --")
	listDirRecursive(segDir)

	_ = store.Close()

	// Assertions
	if *assertEmpty {
		if scannedAfter < 0 {
			ctx, cancel = context.WithTimeout(context.Background(), *phaseTimeout)
			la, err := countLive(ctx, store)
			if err != nil {
				panic(err)
			}
			cancel()
			liveAfter = la
		}
		if liveAfter > 0 {
			fmt.Printf("assert_empty failed: %d live entries remain after compaction\n", liveAfter)
			os.Exit(1)
		}
		fmt.Println("assert_empty passed: no live entries remain")
	}

	// Report
	fmt.Printf("n=%d m=%d k=%d\n", *n, *m, *k)
	fmt.Printf("live before: %d, tombstones (logical) before: %d\n", liveBefore, tombBefore)
	fmt.Printf("live after:  %d, tombstones (logical) after:  %d\n", liveAfter, tombAfter)
	// Also show active manifest bytes vs filesystem bytes
	fsAfter, _ := dirSize(segDir)
	fmt.Printf("active size before: %d bytes\n", beforeActive)
	fmt.Printf("active size after:  %d bytes\n", afterActive)
	fmt.Printf("fs size (raw) after: %d bytes\n", fsAfter)
	if beforeActive > 0 {
		reduction := float64(beforeActive-afterActive) / float64(beforeActive) * 100.0
		fmt.Printf("reduction: %.2f%%\n", reduction)
	}
}

// listDirRecursive prints a recursive listing of files under root with sizes.
func listDirRecursive(root string) {
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		fi, _ := d.Info()
		fmt.Printf("FILE %s (%d bytes)\n", path, fi.Size())
		return nil
	})
}

// activeBytes sums sizes of active segments from manifest.
func activeBytes(storeDir string) int64 {
	m, err := manifest.New(storeDir, nil)
	if err != nil {
		return 0
	}
	var total int64
	for _, seg := range m.GetActiveSegments() {
		total += seg.Size
	}
	return total
}

// purgeObsoleteSegments removes segment directories not present in manifest active set.
func purgeObsoleteSegments(storeDir string) {
	m, err := manifest.New(storeDir, nil)
	if err != nil {
		return
	}
	active := make(map[string]struct{})
	for _, seg := range m.GetActiveSegments() {
		active[fmt.Sprintf("%016d", seg.ID)] = struct{}{}
	}
	segDir := filepath.Join(storeDir, "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if _, ok := active[name]; ok {
			continue
		}
		_ = os.RemoveAll(filepath.Join(segDir, name))
	}
}
