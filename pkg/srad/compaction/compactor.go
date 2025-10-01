package compaction

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/pkg/srad/manifest"
	"github.com/CVDpl/go-live-srad/pkg/srad/segment"
)

// Compactor manages LSM compaction.
type Compactor struct {
	mu       sync.Mutex
	dir      string
	manifest *manifest.Manifest

	alloc func() uint64

	// Configuration
	levelRatio     int   // Size ratio between levels (default 10)
	maxL0Files     int   // Max files in L0 before compaction (default 4)
	maxSegmentSize int64 // Max segment size in bytes

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	logger common.Logger

	// Optional: configure each segment.Builder before use (filters, LOUDS, trie, GC, etc.)
	configureBuilder func(*segment.Builder)
}

// CompactionPlan describes a compaction task.
type CompactionPlan struct {
	IsTrivialMove bool                   // True if we can just move a single L0 file to L1
	Level         int                    // Level to compact from
	TargetLevel   int                    // Level to compact to
	Inputs        []manifest.SegmentInfo // Segments to compact
	Overlaps      []manifest.SegmentInfo // Overlapping segments in target level
	Reason        string
}

// NewCompactor creates a new compactor.
func NewCompactor(dir string, m *manifest.Manifest, logger common.Logger, alloc func() uint64) *Compactor {
	if logger == nil {
		logger = common.NewNullLogger()
	}

	if alloc == nil {
		alloc = func() uint64 { return uint64(time.Now().UnixNano()) }
	}

	return &Compactor{
		dir:            dir,
		manifest:       m,
		levelRatio:     10,
		maxL0Files:     8,
		maxSegmentSize: 512 * 1024 * 1024, // 512MB
		logger:         logger,
		alloc:          alloc,
	}
}

// Start starts the compaction background process.
func (c *Compactor) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx != nil {
		return // Already running
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
}

// Stop stops the compaction process.
func (c *Compactor) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
		c.ctx = nil
		c.cancel = nil
	}
}

// Plan decides if a compaction is needed and returns a plan.
// If no compaction is needed, it returns nil.
func (c *Compactor) Plan() *CompactionPlan {
	c.mu.Lock()
	defer c.mu.Unlock()

	segments := c.manifest.GetActiveSegments()

	// 1. Priority: L0 to L1 compaction
	l0Segments := filterByLevel(segments, 0)
	if len(l0Segments) >= c.maxL0Files {
		return &CompactionPlan{
			Level:       0,
			TargetLevel: 1,
			Inputs:      l0Segments,
			Overlaps:    findOverlaps(l0Segments, filterByLevel(segments, 1)),
			Reason:      fmt.Sprintf("L0 has %d files (trigger is %d)", len(l0Segments), c.maxL0Files),
		}
	}
	// Fallback: compact L0 when at least 2 files to reduce file count even below threshold
	if len(l0Segments) >= 2 {
		return &CompactionPlan{
			Level:       0,
			TargetLevel: 1,
			Inputs:      l0Segments,
			Overlaps:    findOverlaps(l0Segments, filterByLevel(segments, 1)),
			Reason:      fmt.Sprintf("L0 has %d files (fallback >=2)", len(l0Segments)),
		}
	}

	// 2. Leveled compaction for L1+
	for level := 1; level < common.MaxLevel-1; level++ {
		levelSegments := filterByLevel(segments, level)
		if len(levelSegments) == 0 {
			continue
		}

		targetSize := int64(c.levelRatio) * calculateTotalSize(filterByLevel(segments, level-1))
		if targetSize == 0 {
			targetSize = c.maxSegmentSize // Base case
		}
		currentSize := calculateTotalSize(levelSegments)

		if currentSize > targetSize {
			// Pick a segment to compact (e.g., the oldest one)
			sort.Slice(levelSegments, func(i, j int) bool {
				return levelSegments[i].Created < levelSegments[j].Created
			})
			segmentToCompact := levelSegments[0]

			return &CompactionPlan{
				Level:       level,
				TargetLevel: level + 1,
				Inputs:      []manifest.SegmentInfo{segmentToCompact},
				Overlaps:    findOverlaps([]manifest.SegmentInfo{segmentToCompact}, filterByLevel(segments, level+1)),
				Reason:      fmt.Sprintf("L%d size is %dMB, exceeds target %dMB", level, currentSize/(1024*1024), targetSize/(1024*1024)),
			}
		}
	}

	return nil // No compaction needed
}

// Execute runs a compaction job described by the plan.
// It returns the list of new segment readers and an error if any.
func (c *Compactor) Execute(plan *CompactionPlan) ([]*segment.Reader, error) {
	if plan == nil {
		return nil, fmt.Errorf("compaction plan is nil")
	}
	startTime := time.Now()
	c.logger.Info("starting compaction", "reason", plan.Reason, "level", plan.Level, "inputs", len(plan.Inputs), "overlaps", len(plan.Overlaps))

	inputIDs := make([]uint64, 0, len(plan.Inputs)+len(plan.Overlaps))
	for _, s := range plan.Inputs {
		inputIDs = append(inputIDs, s.ID)
	}
	for _, s := range plan.Overlaps {
		inputIDs = append(inputIDs, s.ID)
	}

	segmentsDir := filepath.Join(c.dir, common.DirSegments)
	var readers []*segment.Reader
	for _, segID := range inputIDs {
		// false for checksum verification, assuming it's already been checked on load
		reader, err := segment.NewReader(segID, segmentsDir, c.logger, false)
		if err != nil {
			// Clean up already opened readers
			for _, r := range readers {
				r.Close()
			}
			return nil, fmt.Errorf("failed to open segment reader for %d: %w", segID, err)
		}
		readers = append(readers, reader)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	if len(readers) == 0 {
		return nil, nil // Nothing to compact
	}

	// The rest of the logic is a k-way merge, similar to the legacy function.
	// This part can be refactored into a helper if it gets complex.
	return c.mergeSegments(readers, inputIDs, plan, startTime)
}

// mergeSegments performs the k-way merge of multiple segment readers.
func (c *Compactor) mergeSegments(readers []*segment.Reader, inputIDs []uint64, plan *CompactionPlan, startTime time.Time) ([]*segment.Reader, error) {
	// newest first, so newest wins
	sort.Slice(readers, func(i, j int) bool {
		mi := readers[i].GetMetadata()
		mj := readers[j].GetMetadata()
		if mi == nil || mj == nil {
			return false
		}
		return mi.CreatedAtUnix > mj.CreatedAtUnix
	})

	targetLevel := plan.TargetLevel
	if plan.Level == 0 && len(plan.Inputs) < c.maxL0Files*2 {
		targetLevel = 1
	}

	type chunkState struct {
		builder       *segment.Builder
		id            uint64
		approxBytes   int64
		acceptedCount int64
	}
	var cur chunkState
	outputs := make([]uint64, 0, 8)
	segmentsDir := filepath.Join(c.dir, common.DirSegments)

	ensureBuilder := func() {
		if cur.builder != nil {
			return
		}
		// Fallback alloc if not configured
		if c.alloc == nil {
			cur.id = uint64(time.Now().UnixNano())
		} else {
			cur.id = c.alloc()
		}
		cur.builder = segment.NewBuilder(cur.id, targetLevel, segmentsDir, c.logger)
		if c.configureBuilder != nil {
			c.configureBuilder(cur.builder)
		}
		// Compaction produces keys in strictly sorted order; enable fast-paths.
		cur.builder.MarkAlreadySorted()
		cur.approxBytes = 0
		cur.acceptedCount = 0
	}
	// Collect segment infos for atomic manifest update
	var pendingSegmentInfos []manifest.SegmentInfo

	finalizeChunk := func() error {
		if cur.builder == nil {
			return nil
		}
		ctx := context.Background()
		if c.ctx != nil {
			ctx = c.ctx
		}
		md, err := cur.builder.BuildWithContext(ctx)
		if err != nil {
			return fmt.Errorf("build segment: %w", err)
		}
		info := manifest.SegmentInfo{ID: cur.id, Level: targetLevel, NumKeys: md.Counts.Accepted}
		if minb, err := md.GetMinKey(); err == nil {
			info.MinKeyHex = fmt.Sprintf("%x", minb)
		}
		if maxb, err := md.GetMaxKey(); err == nil {
			info.MaxKeyHex = fmt.Sprintf("%x", maxb)
		}
		if md2, err := segment.LoadFromFile(filepath.Join(segmentsDir, fmt.Sprintf("%016d", cur.id), "segment.json")); err == nil {
			var total int64
			files := []string{md2.Files.Louds, md2.Files.Edges, md2.Files.Accept, md2.Files.TMap, md2.Files.Tails}
			for _, f := range files {
				if f == "" {
					continue
				}
				p := filepath.Join(segmentsDir, fmt.Sprintf("%016d", cur.id), f)
				if st, err := os.Stat(p); err == nil {
					total += st.Size()
				}
			}
			for _, f := range []string{"filters/prefix.bf", "filters/tri.bits", "keys.dat", "tombstones.dat"} {
				p := filepath.Join(segmentsDir, fmt.Sprintf("%016d", cur.id), f)
				if st, err := os.Stat(p); err == nil {
					total += st.Size()
				}
			}
			info.Size = total
		}
		// Don't add to manifest yet - collect for atomic update
		pendingSegmentInfos = append(pendingSegmentInfos, info)
		outputs = append(outputs, cur.id)
		cur.builder = nil
		cur.id = 0
		cur.approxBytes = 0
		cur.acceptedCount = 0
		return nil
	}
	maybeFlush := func() error {
		if cur.approxBytes >= c.maxSegmentSize {
			return finalizeChunk()
		}
		return nil
	}

	type srcIter struct {
		key   []byte
		ok    bool
		next  func() ([]byte, bool)
		order int
		tomb  bool
	}

	// Min-heap emulation using sorted slice for small N
	h := make([]*srcIter, 0, len(readers)*2)
	less := func(i, j int) bool {
		cmp := bytes.Compare(h[i].key, h[j].key)
		if cmp != 0 {
			return cmp < 0
		}
		return h[i].order > h[j].order
	}
	push := func(si *srcIter) {
		if si != nil && si.ok {
			h = append(h, si)
			sort.Slice(h, less)
		}
	}
	pop := func() *srcIter {
		if len(h) == 0 {
			return nil
		}
		x := h[0]
		h = h[1:]
		return x
	}

	// Track closers to ensure cleanup
	var closers []func()
	defer func() {
		for _, closer := range closers {
			closer()
		}
	}()

	for idx, r := range readers {
		order := len(readers) - idx // newer first has higher order
		advK, closeK := r.StreamKeys()
		closers = append(closers, closeK)
		if k, ok := advK(); ok {
			push(&srcIter{key: k, ok: true, next: advK, order: order, tomb: false})
		}
		advT, closeT := r.StreamTombstones()
		closers = append(closers, closeT)
		if tk, ok := advT(); ok {
			push(&srcIter{key: tk, ok: true, next: advT, order: order, tomb: true})
		}
	}

	for len(h) > 0 {
		si := pop()
		curKey := append([]byte(nil), si.key...)
		bestOrder := si.order
		bestTomb := si.tomb
		group := []*srcIter{si}
		// Drain all entries with the same key
		for len(h) > 0 && bytes.Equal(h[0].key, curKey) {
			gj := pop()
			group = append(group, gj)
			if gj.order > bestOrder {
				bestOrder = gj.order
				bestTomb = gj.tomb
			}
		}
		// Advance all group members
		for _, gi := range group {
			if nk, ok := gi.next(); ok {
				gi.key = nk
				gi.ok = true
				push(gi)
			}
		}
		// Emit based on newest record type
		ensureBuilder()
		if bestTomb {
			cur.builder.AddTombstone(curKey)
			cur.approxBytes += int64(len(curKey))
		} else {
			if err := cur.builder.Add(curKey, curKey, false); err != nil {
				return nil, fmt.Errorf("builder add: %w", err)
			}
			cur.approxBytes += int64(len(curKey))
			cur.acceptedCount++
		}
		if err := maybeFlush(); err != nil {
			return nil, err
		}
		if c.ctx != nil {
			select {
			case <-c.ctx.Done():
				return nil, c.ctx.Err()
			default:
			}
		}
	}
	if err := finalizeChunk(); err != nil {
		return nil, err
	}

	if len(outputs) == 0 {
		c.logger.Info("compaction produced no output segments", "reason", plan.Reason)
		// If no output, we still need to remove old segments from manifest
		if err := c.manifest.RemoveSegments(inputIDs); err != nil {
			return nil, fmt.Errorf("remove old segments from manifest: %w", err)
		}
		return []*segment.Reader{}, nil
	}

	// ATOMIC COMPACTION UPDATE: Add all new segments and remove old ones atomically
	c.logger.Info("performing atomic compaction manifest update", "new_segments", len(pendingSegmentInfos), "removing_segments", len(inputIDs))

	// Add new segments first
	if err := c.manifest.AddSegments(pendingSegmentInfos); err != nil {
		c.logger.Error("failed to add new segments to manifest - cleaning up segment files", "error", err)
		// Clean up segment files since manifest update failed
		cleanupFailed := 0
		for _, segID := range outputs {
			segmentPath := filepath.Join(segmentsDir, fmt.Sprintf("%016d", segID))
			if err := os.RemoveAll(segmentPath); err != nil {
				cleanupFailed++
				c.logger.Warn("failed to clean up segment file after manifest failure", "id", segID, "path", segmentPath, "error", err)
			} else {
				c.logger.Info("cleaned up segment file after manifest failure", "id", segID, "path", segmentPath)
			}
		}
		if cleanupFailed > 0 {
			c.logger.Error("cleanup incomplete - some orphaned segment files may remain", "failed_cleanups", cleanupFailed)
		}
		return nil, fmt.Errorf("atomic compaction manifest update failed: %w", err)
	}

	// Remove old segments after successful addition of new ones
	if err := c.manifest.RemoveSegments(inputIDs); err != nil {
		c.logger.Error("failed to remove old segments from manifest after adding new ones", "error", err, "new_segments", outputs, "old_segments", inputIDs)
		// This is a problematic state, new segments exist and old ones are not marked for deletion.
		// Log extensively for recovery purposes but continue - the system is still functional
		return nil, fmt.Errorf("remove old segments from manifest: %w", err)
	}

	compactionInfo := manifest.CompactionInfo{
		ID:        uint64(startTime.UnixNano()),
		Level:     plan.Level,
		Inputs:    inputIDs,
		Outputs:   outputs,
		StartTime: startTime.Unix(),
		EndTime:   time.Now().Unix(),
		Reason:    plan.Reason,
	}
	if err := c.manifest.RecordCompaction(compactionInfo); err != nil {
		c.logger.Warn("failed to record compaction", "error", err)
	}

	c.logger.Info("atomic compaction manifest update completed", "new_segments", len(outputs), "removed_segments", len(inputIDs))

	c.logger.Info("compaction completed", "reason", plan.Reason, "duration", time.Since(startTime), "outputs", len(outputs))

	// Create readers for the new segments
	newReaders := make([]*segment.Reader, 0, len(outputs))
	for _, outID := range outputs {
		reader, err := segment.NewReader(outID, segmentsDir, c.logger, true) // Verify checksums on new segments
		if err != nil {
			// Clean up already created readers
			for _, r := range newReaders {
				r.Close()
			}
			return nil, fmt.Errorf("failed to open new segment reader %d: %w", outID, err)
		}
		newReaders = append(newReaders, reader)
	}

	return newReaders, nil
}

// TriggerCompaction manually triggers compaction.
func (c *Compactor) TriggerCompaction() {
	plan := c.Plan()
	if plan == nil {
		c.logger.Info("manual compaction: no work to do")
		return
	}
	if _, err := c.Execute(plan); err != nil {
		c.logger.Warn("manual compaction failed", "error", err)
	}
}

// SetBuilderConfigurator sets a callback that configures each new segment.Builder the compactor creates.
func (c *Compactor) SetBuilderConfigurator(fn func(*segment.Builder)) { c.configureBuilder = fn }

// Helper functions

func findOverlaps(inputs []manifest.SegmentInfo, candidates []manifest.SegmentInfo) []manifest.SegmentInfo {
	if len(inputs) == 0 || len(candidates) == 0 {
		return nil
	}

	// Get min/max key range for the input set
	var minKey, maxKey []byte
	for _, s := range inputs {
		minB, _ := hex.DecodeString(s.MinKeyHex)
		maxB, _ := hex.DecodeString(s.MaxKeyHex)
		if len(minB) > 0 && (minKey == nil || bytes.Compare(minB, minKey) < 0) {
			minKey = minB
		}
		if len(maxB) > 0 && (maxKey == nil || bytes.Compare(maxB, maxKey) > 0) {
			maxKey = maxB
		}
	}

	if minKey == nil || maxKey == nil {
		return candidates // No range info, assume all overlap
	}

	var overlaps []manifest.SegmentInfo
	for _, cand := range candidates {
		candMin, _ := hex.DecodeString(cand.MinKeyHex)
		candMax, _ := hex.DecodeString(cand.MaxKeyHex)
		if len(candMin) == 0 || len(candMax) == 0 {
			overlaps = append(overlaps, cand) // No range info, assume overlap
			continue
		}

		// Check for range intersection: !(candMax < minKey || candMin > maxKey)
		if !(bytes.Compare(candMax, minKey) < 0 || bytes.Compare(candMin, maxKey) > 0) {
			overlaps = append(overlaps, cand)
		}
	}
	return overlaps
}

func filterByLevel(segments []manifest.SegmentInfo, level int) []manifest.SegmentInfo {
	var result []manifest.SegmentInfo
	for _, seg := range segments {
		if seg.Level == level {
			result = append(result, seg)
		}
	}
	return result
}

func calculateTotalSize(segments []manifest.SegmentInfo) int64 {
	var total int64
	for _, seg := range segments {
		total += seg.Size
	}
	return total
}
