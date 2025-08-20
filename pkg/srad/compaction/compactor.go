package compaction

import (
	"bytes"
	"context"
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

	// Configuration
	levelRatio     int   // Size ratio between levels (default 10)
	maxL0Files     int   // Max files in L0 before compaction (default 4)
	maxSegmentSize int64 // Max segment size in bytes

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// State
	running            bool
	pendingCompactions []CompactionJob

	logger common.Logger
}

// CompactionJob describes a compaction task.
type CompactionJob struct {
	ID       uint64
	Level    int
	Inputs   []uint64 // Segment IDs to compact
	Priority int      // Higher priority = more urgent
	Reason   string
}

// NewCompactor creates a new compactor.
func NewCompactor(dir string, m *manifest.Manifest, logger common.Logger) *Compactor {
	if logger == nil {
		logger = &NullLogger{}
	}

	return &Compactor{
		dir:            dir,
		manifest:       m,
		levelRatio:     10,
		maxL0Files:     8,
		maxSegmentSize: 512 * 1024 * 1024, // 512MB
		logger:         logger,
	}
}

// Start starts the compaction background process.
func (c *Compactor) Start(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	// Derive cancelable context
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	go c.runCompactionLoop(c.ctx)
}

// Stop stops the compaction process.
func (c *Compactor) Stop() {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	c.running = false
	c.mu.Unlock()
}

// runCompactionLoop runs the compaction loop.
func (c *Compactor) runCompactionLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			c.checkAndScheduleCompactions()
			c.runPendingCompaction(ctx)
		}
	}
}

// checkAndScheduleCompactions checks if compaction is needed.
func (c *Compactor) checkAndScheduleCompactions() {
	// Get current segment state
	segments := c.manifest.GetActiveSegments()

	// Check L0 compaction (too many files in L0)
	l0Segments := filterByLevel(segments, 0)
	if len(l0Segments) >= c.maxL0Files {
		c.scheduleL0Compaction(l0Segments)
		return
	}

	// Check leveled compaction (size imbalance between levels)
	for level := 0; level < common.MaxLevel-1; level++ {
		levelSegments := filterByLevel(segments, level)
		nextLevelSegments := filterByLevel(segments, level+1)

		levelSize := calculateTotalSize(levelSegments)
		nextLevelSize := calculateTotalSize(nextLevelSegments)

		// Check if this level is too large compared to next level
		if levelSize > 0 && levelSize > nextLevelSize/int64(c.levelRatio) {
			c.scheduleLeveledCompaction(level, levelSegments)
			return
		}
	}
}

// scheduleL0Compaction schedules L0 compaction.
func (c *Compactor) scheduleL0Compaction(segments []manifest.SegmentInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Select segments to compact (oldest first)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Created < segments[j].Created
	})

	// Take first batch
	batch := segments
	if len(batch) > c.maxL0Files*2 {
		batch = segments[:c.maxL0Files*2]
	}

	segmentIDs := make([]uint64, len(batch))
	for i, seg := range batch {
		segmentIDs[i] = seg.ID
	}

	job := CompactionJob{
		ID:       uint64(time.Now().UnixNano()),
		Level:    0,
		Inputs:   segmentIDs,
		Priority: 100, // High priority for L0
		Reason:   fmt.Sprintf("L0 has %d files (max %d)", len(segments), c.maxL0Files),
	}

	c.pendingCompactions = append(c.pendingCompactions, job)

	c.logger.Info("scheduled L0 compaction", "segments", len(segmentIDs), "reason", job.Reason)
}

// scheduleLeveledCompaction schedules leveled compaction.
func (c *Compactor) scheduleLeveledCompaction(level int, segments []manifest.SegmentInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Select segments with overlapping key ranges
	// For simplicity, just take all segments at this level
	segmentIDs := make([]uint64, len(segments))
	for i, seg := range segments {
		segmentIDs[i] = seg.ID
	}

	job := CompactionJob{
		ID:       uint64(time.Now().UnixNano()),
		Level:    level,
		Inputs:   segmentIDs,
		Priority: 50 - level, // Lower levels have higher priority
		Reason:   fmt.Sprintf("Level %d size imbalance", level),
	}

	c.pendingCompactions = append(c.pendingCompactions, job)

	c.logger.Info("scheduled leveled compaction", "level", level, "segments", len(segmentIDs), "reason", job.Reason)
}

// runPendingCompaction runs the highest priority pending compaction.
func (c *Compactor) runPendingCompaction(ctx context.Context) {
	c.mu.Lock()
	if len(c.pendingCompactions) == 0 {
		c.mu.Unlock()
		return
	}

	// Sort by priority (highest first)
	sort.Slice(c.pendingCompactions, func(i, j int) bool {
		return c.pendingCompactions[i].Priority > c.pendingCompactions[j].Priority
	})

	// Take the highest priority job
	job := c.pendingCompactions[0]
	c.pendingCompactions = c.pendingCompactions[1:]
	c.mu.Unlock()

	// Run compaction
	if err := c.runCompaction(ctx, job); err != nil {
		c.logger.Error("compaction failed", "job", job.ID, "error", err)
	}
}

// runCompaction runs a single compaction job.
func (c *Compactor) runCompaction(ctx context.Context, job CompactionJob) error {
	startTime := time.Now()

	c.logger.Info("starting compaction", "job", job.ID, "level", job.Level, "inputs", len(job.Inputs))

	// Load input segments
	segmentsDir := filepath.Join(c.dir, common.DirSegments)
	var readers []*segment.Reader

	for _, segID := range job.Inputs {
		reader, err := segment.NewReader(segID, segmentsDir, c.logger, false)
		if err != nil {
			c.logger.Warn("failed to open segment", "id", segID, "error", err)
			continue
		}
		readers = append(readers, reader)
		defer reader.Close()
	}

	if len(readers) == 0 {
		return fmt.Errorf("no segments to compact")
	}

	// Create merged iterator
	var keys [][]byte
	var values [][]byte

	// Collect all keys (simplified - should use merge iterator)
	for _, reader := range readers {
		iter := reader.Iterator()
		for iter.Next() {
			keys = append(keys, iter.Key())
			values = append(values, iter.Value())
		}
		iter.Close()
	}

	// Sort keys
	indices := make([]int, len(keys))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(keys[indices[i]], keys[indices[j]]) < 0
	})

	// Build new segment(s)
	targetLevel := job.Level + 1
	if job.Level == 0 && len(job.Inputs) < c.maxL0Files*2 {
		targetLevel = 1 // L0 -> L1 compaction
	}

	// Create new segment
	newSegmentID := uint64(time.Now().UnixNano())
	builder := segment.NewBuilder(newSegmentID, targetLevel, segmentsDir, c.logger)

	for _, idx := range indices {
		if err := builder.Add(keys[idx], values[idx], false); err != nil {
			return fmt.Errorf("add to builder: %w", err)
		}

		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Build the segment
	_, err := builder.Build()
	if err != nil {
		return fmt.Errorf("build segment: %w", err)
	}

	// Update manifest
	info := manifest.SegmentInfo{
		ID:        newSegmentID,
		Level:     targetLevel,
		MinKeyHex: fmt.Sprintf("%x", keys[indices[0]]),
		MaxKeyHex: fmt.Sprintf("%x", keys[indices[len(indices)-1]]),
		Size:      0,
		NumKeys:   uint64(len(keys)),
	}

	// Try to compute actual size from on-disk files
	if md, err := segment.LoadFromFile(filepath.Join(segmentsDir, fmt.Sprintf("%016d", newSegmentID), "segment.json")); err == nil {
		// Sum known file sizes
		var total int64
		files := []string{
			md.Files.Louds,
			md.Files.Edges,
			md.Files.Accept,
			md.Files.TMap,
			md.Files.Tails,
		}
		for _, f := range files {
			if f == "" {
				continue
			}
			p := filepath.Join(segmentsDir, fmt.Sprintf("%016d", newSegmentID), f)
			if st, err := os.Stat(p); err == nil {
				total += st.Size()
			}
		}
		// include filters and keys if present
		for _, f := range []string{"filters/prefix.bf", "filters/tri.bits", "keys.dat", "tombstones.dat"} {
			p := filepath.Join(segmentsDir, fmt.Sprintf("%016d", newSegmentID), f)
			if st, err := os.Stat(p); err == nil {
				total += st.Size()
			}
		}
		info.Size = total
	}

	if err := c.manifest.AddSegment(info); err != nil {
		return fmt.Errorf("add segment to manifest: %w", err)
	}

	// Mark old segments for deletion
	if err := c.manifest.RemoveSegments(job.Inputs); err != nil {
		return fmt.Errorf("remove old segments from manifest: %w", err)
	}

	// Record compaction
	compactionInfo := manifest.CompactionInfo{
		ID:        job.ID,
		Level:     job.Level,
		Inputs:    job.Inputs,
		Outputs:   []uint64{newSegmentID},
		StartTime: startTime.Unix(),
		EndTime:   time.Now().Unix(),
		Reason:    job.Reason,
	}

	if err := c.manifest.RecordCompaction(compactionInfo); err != nil {
		c.logger.Warn("failed to record compaction", "error", err)
	}

	duration := time.Since(startTime)
	c.logger.Info("compaction completed",
		"job", job.ID,
		"duration", duration,
		"inputs", len(job.Inputs),
		"output", newSegmentID,
		"level", fmt.Sprintf("L%d->L%d", job.Level, targetLevel))

	return nil
}

// TriggerCompaction manually triggers compaction.
func (c *Compactor) TriggerCompaction() {
	c.checkAndScheduleCompactions()
}

// Helper functions

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

// NullLogger is a logger that discards all messages.
type NullLogger struct{}

func (n *NullLogger) Debug(msg string, fields ...interface{}) {}
func (n *NullLogger) Info(msg string, fields ...interface{})  {}
func (n *NullLogger) Warn(msg string, fields ...interface{})  {}
func (n *NullLogger) Error(msg string, fields ...interface{}) {}
