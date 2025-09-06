package compaction

import (
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
	blake3 "lukechampine.com/blake3"
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
func NewCompactor(dir string, m *manifest.Manifest, logger common.Logger, alloc func() uint64) *Compactor {
	if logger == nil {
		logger = &NullLogger{}
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

	// Prefer smaller segments first to keep job size bounded
	sort.Slice(segments, func(i, j int) bool { return segments[i].Size < segments[j].Size })

	// Select segments with overlapping key ranges (simplified: take a limited batch)
	maxBatch := c.maxL0Files * 3
	if maxBatch < 4 {
		maxBatch = 4
	}
	if len(segments) > maxBatch {
		segments = segments[:maxBatch]
	}

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
	if err := c.runCompactionLegacy(ctx, job); err != nil {
		c.logger.Error("compaction failed", "job", job.ID, "error", err)
	}
}

// runCompactionLegacy runs a single compaction job using a legacy approach.
func (c *Compactor) runCompactionLegacy(ctx context.Context, job CompactionJob) error {
	startTime := time.Now()

	c.logger.Info("starting compaction", "job", job.ID, "level", job.Level, "inputs", len(job.Inputs))

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

	// newest first, so newest wins
	sort.Slice(readers, func(i, j int) bool {
		mi := readers[i].GetMetadata()
		mj := readers[j].GetMetadata()
		if mi == nil || mj == nil {
			return false
		}
		return mi.CreatedAtUnix > mj.CreatedAtUnix
	})

	targetLevel := job.Level + 1
	if job.Level == 0 && len(job.Inputs) < c.maxL0Files*2 {
		targetLevel = 1
	}

	seen := make(map[[32]byte]struct{}, 1024)
	type chunkState struct {
		builder       *segment.Builder
		id            uint64
		approxBytes   int64
		acceptedCount int64
	}
	var cur chunkState
	outputs := make([]uint64, 0, 8)

	ensureBuilder := func() {
		if cur.builder != nil {
			return
		}
		cur.id = c.alloc()
		cur.builder = segment.NewBuilder(cur.id, targetLevel, segmentsDir, c.logger)
		cur.approxBytes = 0
		cur.acceptedCount = 0
	}
	finalizeChunk := func() error {
		if cur.builder == nil {
			return nil
		}
		md, err := cur.builder.Build()
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
		if err := c.manifest.AddSegment(info); err != nil {
			return fmt.Errorf("add segment to manifest: %w", err)
		}
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

	for _, r := range readers {
		_, _ = r.IterateTombstones(func(tk []byte) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			h := blake3.Sum256(tk)
			if _, ok := seen[h]; ok {
				return true
			}
			ensureBuilder()
			cur.builder.AddTombstone(tk)
			seen[h] = struct{}{}
			cur.approxBytes += int64(len(tk))
			if err := maybeFlush(); err != nil {
				return false
			}
			return true
		})
		_, _ = r.IterateKeys(func(k []byte) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			h := blake3.Sum256(k)
			if _, ok := seen[h]; ok {
				return true
			}
			ensureBuilder()
			if err := cur.builder.Add(k, k, false); err != nil {
				c.logger.Warn("builder add failed during streaming compaction", "error", err)
				seen[h] = struct{}{}
				return true
			}
			seen[h] = struct{}{}
			cur.approxBytes += int64(len(k))
			cur.acceptedCount++
			if err := maybeFlush(); err != nil {
				return false
			}
			return true
		})
	}
	if err := finalizeChunk(); err != nil {
		return err
	}
	if len(outputs) == 0 {
		c.logger.Info("compaction produced no output segments", "job", job.ID)
		return nil
	}
	if err := c.manifest.RemoveSegments(job.Inputs); err != nil {
		return fmt.Errorf("remove old segments from manifest: %w", err)
	}
	compactionInfo := manifest.CompactionInfo{
		ID:        job.ID,
		Level:     job.Level,
		Inputs:    job.Inputs,
		Outputs:   outputs,
		StartTime: startTime.Unix(),
		EndTime:   time.Now().Unix(),
		Reason:    job.Reason,
	}
	if err := c.manifest.RecordCompaction(compactionInfo); err != nil {
		c.logger.Warn("failed to record compaction", "error", err)
	}
	c.logger.Info("compaction completed", "job", job.ID, "inputs", len(job.Inputs), "outputs", len(outputs), "level", fmt.Sprintf("L%d->L%d", job.Level, targetLevel))
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
