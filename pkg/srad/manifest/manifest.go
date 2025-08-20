package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
)

// Manifest manages the list of segments and provides atomic updates.
type Manifest struct {
	mu       sync.RWMutex
	dir      string
	current  *Version
	versions []*Version // History of versions

	// RCU support
	epoch   uint64
	readers sync.Map // epoch -> reader count

	logger common.Logger
}

// Version represents a manifest version.
type Version struct {
	VersionID   uint64           `json:"versionID"`
	CreatedAt   int64            `json:"createdAt"`
	Segments    []SegmentInfo    `json:"segments"`
	Compactions []CompactionInfo `json:"compactions,omitempty"`
}

// SegmentInfo describes a segment in the manifest.
type SegmentInfo struct {
	ID        uint64 `json:"id"`
	Level     int    `json:"level"`
	MinKeyHex string `json:"minKeyHex,omitempty"`
	MaxKeyHex string `json:"maxKeyHex,omitempty"`
	Size      int64  `json:"size"`
	NumKeys   uint64 `json:"numKeys"`
	Created   int64  `json:"created"`
	Deleted   int64  `json:"deleted,omitempty"` // Timestamp when marked for deletion
}

// CompactionInfo describes a compaction operation.
type CompactionInfo struct {
	ID        uint64   `json:"id"`
	Level     int      `json:"level"`
	Inputs    []uint64 `json:"inputs"`
	Outputs   []uint64 `json:"outputs"`
	StartTime int64    `json:"startTime"`
	EndTime   int64    `json:"endTime"`
	Reason    string   `json:"reason"`
}

// New creates a new manifest.
func New(dir string, logger common.Logger) (*Manifest, error) {
	if logger == nil {
		logger = &NullLogger{}
	}

	manifestDir := filepath.Join(dir, common.DirManifest)
	if err := os.MkdirAll(manifestDir, 0755); err != nil {
		return nil, fmt.Errorf("create manifest directory: %w", err)
	}

	m := &Manifest{
		dir:      manifestDir,
		logger:   logger,
		versions: make([]*Version, 0),
	}

	// Load existing manifest or create new one
	if err := m.load(); err != nil {
		// Create initial version
		m.current = &Version{
			VersionID: 1,
			CreatedAt: time.Now().Unix(),
			Segments:  []SegmentInfo{},
		}

		if err := m.save(); err != nil {
			return nil, fmt.Errorf("save initial manifest: %w", err)
		}
	}

	return m, nil
}

// load loads the manifest from disk.
func (m *Manifest) load() error {
	// Prefer CURRENT pointer
	currentPath := filepath.Join(m.dir, "CURRENT")
	var latestFile string
	if data, err := os.ReadFile(currentPath); err == nil {
		name := string(data)
		name = strings.TrimSpace(name)
		candidate := filepath.Join(m.dir, name)
		if _, err := os.Stat(candidate); err == nil {
			latestFile = candidate
		}
	}
	if latestFile == "" {
		// Fallback to glob
		pattern := filepath.Join(m.dir, "manifest-*.json")
		files, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			return fmt.Errorf("no manifest files found")
		}
		sort.Strings(files)
		latestFile = files[len(files)-1]
	}

	// Read the file
	data, err := os.ReadFile(latestFile)
	if err != nil {
		return fmt.Errorf("read manifest file: %w", err)
	}

	// Unmarshal
	var version Version
	if err := json.Unmarshal(data, &version); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	m.current = &version
	m.versions = append(m.versions, &version)

	m.logger.Info("loaded manifest", "version", version.VersionID, "segments", len(version.Segments))

	return nil
}

// save saves the current manifest version to disk.
func (m *Manifest) save() error {
	if m.current == nil {
		return fmt.Errorf("no current version")
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(m.current, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	// Write to temporary file
	filename := fmt.Sprintf("%016d.json", m.current.VersionID)
	path := filepath.Join(m.dir, filename)

	file, err := utils.NewAtomicFile(path)
	if err != nil {
		return fmt.Errorf("create atomic file: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	if err := file.Commit(); err != nil {
		return fmt.Errorf("commit manifest: %w", err)
	}

	m.logger.Debug("saved manifest", "version", m.current.VersionID, "path", path)

	// Update CURRENT pointer atomically (best effort)
	currentPath := filepath.Join(m.dir, "CURRENT")
	tmp := currentPath + ".tmp"
	content := []byte(filename + "\n")
	if err := os.WriteFile(tmp, content, 0644); err == nil {
		_ = os.Rename(tmp, currentPath)
		_ = utils.SyncDir(m.dir)
	}

	return nil
}

// AddSegment adds a new segment to the manifest.
func (m *Manifest) AddSegment(info SegmentInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new version
	newVersion := &Version{
		VersionID:   m.current.VersionID + 1,
		CreatedAt:   time.Now().Unix(),
		Segments:    make([]SegmentInfo, len(m.current.Segments)),
		Compactions: m.current.Compactions,
	}

	// Copy existing segments
	copy(newVersion.Segments, m.current.Segments)

	// Add new segment
	info.Created = time.Now().Unix()
	newVersion.Segments = append(newVersion.Segments, info)

	// Save new version
	oldVersion := m.current
	m.current = newVersion

	if err := m.save(); err != nil {
		// Rollback on error
		m.current = oldVersion
		return err
	}

	m.versions = append(m.versions, newVersion)

	// Start RCU grace period for old version
	m.startGracePeriod(oldVersion)

	return nil
}

// RemoveSegments marks segments for deletion.
func (m *Manifest) RemoveSegments(segmentIDs []uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new version
	newVersion := &Version{
		VersionID:   m.current.VersionID + 1,
		CreatedAt:   time.Now().Unix(),
		Segments:    make([]SegmentInfo, 0, len(m.current.Segments)),
		Compactions: m.current.Compactions,
	}

	// Copy segments, marking deleted ones
	deleteMap := make(map[uint64]bool)
	for _, id := range segmentIDs {
		deleteMap[id] = true
	}

	now := time.Now().Unix()
	for _, seg := range m.current.Segments {
		if deleteMap[seg.ID] {
			seg.Deleted = now
		}
		newVersion.Segments = append(newVersion.Segments, seg)
	}

	// Save new version
	oldVersion := m.current
	m.current = newVersion

	if err := m.save(); err != nil {
		// Rollback on error
		m.current = oldVersion
		return err
	}

	m.versions = append(m.versions, newVersion)

	// Start RCU grace period for old version
	m.startGracePeriod(oldVersion)

	return nil
}

// GetActiveSegments returns the list of active (non-deleted) segments.
func (m *Manifest) GetActiveSegments() []SegmentInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Register reader for current epoch
	epoch := atomic.LoadUint64(&m.epoch)
	m.registerReader(epoch)
	defer m.unregisterReader(epoch)

	active := make([]SegmentInfo, 0, len(m.current.Segments))
	for _, seg := range m.current.Segments {
		if seg.Deleted == 0 {
			active = append(active, seg)
		}
	}

	return active
}

// GetSegmentsByLevel returns segments at a specific level.
func (m *Manifest) GetSegmentsByLevel(level int) []SegmentInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Register reader for current epoch
	epoch := atomic.LoadUint64(&m.epoch)
	m.registerReader(epoch)
	defer m.unregisterReader(epoch)

	segments := make([]SegmentInfo, 0)
	for _, seg := range m.current.Segments {
		if seg.Level == level && seg.Deleted == 0 {
			segments = append(segments, seg)
		}
	}

	return segments
}

// RecordCompaction records a compaction operation.
func (m *Manifest) RecordCompaction(info CompactionInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new version
	newVersion := &Version{
		VersionID:   m.current.VersionID + 1,
		CreatedAt:   time.Now().Unix(),
		Segments:    m.current.Segments,
		Compactions: append(m.current.Compactions, info),
	}

	// Save new version
	oldVersion := m.current
	m.current = newVersion

	if err := m.save(); err != nil {
		// Rollback on error
		m.current = oldVersion
		return err
	}

	m.versions = append(m.versions, newVersion)

	// Start RCU grace period for old version
	m.startGracePeriod(oldVersion)

	return nil
}

// RCU support

// startGracePeriod starts an RCU grace period for an old version.
func (m *Manifest) startGracePeriod(oldVersion *Version) {
	// Increment epoch
	newEpoch := atomic.AddUint64(&m.epoch, 1)

	// Schedule cleanup after grace period
	go func() {
		time.Sleep(time.Duration(common.RCUGracePeriod) * time.Second)
		m.cleanupOldVersion(oldVersion, newEpoch-1)
	}()
}

// registerReader registers a reader for an epoch.
func (m *Manifest) registerReader(epoch uint64) {
	val, _ := m.readers.LoadOrStore(epoch, &atomic.Int32{})
	counter := val.(*atomic.Int32)
	counter.Add(1)
}

// unregisterReader unregisters a reader for an epoch.
func (m *Manifest) unregisterReader(epoch uint64) {
	val, ok := m.readers.Load(epoch)
	if !ok {
		return
	}

	counter := val.(*atomic.Int32)
	if counter.Add(-1) <= 0 {
		m.readers.Delete(epoch)
	}
}

// cleanupOldVersion cleans up an old version after grace period.
func (m *Manifest) cleanupOldVersion(version *Version, epoch uint64) {
	// Check if there are still readers
	if val, ok := m.readers.Load(epoch); ok {
		counter := val.(*atomic.Int32)
		if counter.Load() > 0 {
			// Still has readers, retry later
			go func() {
				time.Sleep(5 * time.Second)
				m.cleanupOldVersion(version, epoch)
			}()
			return
		}
	}

	// Safe to cleanup
	m.readers.Delete(epoch)

	// Remove old manifest file if it's not the current one
	if version.VersionID < m.current.VersionID-10 {
		filename := fmt.Sprintf("manifest-%06d.json", version.VersionID)
		path := filepath.Join(m.dir, filename)
		os.Remove(path)

		m.logger.Debug("cleaned up old manifest", "version", version.VersionID)
	}
}

// GetCurrentVersion returns the current manifest version.
func (m *Manifest) GetCurrentVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.current.VersionID
}

// GetStats returns manifest statistics.
func (m *Manifest) GetStats() ManifestStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := ManifestStats{
		Version:        m.current.VersionID,
		TotalSegments:  len(m.current.Segments),
		ActiveSegments: 0,
		TotalSize:      0,
		TotalKeys:      0,
	}

	levelCounts := make(map[int]int)
	for _, seg := range m.current.Segments {
		if seg.Deleted == 0 {
			stats.ActiveSegments++
			stats.TotalSize += seg.Size
			stats.TotalKeys += seg.NumKeys
			levelCounts[seg.Level]++
		}
	}

	stats.LevelCounts = levelCounts

	return stats
}

// ManifestStats contains manifest statistics.
type ManifestStats struct {
	Version        uint64
	TotalSegments  int
	ActiveSegments int
	TotalSize      int64
	TotalKeys      uint64
	LevelCounts    map[int]int
}

// NullLogger is a logger that discards all messages.
type NullLogger struct{}

func (n *NullLogger) Debug(msg string, fields ...interface{}) {}
func (n *NullLogger) Info(msg string, fields ...interface{})  {}
func (n *NullLogger) Warn(msg string, fields ...interface{})  {}
func (n *NullLogger) Error(msg string, fields ...interface{}) {}
