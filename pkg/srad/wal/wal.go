package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	enc "github.com/CVDpl/go-live-srad/internal/encoding"
	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
)

// WAL represents a Write-Ahead Log for durability.
type WAL struct {
	mu          sync.Mutex
	dir         string
	currentFile *os.File
	currentPath string
	writer      *bufio.Writer
	currentSeq  uint64
	currentSize int64
	rotateSize  int64
	maxFileSize int64
	logger      common.Logger
	closed      bool
	bufferSize  int

	// Durability policy
	syncOnEveryWrite    bool
	flushOnEveryWrite   bool
	flushEveryBytes     int
	bytesSinceLastFlush int

	// Memory pooling for batch operations
	batchBufferPool sync.Pool // Pool of []byte buffers for encoding
	batchSlicePool  sync.Pool // Pool of [][]byte slices for batch data
}

// Config controls WAL sizing and buffering.
type Config struct {
	RotateSize  int64
	MaxFileSize int64
	BufferSize  int
	// Durability policy
	SyncOnEveryWrite  bool
	FlushOnEveryWrite bool
	FlushEveryBytes   int
}

// WALHeader represents the fixed-size WAL file header (14 bytes).
type WALHeader struct {
	Magic     uint32 // 4 bytes
	Version   uint16 // 2 bytes
	CreatedAt int64  // 8 bytes (unix timestamp)
}

// WALRecord represents a single WAL record.
type WALRecord struct {
	Op     uint8
	Key    []byte
	SeqNum uint64
	TTL    time.Duration // 0 means no TTL
	CRC32C uint32
}

// WALEntry represents a single operation to be written to the WAL.
type WALEntry struct {
	Op  uint8
	Key []byte
	TTL time.Duration
}

// New creates a new WAL instance.
func New(dir string, logger common.Logger) (*WAL, error) {
	return NewWithConfig(dir, logger, Config{
		RotateSize:  common.WALRotateSize,
		MaxFileSize: common.WALMaxFileSize,
		BufferSize:  int(common.WALBufferSize),
	})
}

// NewWithConfig creates a new WAL instance with custom configuration.
func NewWithConfig(dir string, logger common.Logger, cfg Config) (*WAL, error) {
	if err := utils.CreateDirIfNotExists(dir); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}

	if logger == nil {
		logger = common.NewNullLogger()
	}

	if cfg.RotateSize <= 0 {
		cfg.RotateSize = common.WALRotateSize
	}
	if cfg.MaxFileSize <= 0 {
		cfg.MaxFileSize = common.WALMaxFileSize
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = int(common.WALBufferSize)
	}

	w := &WAL{
		dir:               dir,
		rotateSize:        cfg.RotateSize,
		maxFileSize:       cfg.MaxFileSize,
		bufferSize:        cfg.BufferSize,
		logger:            logger,
		syncOnEveryWrite:  cfg.SyncOnEveryWrite,
		flushOnEveryWrite: cfg.FlushOnEveryWrite,
		flushEveryBytes:   cfg.FlushEveryBytes,
	}

	// Initialize memory pools for batch operations
	w.batchBufferPool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate buffer for typical record size (~key + overhead)
			buf := make([]byte, 0, 256)
			return &buf
		},
	}
	w.batchSlicePool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate slice for typical batch size
			s := make([][]byte, 0, 64)
			return &s
		},
	}

	// Find the latest WAL file or create a new one
	if err := w.openOrCreateFile(); err != nil {
		return nil, err
	}

	return w, nil
}

// openOrCreateFile opens the latest WAL file or creates a new one.
func (w *WAL) openOrCreateFile() error {
	files, err := w.listWALFiles()
	if err != nil {
		return fmt.Errorf("list WAL files: %w", err)
	}

	if len(files) == 0 {
		// Create first WAL file
		return w.createNewFile(1)
	}

	// Open the latest file
	latestFile := files[len(files)-1]
	return w.openFile(latestFile)
}

// listWALFiles returns sorted list of WAL files.
func (w *WAL) listWALFiles() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasSuffix(name, ".log") && !strings.Contains(name, ".corrupt") {
			files = append(files, filepath.Join(w.dir, name))
		}
	}

	// Sort by sequence number
	sort.Slice(files, func(i, j int) bool {
		seqI := w.extractSequence(files[i])
		seqJ := w.extractSequence(files[j])
		return seqI < seqJ
	})

	return files, nil
}

// extractSequence extracts sequence number from WAL filename.
func (w *WAL) extractSequence(path string) uint64 {
	base := filepath.Base(path)
	parts := strings.Split(base, ".")
	if len(parts) < 2 {
		return 0
	}

	seq, _ := strconv.ParseUint(parts[0], 10, 64)
	return seq
}

// createNewFile creates a new WAL file.
func (w *WAL) createNewFile(seq uint64) error {
	filename := fmt.Sprintf("%016d.log", seq)
	path := filepath.Join(w.dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("create WAL file: %w", err)
	}

	// Write header
	header := WALHeader{
		Magic:     common.MagicWAL,
		Version:   common.VersionWAL,
		CreatedAt: time.Now().Unix(),
	}

	if err := w.writeHeader(file, header); err != nil {
		file.Close()
		os.Remove(path)
		return fmt.Errorf("write WAL header: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync WAL file: %w", err)
	}

	w.currentFile = file
	w.currentPath = path
	w.currentSeq = seq
	w.currentSize = 14 // header size
	w.writer = bufio.NewWriterSize(file, w.bufferSize)

	w.logger.Info("created new WAL file", "path", path, "seq", seq)

	return nil
}

// openFile opens an existing WAL file.
func (w *WAL) openFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open WAL file: %w", err)
	}

	// Verify header
	header, err := w.readHeader(file)
	if err != nil {
		file.Close()
		// Quarantine corrupted file
		if err := utils.QuarantineFile(path); err != nil {
			w.logger.Error("failed to quarantine corrupted WAL file", "path", path, "error", err)
		}
		return fmt.Errorf("read WAL header: %w", err)
	}

	if header.Magic != common.MagicWAL {
		file.Close()
		if err := utils.QuarantineFile(path); err != nil {
			w.logger.Error("failed to quarantine WAL file with invalid magic", "path", path, "error", err)
		}
		return fmt.Errorf("invalid WAL magic: %x", header.Magic)
	}

	if header.Version != common.VersionWAL {
		file.Close()
		return fmt.Errorf("unsupported WAL version: %x", header.Version)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("stat WAL file: %w", err)
	}

	w.currentFile = file
	w.currentPath = path
	w.currentSeq = w.extractSequence(path)
	w.currentSize = stat.Size()
	w.writer = bufio.NewWriterSize(file, w.bufferSize)

	w.logger.Info("opened WAL file", "path", path, "size", w.currentSize)

	return nil
}

// WriteBatch writes a batch of operations to the WAL.
// This is more efficient than multiple individual writes as it minimizes lock contention and disk I/O.
func (w *WAL) WriteBatch(entries []*WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return common.ErrClosed
	}

	// Get pooled slice for batch data
	dataToWritePtr := w.batchSlicePool.Get().(*[][]byte)
	dataToWrite := (*dataToWritePtr)[:0] // Reset but keep capacity
	defer func() {
		// Return slice to pool
		*dataToWritePtr = dataToWrite
		w.batchSlicePool.Put(dataToWritePtr)
	}()

	var totalSize int64
	var encodedBuffers []*[]byte // Track buffers to return to pool

	for _, entry := range entries {
		if len(entry.Key) > common.MaxKeySize {
			// Return all buffers before erroring
			for _, bufPtr := range encodedBuffers {
				w.batchBufferPool.Put(bufPtr)
			}
			return common.ErrKeyTooLarge // Fail the whole batch on invalid entry
		}

		record := WALRecord{
			Op:  entry.Op,
			Key: entry.Key,
			TTL: entry.TTL,
		}

		// Get pooled buffer
		bufPtr := w.batchBufferPool.Get().(*[]byte)
		encodedBuffers = append(encodedBuffers, bufPtr)

		data, err := w.encodeRecordTo(record, *bufPtr)
		if err != nil {
			// Return all buffers before erroring
			for _, bp := range encodedBuffers {
				w.batchBufferPool.Put(bp)
			}
			return fmt.Errorf("encode WAL record: %w", err)
		}

		// Make a copy since we'll return the buffer to pool
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		dataToWrite = append(dataToWrite, dataCopy)
		totalSize += int64(len(dataCopy))
	}

	// Return all encoding buffers to pool now that we're done with them
	for _, bufPtr := range encodedBuffers {
		w.batchBufferPool.Put(bufPtr)
	}

	if w.currentSize+totalSize > w.rotateSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("rotate WAL before batch write: %w", err)
		}
	}

	for _, data := range dataToWrite {
		n, err := w.writer.Write(data)
		if err != nil {
			return fmt.Errorf("write WAL record from batch: %w", err)
		}
		w.bytesSinceLastFlush += n
	}

	w.currentSize += totalSize

	if w.flushOnEveryWrite || w.syncOnEveryWrite || (w.flushEveryBytes > 0 && w.bytesSinceLastFlush >= w.flushEveryBytes) {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush WAL buffer after batch: %w", err)
		}
		w.bytesSinceLastFlush = 0
	}

	if w.syncOnEveryWrite {
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("sync WAL file after batch: %w", err)
		}
	}

	return nil
}

// Write writes an operation to the WAL.
func (w *WAL) Write(op uint8, key []byte) error {
	return w.WriteWithTTL(op, key, 0)
}

// WriteWithTTL writes an operation to the WAL with TTL.
func (w *WAL) WriteWithTTL(op uint8, key []byte, ttl time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return common.ErrClosed
	}

	// Check key size
	if len(key) > common.MaxKeySize {
		return common.ErrKeyTooLarge
	}

	// Encode record
	record := WALRecord{
		Op:  op,
		Key: key,
		TTL: ttl,
	}

	data, err := w.encodeRecord(record)
	if err != nil {
		return fmt.Errorf("encode WAL record: %w", err)
	}

	// Check if rotation is needed
	if w.currentSize+int64(len(data)) > w.rotateSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("rotate WAL: %w", err)
		}
	}

	// Write record
	n, err := w.writer.Write(data)
	if err != nil {
		return fmt.Errorf("write WAL record: %w", err)
	}

	w.currentSize += int64(len(data))
	w.bytesSinceLastFlush += n

	// Optional flush policy
	if w.flushOnEveryWrite || (w.flushEveryBytes > 0 && w.bytesSinceLastFlush >= w.flushEveryBytes) {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush WAL buffer: %w", err)
		}
		w.bytesSinceLastFlush = 0
	}
	// Optional sync policy (implies flush)
	if w.syncOnEveryWrite {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush WAL buffer before sync: %w", err)
		}
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("sync WAL file: %w", err)
		}
	}

	return nil
}

// Flush flushes buffered data to disk.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return common.ErrClosed
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush WAL buffer: %w", err)
	}

	return nil
}

// Sync syncs the WAL file to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return common.ErrClosed
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush WAL buffer: %w", err)
	}

	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("sync WAL file: %w", err)
	}

	return nil
}

// rotate rotates to a new WAL file.
func (w *WAL) rotate() error {
	// Flush and sync current file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush before rotate: %w", err)
	}

	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("sync before rotate: %w", err)
	}

	w.currentFile.Close()

	// Create new file
	nextSeq := w.currentSeq + 1
	if err := w.createNewFile(nextSeq); err != nil {
		// Try to reopen the old file
		w.openFile(w.currentPath)
		return fmt.Errorf("create new WAL file: %w", err)
	}

	w.logger.Info("rotated WAL file", "old_seq", w.currentSeq-1, "new_seq", w.currentSeq)

	// reset flush counter
	w.bytesSinceLastFlush = 0

	return nil
}

// Rotate exposes WAL rotation to callers.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return common.ErrClosed
	}
	return w.rotate()
}

// CurrentSeq returns the current WAL sequence number.
func (w *WAL) CurrentSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentSeq
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	if w.writer != nil {
		w.writer.Flush()
	}

	if w.currentFile != nil {
		w.currentFile.Sync()
		w.currentFile.Close()
	}

	return nil
}

// writeHeader writes a WAL header to a file.
func (w *WAL) writeHeader(file *os.File, header WALHeader) error {
	buf := make([]byte, 14)
	binary.LittleEndian.PutUint32(buf[0:4], header.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], header.Version)
	binary.LittleEndian.PutUint64(buf[6:14], uint64(header.CreatedAt))

	_, err := file.Write(buf)
	return err
}

// readHeader reads a WAL header from a file.
func (w *WAL) readHeader(file *os.File) (WALHeader, error) {
	buf := make([]byte, 14)
	if _, err := io.ReadFull(file, buf); err != nil {
		return WALHeader{}, err
	}

	return WALHeader{
		Magic:     binary.LittleEndian.Uint32(buf[0:4]),
		Version:   binary.LittleEndian.Uint16(buf[4:6]),
		CreatedAt: int64(binary.LittleEndian.Uint64(buf[6:14])),
	}, nil
}

// encodeRecord encodes a WAL record.
func (w *WAL) encodeRecord(record WALRecord) ([]byte, error) {
	var buf bytes.Buffer

	// Write op as varint
	if err := utils.WriteUvarint(&buf, uint64(record.Op)); err != nil {
		return nil, err
	}

	// Write key length as varint
	if err := utils.WriteUvarint(&buf, uint64(len(record.Key))); err != nil {
		return nil, err
	}

	// Write key
	if _, err := buf.Write(record.Key); err != nil {
		return nil, err
	}

	// Write absolute expiry as int64 unix nanos (0 means none)
	var abs int64
	if record.TTL > 0 {
		abs = time.Now().Add(record.TTL).UnixNano()
	} else {
		abs = 0
	}
	absBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(absBuf, uint64(abs))
	buf.Write(absBuf)

	// Compute CRC32C
	data := buf.Bytes()
	crc := utils.ComputeCRC32C(data)

	// Append CRC32C
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	buf.Write(crcBuf)

	return buf.Bytes(), nil
}

// encodeRecordTo encodes a WAL record into the provided byte buffer.
// Returns the encoded data as a slice of the buffer.
// This is a zero-allocation version for use with pooled buffers.
func (w *WAL) encodeRecordTo(record WALRecord, buf []byte) ([]byte, error) {
	// Reset buffer but keep capacity
	buf = buf[:0]

	// Write op as varint
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(record.Op))
	buf = append(buf, tmp[:n]...)

	// Write key length as varint
	n = binary.PutUvarint(tmp[:], uint64(len(record.Key)))
	buf = append(buf, tmp[:n]...)

	// Write key
	buf = append(buf, record.Key...)

	// Write absolute expiry as int64 unix nanos (0 means none)
	var abs int64
	if record.TTL > 0 {
		abs = time.Now().Add(record.TTL).UnixNano()
	} else {
		abs = 0
	}
	binary.LittleEndian.PutUint64(tmp[:8], uint64(abs))
	buf = append(buf, tmp[:8]...)

	// Compute CRC32C over the data so far
	crc := utils.ComputeCRC32C(buf)

	// Append CRC32C
	binary.LittleEndian.PutUint32(tmp[:4], crc)
	buf = append(buf, tmp[:4]...)

	return buf, nil
}

// Replay replays all WAL files and calls the callback for each record.
func (w *WAL) Replay(callback func(op uint8, key []byte, expiresAt time.Time) error) error {
	files, err := w.listWALFiles()
	if err != nil {
		return fmt.Errorf("list WAL files: %w", err)
	}

	for _, path := range files {
		if err := w.replayFile(path, callback); err != nil {
			w.logger.Warn("failed to replay WAL file", "path", path, "error", err)
			// Continue with other files
		}
	}

	return nil
}

// replayFile replays a single WAL file.
func (w *WAL) replayFile(path string, callback func(op uint8, key []byte, expiresAt time.Time) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open WAL file: %w", err)
	}
	defer file.Close()

	// Read and verify header
	header, err := w.readHeader(file)
	if err != nil {
		// Quarantine corrupted file
		if err := utils.QuarantineFile(path); err != nil {
			w.logger.Error("failed to quarantine corrupted WAL file", "path", path, "error", err)
		}
		return fmt.Errorf("read WAL header: %w", err)
	}

	if header.Magic != common.MagicWAL {
		if err := utils.QuarantineFile(path); err != nil {
			w.logger.Error("failed to quarantine WAL file with invalid magic", "path", path, "error", err)
		}
		return fmt.Errorf("invalid WAL magic: %x", header.Magic)
	}

	if header.Version != common.VersionWAL {
		return fmt.Errorf("unsupported WAL version: %x", header.Version)
	}

	// Read records
	reader := bufio.NewReaderSize(file, 1<<20)
	offset := int64(14) // header size
	lastValidOffset := offset
	recordCount := 0

	for {
		record, size, err := w.readRecord(reader)
		if err == io.EOF || errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Truncate only on non-EOF corruption
			w.logger.Warn("truncating WAL file due to corrupted record",
				"path", path, "offset", offset, "error", err)

			file.Close()
			if err := utils.TruncateFile(path, lastValidOffset); err != nil {
				w.logger.Error("failed to truncate WAL file", "path", path, "error", err)
			}
			break
		}

		// Skip tombstones if needed
		if len(record.Key) > common.MaxKeySize {
			w.logger.Warn("skipping record with oversized key",
				"path", path, "offset", offset, "key_size", len(record.Key))
			offset += int64(size)
			continue
		}

		// Convert to absolute expiry
		var expiresAt time.Time
		if record.TTL > 0 {
			// In new format readRecord stores absolute unix nanos in TTL field as time.Duration
			abs := int64(record.TTL)
			if abs > 0 {
				expiresAt = time.Unix(0, abs)
			}
		}

		// Call callback
		if err := callback(record.Op, record.Key, expiresAt); err != nil {
			return fmt.Errorf("replay callback error: %w", err)
		}

		lastValidOffset = offset + int64(size)
		offset = lastValidOffset
		recordCount++
	}

	w.logger.Info("replayed WAL file", "path", path, "records", recordCount)

	return nil
}

// readRecord reads a single WAL record.
func (w *WAL) readRecord(reader *bufio.Reader) (WALRecord, int, error) {
	// Read op as varint
	op, err := utils.ReadUvarint(reader)
	if err == io.EOF {
		return WALRecord{}, 0, io.EOF
	}
	if err != nil {
		return WALRecord{}, 0, fmt.Errorf("read op: %w", err)
	}

	// Read key length as varint
	keyLen, err := utils.ReadUvarint(reader)
	if err != nil {
		return WALRecord{}, 0, fmt.Errorf("read key length: %w", err)
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return WALRecord{}, 0, fmt.Errorf("read key: %w", err)
	}

	// Read absolute expiry as int64 (unix nanos)
	expBuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, expBuf); err != nil {
		return WALRecord{}, 0, fmt.Errorf("read expiry: %w", err)
	}
	abs := int64(binary.LittleEndian.Uint64(expBuf))

	// Read CRC32C
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, crcBuf); err != nil {
		return WALRecord{}, 0, fmt.Errorf("read CRC: %w", err)
	}
	expectedCRC := binary.LittleEndian.Uint32(crcBuf)

	// Verify CRC
	var buf bytes.Buffer
	utils.WriteUvarint(&buf, uint64(op))
	utils.WriteUvarint(&buf, uint64(keyLen))
	buf.Write(key)
	buf.Write(expBuf)

	actualCRC := utils.ComputeCRC32C(buf.Bytes())
	if actualCRC != expectedCRC {
		return WALRecord{}, 0, common.ErrCRCMismatch
	}

	// Compute exact size consumed
	size := 0
	size += enc.SizeUvarint(uint64(op))
	size += enc.SizeUvarint(uint64(keyLen))
	size += int(keyLen)
	size += 8 // expiry
	size += 4 // CRC32C

	var ttlAsDuration time.Duration
	if abs > 0 {
		// temporarily store absolute nanos in TTL field for compatibility inside this file
		ttlAsDuration = time.Duration(abs)
	}
	return WALRecord{
		Op:     uint8(op),
		Key:    key,
		TTL:    ttlAsDuration,
		CRC32C: expectedCRC,
	}, size, nil
}

// DeleteOldFiles deletes WAL files older than the specified sequence.
func (w *WAL) DeleteOldFiles(beforeSeq uint64) error {
	files, err := w.listWALFiles()
	if err != nil {
		return fmt.Errorf("list WAL files: %w", err)
	}

	if len(files) == 0 {
		return nil // No files to delete
	}

	var deleteErrors []error
	deletedCount := 0

	for _, path := range files {
		seq := w.extractSequence(path)
		if seq < beforeSeq && seq > 0 { // Safety: never delete seq 0, and ensure valid seq
			if err := os.Remove(path); err != nil {
				deleteErrors = append(deleteErrors, fmt.Errorf("delete %s (seq %d): %w", path, seq, err))
				w.logger.Warn("failed to delete old WAL file", "path", path, "seq", seq, "error", err)
			} else {
				deletedCount++
				w.logger.Info("deleted old WAL file", "path", path, "seq", seq)
			}
		}
	}

	if len(deleteErrors) > 0 {
		// Log summary of errors but don't fail if some files were deleted successfully
		w.logger.Warn("some WAL file deletions failed", "failed", len(deleteErrors), "succeeded", deletedCount)
		if deletedCount == 0 {
			// If no files were deleted successfully, return error
			return fmt.Errorf("failed to delete any old WAL files: %v", deleteErrors)
		}
	}

	if deletedCount > 0 {
		w.logger.Info("WAL cleanup completed", "deleted", deletedCount, "failed", len(deleteErrors))
	}

	return nil
}

// ReplayDir replays WAL files from a directory in read-only fashion.
// It does not create, rotate, modify, or quarantine files. Best-effort: on corruption, it logs and continues.
func ReplayDir(dir string, logger common.Logger, callback func(op uint8, key []byte, expiresAt time.Time) error) error {
	if logger == nil {
		logger = common.NewNullLogger()
	}
	files, err := listWALFilesRO(dir)
	if err != nil {
		return fmt.Errorf("list WAL files (RO): %w", err)
	}
	for _, path := range files {
		if err := replayFileRO(path, logger, callback); err != nil {
			logger.Warn("failed to replay WAL file (RO)", "path", path, "error", err)
			// continue with other files
		}
	}
	return nil
}

// listWALFilesRO returns sorted list of WAL files in a directory without using WAL instance.
func listWALFilesRO(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".log") && !strings.Contains(name, ".corrupt") {
			files = append(files, filepath.Join(dir, name))
		}
	}
	// Sort by sequence number
	sort.Slice(files, func(i, j int) bool {
		return extractSequenceRO(files[i]) < extractSequenceRO(files[j])
	})
	return files, nil
}

func extractSequenceRO(path string) uint64 {
	base := filepath.Base(path)
	parts := strings.Split(base, ".")
	if len(parts) < 2 {
		return 0
	}
	seq, _ := strconv.ParseUint(parts[0], 10, 64)
	return seq
}

func replayFileRO(path string, logger common.Logger, callback func(op uint8, key []byte, expiresAt time.Time) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open WAL file (RO): %w", err)
	}
	defer f.Close()

	// Read header
	hdr, err := readHeaderRO(f)
	if err != nil {
		return fmt.Errorf("read WAL header (RO): %w", err)
	}
	if hdr.Magic != common.MagicWAL || hdr.Version != common.VersionWAL {
		return fmt.Errorf("invalid WAL header (RO)")
	}

	reader := bufio.NewReaderSize(f, 1<<20)
	for {
		rec, _, rerr := readRecordRO(reader)
		if rerr == io.EOF || errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			// best-effort: log and stop this file
			logger.Warn("stop replay due to record error (RO)", "path", path, "error", rerr)
			break
		}
		var expiresAt time.Time
		if rec.TTL > 0 {
			abs := int64(rec.TTL)
			if abs > 0 {
				expiresAt = time.Unix(0, abs)
			}
		}
		if err := callback(rec.Op, rec.Key, expiresAt); err != nil {
			return fmt.Errorf("replay callback error (RO): %w", err)
		}
	}
	return nil
}

func readHeaderRO(file *os.File) (WALHeader, error) {
	buf := make([]byte, 14)
	if _, err := io.ReadFull(file, buf); err != nil {
		return WALHeader{}, err
	}
	return WALHeader{
		Magic:     binary.LittleEndian.Uint32(buf[0:4]),
		Version:   binary.LittleEndian.Uint16(buf[4:6]),
		CreatedAt: int64(binary.LittleEndian.Uint64(buf[6:14])),
	}, nil
}

func readRecordRO(reader *bufio.Reader) (WALRecord, int, error) {
	// Read op as varint
	op, err := utils.ReadUvarint(reader)
	if err == io.EOF {
		return WALRecord{}, 0, io.EOF
	}
	if err != nil {
		return WALRecord{}, 0, fmt.Errorf("read op: %w", err)
	}
	// Read key length
	keyLen, err := utils.ReadUvarint(reader)
	if err != nil {
		return WALRecord{}, 0, fmt.Errorf("read key length: %w", err)
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return WALRecord{}, 0, fmt.Errorf("read key: %w", err)
	}
	// Read expiry
	expBuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, expBuf); err != nil {
		return WALRecord{}, 0, fmt.Errorf("read expiry: %w", err)
	}
	// Read CRC
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, crcBuf); err != nil {
		return WALRecord{}, 0, fmt.Errorf("read CRC: %w", err)
	}

	expectedCRC := binary.LittleEndian.Uint32(crcBuf)
	// Verify CRC
	var buf bytes.Buffer
	utils.WriteUvarint(&buf, uint64(op))
	utils.WriteUvarint(&buf, uint64(keyLen))
	buf.Write(key)
	buf.Write(expBuf)
	actualCRC := utils.ComputeCRC32C(buf.Bytes())
	if actualCRC != expectedCRC {
		return WALRecord{}, 0, common.ErrCRCMismatch
	}
	size := 0
	size += enc.SizeUvarint(uint64(op))
	size += enc.SizeUvarint(uint64(keyLen))
	size += int(keyLen)
	size += 8
	size += 4
	abs := int64(binary.LittleEndian.Uint64(expBuf))
	var ttlAsDuration time.Duration
	if abs > 0 {
		ttlAsDuration = time.Duration(abs)
	}
	return WALRecord{Op: uint8(op), Key: key, TTL: ttlAsDuration, CRC32C: expectedCRC}, size, nil
}
