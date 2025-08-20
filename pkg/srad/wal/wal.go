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
}

// Config controls WAL sizing and buffering.
type Config struct {
	RotateSize  int64
	MaxFileSize int64
	BufferSize  int
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

// NullLogger is a logger that discards all log messages.
type NullLogger struct{}

func (n *NullLogger) Debug(msg string, fields ...interface{}) {}
func (n *NullLogger) Info(msg string, fields ...interface{})  {}
func (n *NullLogger) Warn(msg string, fields ...interface{})  {}
func (n *NullLogger) Error(msg string, fields ...interface{}) {}

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
		logger = &NullLogger{}
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
		dir:         dir,
		rotateSize:  cfg.RotateSize,
		maxFileSize: cfg.MaxFileSize,
		bufferSize:  cfg.BufferSize,
		logger:      logger,
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
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("write WAL record: %w", err)
	}

	w.currentSize += int64(len(data))

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

	// Write TTL as int64 (nanoseconds)
	ttlNanos := int64(record.TTL)
	ttlBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(ttlBuf, uint64(ttlNanos))
	buf.Write(ttlBuf)

	// Compute CRC32C
	data := buf.Bytes()
	crc := utils.ComputeCRC32C(data)

	// Append CRC32C
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	buf.Write(crcBuf)

	return buf.Bytes(), nil
}

// Replay replays all WAL files and calls the callback for each record.
func (w *WAL) Replay(callback func(op uint8, key []byte, ttl time.Duration) error) error {
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
func (w *WAL) replayFile(path string, callback func(op uint8, key []byte, ttl time.Duration) error) error {
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
	reader := bufio.NewReader(file)
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

		// Call callback
		if err := callback(record.Op, record.Key, record.TTL); err != nil {
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
	startPos := 0

	// Read op as varint
	op, err := utils.ReadUvarint(reader)
	if err == io.EOF {
		return WALRecord{}, startPos, io.EOF
	}
	if err != nil {
		return WALRecord{}, startPos, fmt.Errorf("read op: %w", err)
	}
	startPos += binary.MaxVarintLen64 // approximate

	// Read key length as varint
	keyLen, err := utils.ReadUvarint(reader)
	if err != nil {
		return WALRecord{}, startPos, fmt.Errorf("read key length: %w", err)
	}
	startPos += binary.MaxVarintLen64 // approximate

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return WALRecord{}, startPos, fmt.Errorf("read key: %w", err)
	}
	startPos += int(keyLen)

	// Read TTL as int64 (nanoseconds)
	ttlBuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, ttlBuf); err != nil {
		return WALRecord{}, startPos, fmt.Errorf("read TTL: %w", err)
	}
	ttlNanos := int64(binary.LittleEndian.Uint64(ttlBuf))
	ttl := time.Duration(ttlNanos)
	startPos += 8

	// Read CRC32C
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, crcBuf); err != nil {
		return WALRecord{}, startPos, fmt.Errorf("read CRC: %w", err)
	}
	expectedCRC := binary.LittleEndian.Uint32(crcBuf)
	startPos += 4

	// Verify CRC
	var buf bytes.Buffer
	utils.WriteUvarint(&buf, uint64(op))
	utils.WriteUvarint(&buf, uint64(keyLen))
	buf.Write(key)
	buf.Write(ttlBuf)

	actualCRC := utils.ComputeCRC32C(buf.Bytes())
	if actualCRC != expectedCRC {
		return WALRecord{}, startPos, common.ErrCRCMismatch
	}

	return WALRecord{
		Op:     uint8(op),
		Key:    key,
		TTL:    ttl,
		CRC32C: expectedCRC,
	}, startPos, nil
}

// DeleteOldFiles deletes WAL files older than the specified sequence.
func (w *WAL) DeleteOldFiles(beforeSeq uint64) error {
	files, err := w.listWALFiles()
	if err != nil {
		return fmt.Errorf("list WAL files: %w", err)
	}

	for _, path := range files {
		seq := w.extractSequence(path)
		if seq < beforeSeq {
			if err := os.Remove(path); err != nil {
				w.logger.Warn("failed to delete old WAL file", "path", path, "error", err)
				// Continue with other files
			} else {
				w.logger.Info("deleted old WAL file", "path", path, "seq", seq)
			}
		}
	}

	return nil
}
