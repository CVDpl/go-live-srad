package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// AtomicFile provides atomic file operations.
type AtomicFile struct {
	path     string
	tempPath string
	file     *os.File
	mu       sync.Mutex
}

// NewAtomicFile creates a new atomic file writer.
func NewAtomicFile(path string) (*AtomicFile, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	tempPath := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	return &AtomicFile{
		path:     path,
		tempPath: tempPath,
		file:     file,
	}, nil
}

// Write writes data to the temporary file.
func (af *AtomicFile) Write(p []byte) (n int, err error) {
	af.mu.Lock()
	defer af.mu.Unlock()

	if af.file == nil {
		return 0, fmt.Errorf("file is closed")
	}

	return af.file.Write(p)
}

// Commit syncs and atomically renames the temporary file to the final path.
func (af *AtomicFile) Commit() error {
	af.mu.Lock()
	defer af.mu.Unlock()

	if af.file == nil {
		return fmt.Errorf("file is closed")
	}

	// Sync file contents
	if err := af.file.Sync(); err != nil {
		return fmt.Errorf("sync file: %w", err)
	}

	// Close the file
	if err := af.file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	af.file = nil

	// Atomic rename
	if err := os.Rename(af.tempPath, af.path); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	// Sync directory to ensure rename is persisted
	if err := SyncDir(filepath.Dir(af.path)); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}

	return nil
}

// Abort removes the temporary file without committing.
func (af *AtomicFile) Abort() error {
	af.mu.Lock()
	defer af.mu.Unlock()

	if af.file != nil {
		af.file.Close()
		af.file = nil
	}

	return os.Remove(af.tempPath)
}

// Close ensures cleanup of resources.
func (af *AtomicFile) Close() error {
	af.mu.Lock()
	defer af.mu.Unlock()

	if af.file != nil {
		af.file.Close()
		af.file = nil
		os.Remove(af.tempPath)
	}

	return nil
}

// SyncDir syncs a directory to ensure file operations are persisted.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	return d.Sync()
}

// WriteAll writes all data to a writer.
func WriteAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// AlignTo aligns a value to the specified alignment.
func AlignTo(value, alignment int64) int64 {
	if alignment <= 0 {
		return value
	}
	remainder := value % alignment
	if remainder == 0 {
		return value
	}
	return value + alignment - remainder
}

// PadToAlignment pads data to the specified alignment with zeros.
func PadToAlignment(data []byte, alignment int) []byte {
	if alignment <= 0 || len(data)%alignment == 0 {
		return data
	}

	padSize := alignment - (len(data) % alignment)
	padding := make([]byte, padSize)
	return append(data, padding...)
}

// ReadUvarint reads a variable-length encoded integer.
func ReadUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// WriteUvarint writes a variable-length encoded integer.
func WriteUvarint(w io.Writer, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	_, err := w.Write(buf[:n])
	return err
}

// FileExists checks if a file exists.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DirExists checks if a directory exists.
func DirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// CreateDirIfNotExists creates a directory if it doesn't exist.
func CreateDirIfNotExists(path string) error {
	if !DirExists(path) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

// RemoveAll removes a file or directory recursively.
func RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// TruncateFile truncates a file to the specified size.
func TruncateFile(path string, size int64) error {
	return os.Truncate(path, size)
}

// Fdatasync calls fdatasync on the file (or fsync on platforms without fdatasync).
func Fdatasync(f *os.File) error {
	// On macOS, use F_FULLFSYNC for similar behavior to fdatasync
	// On other platforms, fall back to fsync
	return f.Sync()
}

// MemoryMap represents a memory-mapped file.
type MemoryMap struct {
	data []byte
	file *os.File
}

// MapFile memory-maps a file for reading.
func MapFile(path string) (*MemoryMap, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if stat.Size() == 0 {
		return &MemoryMap{
			data: []byte{},
			file: file,
		}, nil
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &MemoryMap{
		data: data,
		file: file,
	}, nil
}

// Data returns the mapped data.
func (m *MemoryMap) Data() []byte {
	return m.data
}

// Close unmaps the file and closes it.
func (m *MemoryMap) Close() error {
	if len(m.data) > 0 {
		if err := syscall.Munmap(m.data); err != nil {
			m.file.Close()
			return err
		}
	}
	return m.file.Close()
}

// QuarantineFile moves a corrupted file to a .corrupt extension.
func QuarantineFile(path string) error {
	corruptPath := path + ".corrupt"
	return os.Rename(path, corruptPath)
}
