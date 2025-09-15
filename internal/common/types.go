package common

import (
	"errors"
	"time"
)

// Operation codes for WAL and operations
const (
	OpInsert uint8 = 1
	OpDelete uint8 = 2
)

// File format magic numbers (little-endian)
const (
	MagicWAL     uint32 = 0x314C4157 // "WAL1" in little-endian
	MagicLouds   uint32 = 0x44554F4C // "LOUD" in little-endian
	MagicBloom   uint32 = 0x4D4F4C42 // "BLOM" in little-endian
	MagicTrigram uint32 = 0x47495254 // "TRIG" in little-endian
	MagicKeys    uint32 = 0x5359454B // "KEYS" in little-endian
	MagicTombs   uint32 = 0x424D4F54 // "TOMB" in little-endian
	MagicExpiry  uint32 = 0x59525058 // "EXPR" in little-endian
)

// File format versions
const (
	VersionWAL     uint16 = 0x0100
	VersionSegment uint16 = 0x0100
)

// Segment levels
const (
	LevelL0 = 0
	LevelL1 = 1
	LevelL2 = 2
	LevelL3 = 3
	LevelL4 = 4
)

// Size limits
const (
	MaxKeySize         = 1024 * 1024 // 1MB max key size
	RecommendedKeySize = 64 * 1024   // 64KB recommended max
	HeaderAlignment    = 64          // Headers aligned to 64 bytes
)

// Default configuration values
const (
	DefaultMemtableTargetBytes     = 512 * 1024 * 1024 // 512MB
	DefaultCacheLabelAdvanceBytes  = 32 * 1024 * 1024  // 32MB
	DefaultCacheNFATransitionBytes = 32 * 1024 * 1024  // 32MB
	DefaultCompactionPriority      = 3
	DefaultFilterThreshold         = 0.1
	DefaultPrefixBloomLength       = 16
	DefaultBloomFPR                = 0.01
)

// WAL configuration
const (
	WALRotateSize  = 128 * 1024 * 1024  // 128MB rotation size
	WALMaxFileSize = 1024 * 1024 * 1024 // 1GB max file size
	WALBufferSize  = 256 * 1024         // 256KB write buffer
)

// Common errors
var (
	ErrClosed             = errors.New("store is closed")
	ErrReadOnly           = errors.New("store is read-only")
	ErrCorrupt            = errors.New("data corruption detected")
	ErrUnsupportedVersion = errors.New("unsupported file version")
	ErrCanceled           = errors.New("operation canceled")
	ErrKeyTooLarge        = errors.New("key exceeds maximum size")
	ErrInvalidMagic       = errors.New("invalid file magic number")
	ErrCRCMismatch        = errors.New("CRC checksum mismatch")
	ErrInvalidOffset      = errors.New("invalid file offset")
	ErrSegmentNotFound    = errors.New("segment not found")
	ErrManifestNotFound   = errors.New("manifest not found")
	ErrWALCorrupted       = errors.New("WAL file corrupted")
	ErrEmptyKey           = errors.New("empty key not allowed")

	// TTL-related errors
	ErrKeyExpired = errors.New("key has expired")
	ErrInvalidTTL = errors.New("invalid TTL value")

	// TTL constants
	MaxTTL                   = 200 * 365 * 24 * time.Hour // 200 years max TTL
	DefaultTTL time.Duration = 0                          // 0 = never expire by default
)

// File paths within store directory
const (
	DirWAL       = "wal"
	DirSegments  = "segments"
	DirManifest  = "manifest"
	FileManifest = "manifest.json"
	FileCurrent  = "CURRENT"
	FileTuning   = "tuning.json"
)

// Segment file names
const (
	FileIndexLouds  = "index.louds"
	FileSegmentMeta = "segment.json"
	FilePrefixBloom = "filters/prefix.bf"
	FileTrigramBits = "filters/tri.bits"
)

// MaxLevel is the maximum compaction level
const (
	MaxLevel = 10
)

// RCU configuration
const (
	RCUGracePeriod = 30 // seconds
	RCUMaxEpochs   = 3  // maximum concurrent epochs
)

// Logger provides structured logging.
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
}

// LogLevel represents the severity of a log message.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)
