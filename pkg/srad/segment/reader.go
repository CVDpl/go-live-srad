package segment

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/internal/encoding"
	"github.com/CVDpl/go-live-srad/internal/filters"
	q "github.com/CVDpl/go-live-srad/pkg/srad/query"
	"golang.org/x/sys/unix"
)

// Reader provides read access to an immutable segment.
type Reader struct {
	segmentID uint64
	dir       string
	metadata  *Metadata

	// Cached structures
	louds   *encoding.LOUDS
	keys    [][]byte // fallback list for iteration
	keysMap []byte   // mmapped keys.dat (optional)
	// Expiry data per key (parallel to keys order); unix nanos, 0 if none
	expiries []int64

	// File handles (memory mapped in future)
	loudsFile  *os.File
	edgesFile  *os.File
	acceptFile *os.File
	keysFile   *os.File
	expFile    *os.File

	logger common.Logger

	// Trigram filter
	trigram *filters.TrigramFilter
	triFile *os.File

	// Bloom filter for prefixes
	bloom     *filters.BloomFilter
	bloomFile *os.File

	// Tombstones
	tombFile     *os.File
	tombstoneSet map[string]struct{}

	// Verification
	verifyChecksums bool

	// mmapped file content (entire file). We slice payload at [256:].
	mmap []byte
}

// NewReader creates a new segment reader.
func NewReader(segmentID uint64, dir string, logger common.Logger, verifyChecksums bool) (*Reader, error) {
	if logger == nil {
		logger = &NullLogger{}
	}

	segmentDir := filepath.Join(dir, fmt.Sprintf("%016d", segmentID))

	// Load metadata
	metadataPath := filepath.Join(segmentDir, "segment.json")
	metadata, err := LoadFromFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	reader := &Reader{
		segmentID:       segmentID,
		dir:             segmentDir,
		metadata:        metadata,
		logger:          logger,
		verifyChecksums: verifyChecksums,
	}

	// Open segment files
	if err := reader.openFiles(); err != nil {
		return nil, fmt.Errorf("open files: %w", err)
	}

	// Defer LOUDS/keys loading until first use to reduce memory footprint

	return reader, nil
}

// MinKey returns a copy of the minimum key from metadata if available.
func (r *Reader) MinKey() ([]byte, bool) {
	if r.metadata == nil {
		return nil, false
	}
	mk, err := r.metadata.GetMinKey()
	if err != nil || len(mk) == 0 {
		return nil, false
	}
	out := make([]byte, len(mk))
	copy(out, mk)
	return out, true
}

// Bloom returns the loaded bloom filter (may be nil if not present).
func (r *Reader) Bloom() *filters.BloomFilter { return r.bloom }

// Trigram returns the loaded trigram filter (may be nil if not present).
func (r *Reader) Trigram() *filters.TrigramFilter { return r.trigram }

// openFiles opens the segment files.
func (r *Reader) openFiles() error {
	var err error

	// Open LOUDS file
	loudsPath := filepath.Join(r.dir, r.metadata.Files.Louds)
	r.loudsFile, err = os.Open(loudsPath)
	if err != nil {
		return fmt.Errorf("open LOUDS file: %w", err)
	}

	// Open edges file
	edgesPath := filepath.Join(r.dir, r.metadata.Files.Edges)
	r.edgesFile, err = os.Open(edgesPath)
	if err != nil {
		r.loudsFile.Close()
		return fmt.Errorf("open edges file: %w", err)
	}

	// Open accept file
	acceptPath := filepath.Join(r.dir, r.metadata.Files.Accept)
	r.acceptFile, err = os.Open(acceptPath)
	if err != nil {
		r.loudsFile.Close()
		r.edgesFile.Close()
		return fmt.Errorf("open accept file: %w", err)
	}

	// Open keys file (optional)
	keysPath := filepath.Join(r.dir, "keys.dat")
	if f, err := os.Open(keysPath); err == nil {
		r.keysFile = f
		// Try mmap keys file to avoid heap copies
		if stat, err := f.Stat(); err == nil && stat.Size() > 0 {
			if mapped, mErr := unix.Mmap(int(f.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED); mErr == nil {
				r.keysMap = mapped
			}
		}
	}

	// Open expiry file (optional)
	expPath := filepath.Join(r.dir, "expiry.dat")
	if f, err := os.Open(expPath); err == nil {
		r.expFile = f
	}

	// Open trigram filter (optional)
	triPath := filepath.Join(r.dir, "filters", "tri.bits")
	if f, err := os.Open(triPath); err == nil {
		r.triFile = f
	}

	// Open bloom filter (optional)
	bloomPath := filepath.Join(r.dir, "filters", "prefix.bf")
	if f, err := os.Open(bloomPath); err == nil {
		r.bloomFile = f
	}

	// Open tombstones (optional)
	tombPath := filepath.Join(r.dir, "tombstones.dat")
	if f, err := os.Open(tombPath); err == nil {
		r.tombFile = f
	}

	return nil
}

// loadLOUDS loads the LOUDS structure from file.
func (r *Reader) loadLOUDS() error {
	// Prefer mmap over heap allocation to reduce heap usage
	stat, err := r.loudsFile.Stat()
	if err != nil {
		return err
	}
	if stat.Size() < 6 {
		return fmt.Errorf("invalid LOUDS file size")
	}
	fd := int(r.loudsFile.Fd())
	mapped, merr := unix.Mmap(fd, 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if merr != nil {
		// Fallback: read into heap if mmap fails
		if _, err := r.loudsFile.Seek(0, 0); err != nil {
			return err
		}
		buf := make([]byte, stat.Size())
		if _, err := r.loudsFile.Read(buf); err != nil {
			return fmt.Errorf("read LOUDS file: %w", err)
		}
		magic := binary.LittleEndian.Uint32(buf[0:4])
		if magic != common.MagicLouds {
			return fmt.Errorf("invalid LOUDS magic: 0x%08x", magic)
		}
		payload := buf[6:]
		l, uerr := encoding.UnmarshalLOUDS(payload)
		if uerr != nil {
			return fmt.Errorf("unmarshal LOUDS: %w", uerr)
		}
		r.louds = l
	} else {
		// Keep mapping for lifetime of reader
		r.mmap = mapped
		header := mapped[:6]
		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != common.MagicLouds {
			return fmt.Errorf("invalid LOUDS magic: 0x%08x", magic)
		}
		payload := mapped[6:]
		l, uerr := encoding.UnmarshalLOUDS(payload)
		if uerr != nil {
			return fmt.Errorf("unmarshal LOUDS: %w", uerr)
		}
		r.louds = l
	}

	// Load trigram filter if present
	if r.triFile != nil {
		// Read full file (6B header + payload)
		if stat, err := r.triFile.Stat(); err == nil && stat.Size() >= 6 {
			if _, err := r.triFile.Seek(0, 0); err == nil {
				full := make([]byte, stat.Size())
				if _, err := r.triFile.Read(full); err == nil {
					if binary.LittleEndian.Uint32(full[0:4]) == common.MagicTrigram {
						payload := full[6:]
						r.trigram = filters.UnmarshalTrigram(payload)
					}
				}
				r.triFile.Seek(0, 0)
			}
		}
	}

	// Load bloom filter if present
	if r.bloomFile != nil {
		if stat, err := r.bloomFile.Stat(); err == nil && stat.Size() >= 6 {
			if _, err := r.bloomFile.Seek(0, 0); err == nil {
				full := make([]byte, stat.Size())
				if _, err := r.bloomFile.Read(full); err == nil {
					if binary.LittleEndian.Uint32(full[0:4]) == common.MagicBloom {
						payload := full[6:]
						r.bloom = filters.UnmarshalBloomFilter(payload)
					}
				}
				r.bloomFile.Seek(0, 0)
			}
		}
	}

	// Load tombstones if present
	if r.tombFile != nil {
		if err := r.loadTombstones(); err != nil {
			// non-fatal
			r.logger.Warn("failed to load tombstones", "id", r.segmentID, "error", err)
		}
	}

	// Load expiries if present
	if r.expFile != nil {
		if err := r.loadExpiries(); err != nil {
			r.logger.Warn("failed to load expiries", "id", r.segmentID, "error", err)
		}
	}

	return nil
}

func (r *Reader) loadKeys() error {
	if r.keysFile == nil {
		return nil
	}
	// If mmap is available, parse with mandatory 6B header
	if len(r.keysMap) >= 6 {
		data := r.keysMap
		// Mandatory 6B common header (magic+version)
		if binary.LittleEndian.Uint32(data[0:4]) != common.MagicKeys {
			return fmt.Errorf("keys file invalid magic")
		}
		off := 6
		if off+4 <= len(data) {
			count := binary.LittleEndian.Uint32(data[off : off+4])
			off += 4
			keys := make([][]byte, 0, count)
			ok := true
			for i := uint32(0); i < count; i++ {
				if off+4 > len(data) {
					ok = false
					break
				}
				kl := int(binary.LittleEndian.Uint32(data[off : off+4]))
				off += 4
				if off+kl > len(data) {
					ok = false
					break
				}
				k := make([]byte, kl)
				copy(k, data[off:off+kl])
				off += kl
				keys = append(keys, k)
			}
			if ok {
				r.keys = keys
				return nil
			}
		}
	}

	r.logger.Warn("keys.dat is not mmapable, falling back to file read", "id", r.segmentID)

	// File-based path with mandatory header
	defer r.keysFile.Seek(0, 0)
	var count uint32
	// Read and validate required header
	hdr, err := ReadCommonHeader(r.keysFile)
	if err != nil {
		return err
	}
	if err := ValidateHeader(hdr, common.MagicKeys, common.VersionSegment); err != nil {
		return err
	}
	if err := binary.Read(r.keysFile, binary.LittleEndian, &count); err != nil {
		return err
	}
	keys := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		var kl uint32
		if err := binary.Read(r.keysFile, binary.LittleEndian, &kl); err != nil {
			return err
		}
		k := make([]byte, kl)
		if _, err := r.keysFile.Read(k); err != nil {
			return err
		}
		keys = append(keys, k)
	}
	r.keys = keys
	return nil
}

// IterateKeys streams keys from keys.dat sequentially without loading all into memory.
// It returns the number of keys iterated and the first error encountered.
func (r *Reader) IterateKeys(fn func(k []byte) bool) (int, error) {
	if r.keysFile == nil {
		return 0, nil
	}
	if _, err := r.keysFile.Seek(0, 0); err != nil {
		return 0, err
	}
	// Optional header
	if hdr, err := ReadCommonHeader(r.keysFile); err == nil {
		if err := ValidateHeader(hdr, common.MagicKeys, common.VersionSegment); err != nil {
			return 0, err
		}
	} else {
		return 0, err
	}
	var count uint32
	if err := binary.Read(r.keysFile, binary.LittleEndian, &count); err != nil {
		return 0, err
	}
	iterated := 0
	for i := uint32(0); i < count; i++ {
		var kl uint32
		if err := binary.Read(r.keysFile, binary.LittleEndian, &kl); err != nil {
			return iterated, err
		}
		if kl == 0 {
			if !fn(nil) {
				return iterated, nil
			}
			iterated++
			continue
		}
		buf := make([]byte, kl)
		if _, err := r.keysFile.Read(buf); err != nil {
			return iterated, err
		}
		if !fn(buf) {
			return iterated, nil
		}
		iterated++
	}
	return iterated, nil
}

func (r *Reader) loadTombstones() error {
	defer r.tombFile.Seek(0, 0)
	var count uint32
	if hdr, err := ReadCommonHeader(r.tombFile); err == nil {
		if err := ValidateHeader(hdr, common.MagicTombs, common.VersionSegment); err != nil {
			return err
		}
	} else {
		return err
	}
	if err := binary.Read(r.tombFile, binary.LittleEndian, &count); err != nil {
		return err
	}
	set := make(map[string]struct{}, count)
	for i := uint32(0); i < count; i++ {
		var kl uint32
		if err := binary.Read(r.tombFile, binary.LittleEndian, &kl); err != nil {
			return err
		}
		k := make([]byte, kl)
		if _, err := r.tombFile.Read(k); err != nil {
			return err
		}
		set[string(k)] = struct{}{}
	}
	r.tombstoneSet = set
	return nil
}

// IterateTombstones streams tombstone keys from tombstones.dat without building the full in-memory set.
// It returns the number of tombstones iterated and the first error encountered.
func (r *Reader) IterateTombstones(fn func(k []byte) bool) (int, error) {
	if r.tombFile == nil {
		return 0, nil
	}
	defer r.tombFile.Seek(0, 0)
	// Read and validate header
	hdr, err := ReadCommonHeader(r.tombFile)
	if err != nil {
		return 0, err
	}
	if err := ValidateHeader(hdr, common.MagicTombs, common.VersionSegment); err != nil {
		return 0, err
	}
	var count uint32
	if err := binary.Read(r.tombFile, binary.LittleEndian, &count); err != nil {
		return 0, err
	}
	iterated := 0
	for i := uint32(0); i < count; i++ {
		var kl uint32
		if err := binary.Read(r.tombFile, binary.LittleEndian, &kl); err != nil {
			return iterated, err
		}
		k := make([]byte, kl)
		if _, err := r.tombFile.Read(k); err != nil {
			return iterated, err
		}
		if !fn(k) {
			return iterated, nil
		}
		iterated++
	}
	return iterated, nil
}

// HasTombstone reports if key is tombstoned in this segment.
func (r *Reader) HasTombstone(key []byte) bool {
	if r.tombstoneSet == nil {
		return false
	}
	_, ok := r.tombstoneSet[string(key)]
	return ok
}

func (r *Reader) loadExpiries() error {
	if r.expFile == nil {
		return nil
	}
	defer r.expFile.Seek(0, 0)
	// Read and validate header
	hdr, err := ReadCommonHeader(r.expFile)
	if err != nil {
		return err
	}
	if err := ValidateHeader(hdr, common.MagicExpiry, common.VersionSegment); err != nil {
		return err
	}
	var count uint32
	if err := binary.Read(r.expFile, binary.LittleEndian, &count); err != nil {
		return err
	}
	exps := make([]int64, count)
	for i := uint32(0); i < count; i++ {
		var e int64
		if err := binary.Read(r.expFile, binary.LittleEndian, &e); err != nil {
			return err
		}
		exps[i] = e
	}
	r.expiries = exps
	return nil
}

func (r *Reader) isExpiredKeyIndex(idx int) bool {
	if idx < 0 {
		return false
	}
	if idx >= len(r.expiries) {
		return false
	}
	e := r.expiries[idx]
	if e == 0 {
		return false
	}
	return time.Now().UnixNano() >= e
}

// Get retrieves a value by key.
func (r *Reader) Get(key []byte) ([]byte, bool) {
	if r.louds == nil {
		if err := r.loadLOUDS(); err != nil {
			r.logger.Warn("failed to load LOUDS", "id", r.segmentID, "error", err)
			return nil, false
		}
	}
	val, ok := r.louds.Search(key)
	if !ok {
		return nil, false
	}
	// If we have keys loaded, check expiry via index lookup
	if len(r.keys) > 0 && len(r.expiries) == len(r.keys) {
		// binary search to find index
		i := sort.Search(len(r.keys), func(i int) bool { return bytes.Compare(r.keys[i], key) >= 0 })
		if i < len(r.keys) && bytes.Equal(r.keys[i], key) && r.isExpiredKeyIndex(i) {
			return nil, false
		}
	}
	return val, ok
}

// Contains checks if a key exists in the segment.
func (r *Reader) Contains(key []byte) bool {
	_, found := r.Get(key)
	return found
}

// Iterator returns an iterator for the segment.
func (r *Reader) Iterator() *SegmentIterator {
	return &SegmentIterator{
		reader: r,
		stack:  []iteratorState{{node: 1, key: nil}}, // Start at root
	}
}

// RegexIterator returns an iterator for regex matching.
func (r *Reader) RegexIterator(pattern *regexp.Regexp) *SegmentIterator {
	// Ensure tombstones are loaded before iteration to suppress deleted keys
	if r.tombstoneSet == nil && r.tombFile != nil {
		_ = r.loadTombstones()
	}
	// Ensure expiries are loaded
	if len(r.expiries) == 0 && r.expFile != nil {
		_ = r.loadExpiries()
	}
	return &SegmentIterator{
		reader:  r,
		pattern: pattern,
		stack:   []iteratorState{{node: 1, key: nil}}, // Start at root
	}
}

// Close closes the segment reader.
func (r *Reader) Close() error {
	if r.loudsFile != nil {
		r.loudsFile.Close()
	}
	if r.edgesFile != nil {
		r.edgesFile.Close()
	}
	if r.acceptFile != nil {
		r.acceptFile.Close()
	}
	if r.keysFile != nil {
		r.keysFile.Close()
	}
	if r.expFile != nil {
		r.expFile.Close()
	}
	if r.triFile != nil {
		r.triFile.Close()
	}
	if r.bloomFile != nil {
		r.bloomFile.Close()
	}
	if r.tombFile != nil {
		r.tombFile.Close()
	}
	// Unmap if mapped
	if r.mmap != nil {
		_ = unix.Munmap(r.mmap)
		r.mmap = nil
	}

	return nil
}

// GetMetadata returns the segment metadata.
func (r *Reader) GetMetadata() *Metadata { return r.metadata }

// GetSegmentID returns the segment ID.
func (r *Reader) GetSegmentID() uint64 { return r.segmentID }

// AllKeys returns the list of keys stored in this segment (from keys.dat).
func (r *Reader) AllKeys() [][]byte {
	if r.keys == nil {
		if err := r.loadKeys(); err != nil {
			r.logger.Warn("failed to load keys", "id", r.segmentID, "error", err)
		}
	}
	return r.keys
}

// Tombstones returns the list of tombstoned keys recorded in this segment.
func (r *Reader) Tombstones() [][]byte {
	if r.tombstoneSet == nil {
		return nil
	}
	out := make([][]byte, 0, len(r.tombstoneSet))
	for k := range r.tombstoneSet {
		out = append(out, []byte(k))
	}
	return out
}

// MayContain returns true if the segment may contain matches for the regex.
// Uses Bloom filter for literal prefix pruning, then trigram filter; otherwise returns true.
func (r *Reader) MayContain(pattern *regexp.Regexp) bool {
	if r == nil || pattern == nil {
		return true
	}
	lit, _ := pattern.LiteralPrefix()
	if lit != "" && r.bloom != nil {
		b := []byte(lit)
		// Our builder added prefixes up to 10 bytes; exact check is most selective
		if len(b) <= 16 { // be lenient; Bloom still works for longer but may be higher FPR
			if !r.bloom.Contains(b) {
				return false
			}
		}
	}
	if r.trigram == nil {
		return true
	}
	if lit != "" {
		// For pruning we only need 3+ bytes literal for trigram
		b := []byte(lit)
		if len(b) >= 3 {
			return r.trigram.MayContainLiteral(b)
		}
	}
	return true
}

// StreamKeys provides a streaming closure over keys.dat for k-way merge.
// The returned advance func yields the next key (copy) and ok=false on end/error.
func (r *Reader) StreamKeys() (advance func() ([]byte, bool)) {
	if r.keysFile == nil {
		return func() ([]byte, bool) { return nil, false }
	}
	if _, err := r.keysFile.Seek(0, 0); err != nil {
		return func() ([]byte, bool) { return nil, false }
	}
	hdr, err := ReadCommonHeader(r.keysFile)
	if err != nil || ValidateHeader(hdr, common.MagicKeys, common.VersionSegment) != nil {
		return func() ([]byte, bool) { return nil, false }
	}
	var count uint32
	if err := binary.Read(r.keysFile, binary.LittleEndian, &count); err != nil {
		return func() ([]byte, bool) { return nil, false }
	}
	var i uint32
	return func() ([]byte, bool) {
		if i >= count {
			return nil, false
		}
		var kl uint32
		if err := binary.Read(r.keysFile, binary.LittleEndian, &kl); err != nil {
			return nil, false
		}
		var k []byte
		if kl > 0 {
			k = make([]byte, kl)
			if _, err := io.ReadFull(r.keysFile, k); err != nil {
				return nil, false
			}
		}
		// If expiries were loaded into memory and are aligned, check and drop expired
		if len(r.expiries) > 0 && len(r.keys) > 0 {
			// On streaming, we don't have index; this path is primarily for compaction streams; we'll filter by comparing to in-memory keys if loaded
		}
		i++
		return k, true
	}
}

// StreamTombstones provides a streaming closure over tombstones.dat.
// The returned advance func yields the next tombstone key (copy) and ok=false on end/error.
func (r *Reader) StreamTombstones() (advance func() ([]byte, bool)) {
	if r.tombFile == nil {
		return func() ([]byte, bool) { return nil, false }
	}
	if _, err := r.tombFile.Seek(0, 0); err != nil {
		return func() ([]byte, bool) { return nil, false }
	}
	hdr, err := ReadCommonHeader(r.tombFile)
	if err != nil || ValidateHeader(hdr, common.MagicTombs, common.VersionSegment) != nil {
		return func() ([]byte, bool) { return nil, false }
	}
	var count uint32
	if err := binary.Read(r.tombFile, binary.LittleEndian, &count); err != nil {
		return func() ([]byte, bool) { return nil, false }
	}
	var i uint32
	return func() ([]byte, bool) {
		if i >= count {
			return nil, false
		}
		var kl uint32
		if err := binary.Read(r.tombFile, binary.LittleEndian, &kl); err != nil {
			return nil, false
		}
		var k []byte
		if kl > 0 {
			k = make([]byte, kl)
			if _, err := io.ReadFull(r.tombFile, k); err != nil {
				return nil, false
			}
		}
		i++
		return k, true
	}
}

// SegmentIterator iterates over keys in a segment.
type SegmentIterator struct {
	reader  *Reader
	pattern *regexp.Regexp
	stack   []iteratorState
	current iteratorState
	err     error

	// Optimization: precomputed literal prefix
	literal    []byte
	hasLiteral bool

	// NFA engine (currently not used for pruning in segments)
	eng *q.Engine

	// Fallback iteration over keys list
	keysIndex int

	// Streaming fallback over keys.dat (preferred): mmap or buffered file
	streamInit   bool
	usingMmap    bool
	mmapData     []byte
	mmapOff      int
	keysCount    uint32
	keysRead     uint32
	streamFile   *os.File
	streamReader *bufio.Reader
}

type iteratorState struct {
	node uint64
	key  []byte
	// nfa carries product state; nil means unknown and we fall back to CanMatchPrefix
	nfa q.StateSet
}

// Next advances to the next matching key.
func (it *SegmentIterator) Next() bool {
	if it.reader == nil {
		return false
	}

	// If a trigram filter exists and we have a literal, quickly prune segments
	if it.pattern != nil && it.eng == nil {
		if lit, complete := it.pattern.LiteralPrefix(); lit != "" {
			it.literal = []byte(lit)
			it.hasLiteral = complete
		}
		it.eng = q.Compile(it.pattern)
	}
	if it.reader.trigram != nil && len(it.literal) >= 3 {
		if !it.reader.trigram.MayContainLiteral(it.literal) {
			return false
		}
	}

	// LOUDS-based DFS
	if it.reader.louds != nil && it.reader.louds.NumNodes() > 0 {
		if it.pattern != nil && it.eng == nil {
			if lit, complete := it.pattern.LiteralPrefix(); lit != "" {
				it.literal = []byte(lit)
				it.hasLiteral = complete
			}
			it.eng = q.Compile(it.pattern)
		}
		// Initialize product state at root
		if len(it.stack) == 1 && it.stack[0].node == 1 && it.stack[0].nfa == nil && it.eng != nil {
			it.stack[0].nfa = it.eng.StartState()
		}

		for len(it.stack) > 0 {
			state := it.stack[len(it.stack)-1]
			it.stack = it.stack[:len(it.stack)-1]

			if it.reader.louds.IsLeaf(state.node) {
				if (it.pattern == nil || it.pattern.Match(state.key)) && !it.reader.HasTombstone(state.key) {
					it.current = state
					return true
				}
			}

			child := it.reader.louds.FirstChild(state.node)
			var children = make([]iteratorState, 0, 8)
			for child != 0 {
				label := it.reader.louds.GetLabel(child)
				// Early pruning: if we have a literal and current depth is within it,
				// the next label must match the literal's next byte.
				if it.hasLiteral {
					pos := len(state.key)
					if pos < len(it.literal) && it.literal[pos] != label {
						child = it.reader.louds.NextSibling(child)
						continue
					}
				}
				nextState := state.nfa
				viable := true
				if it.eng != nil {
					// If we have an explicit state, step it; otherwise fallback check
					if nextState != nil {
						nextState, viable = it.eng.Step(nextState, label)
					} else {
						// Build minimal key only if needed for fallback viability check
						tmpKey := make([]byte, len(state.key)+1)
						copy(tmpKey, state.key)
						tmpKey[len(tmpKey)-1] = label
						viable = it.eng.CanMatchPrefix(tmpKey)
					}
				}
				if viable {
					childKey := append(append([]byte(nil), state.key...), label)
					children = append(children, iteratorState{node: child, key: childKey, nfa: nextState})
				}
				child = it.reader.louds.NextSibling(child)
			}
			for i := len(children) - 1; i >= 0; i-- {
				it.stack = append(it.stack, children[i])
			}
		}
	}

	// Fallback to keys: prefer streaming over keys.dat to avoid loading all keys into memory
	if !it.streamInit {
		it.streamInit = true
		// Try mmap first (zero-copy) with mandatory 6B header
		if len(it.reader.keysMap) > 0 {
			data := it.reader.keysMap
			if len(data) >= 6 && binary.LittleEndian.Uint32(data[0:4]) == common.MagicKeys {
				off := 6
				// Read count
				if off+4 <= len(data) {
					it.keysCount = binary.LittleEndian.Uint32(data[off : off+4])
					it.mmapOff = off + 4
					it.usingMmap = true
					it.mmapData = data
				}
			}
		}
		// If no mmap, create a dedicated buffered reader over keys.dat (mandatory header)
		if !it.usingMmap {
			// Open a separate file handle to avoid interfering with other readers
			path := filepath.Join(it.reader.dir, "keys.dat")
			if f, err := os.Open(path); err == nil {
				it.streamFile = f
				it.streamReader = bufio.NewReaderSize(f, 256*1024)
				// Mandatory header
				hdr, err := ReadCommonHeader(it.streamReader)
				if err != nil {
					_ = f.Close()
					it.streamFile = nil
					it.streamReader = nil
				} else if err := ValidateHeader(hdr, common.MagicKeys, common.VersionSegment); err != nil {
					_ = f.Close()
					it.streamFile = nil
					it.streamReader = nil
				} else {
					// Read count
					if err := binary.Read(it.streamReader, binary.LittleEndian, &it.keysCount); err != nil {
						// Disable streaming on error
						it.keysCount = 0
					}
				}
			}
		}
	}

	// Streaming via mmap
	if it.usingMmap && it.mmapData != nil {
		data := it.mmapData
		for it.keysRead < it.keysCount {
			// Read key length
			if it.mmapOff+4 > len(data) {
				break
			}
			kl := int(binary.LittleEndian.Uint32(data[it.mmapOff : it.mmapOff+4]))
			it.mmapOff += 4
			if kl < 0 || it.mmapOff+kl > len(data) {
				break
			}
			k := data[it.mmapOff : it.mmapOff+kl]
			it.mmapOff += kl
			it.keysRead++
			if (it.pattern == nil || it.pattern.Match(k)) && !it.reader.HasTombstone(k) {
				// Note: k points into mmap; valid until reader is closed
				if len(it.reader.keys) == 0 {
					// Attempt to lazy load keys to align expiries
					_ = it.reader.loadKeys()
				}
				if len(it.reader.keys) > 0 && len(it.reader.expiries) == len(it.reader.keys) {
					idx := sort.Search(len(it.reader.keys), func(i int) bool { return bytes.Compare(it.reader.keys[i], k) >= 0 })
					if idx < len(it.reader.keys) && bytes.Equal(it.reader.keys[idx], k) && it.reader.isExpiredKeyIndex(idx) {
						continue
					}
				}
				it.current = iteratorState{node: 0, key: k}
				return true
			}
		}
	}

	// Streaming via buffered file
	if it.streamReader != nil {
		for it.keysRead < it.keysCount {
			var kl uint32
			if err := binary.Read(it.streamReader, binary.LittleEndian, &kl); err != nil {
				// End or error
				break
			}
			var k []byte
			if kl > 0 {
				k = make([]byte, kl)
				if _, err := io.ReadFull(it.streamReader, k); err != nil {
					break
				}
			}
			it.keysRead++
			if (it.pattern == nil || it.pattern.Match(k)) && !it.reader.HasTombstone(k) {
				if len(it.reader.keys) == 0 {
					_ = it.reader.loadKeys()
				}
				if len(it.reader.keys) > 0 && len(it.reader.expiries) == len(it.reader.keys) {
					idx := sort.Search(len(it.reader.keys), func(i int) bool { return bytes.Compare(it.reader.keys[i], k) >= 0 })
					if idx < len(it.reader.keys) && bytes.Equal(it.reader.keys[idx], k) && it.reader.isExpiredKeyIndex(idx) {
						continue
					}
				}
				it.current = iteratorState{node: 0, key: k}
				return true
			}
		}
	}

	// Legacy in-memory fallback if keys were loaded elsewhere
	if len(it.reader.keys) > 0 {
		for it.keysIndex < len(it.reader.keys) {
			k := it.reader.keys[it.keysIndex]
			it.keysIndex++
			if (it.pattern == nil || it.pattern.Match(k)) && !it.reader.HasTombstone(k) {
				if len(it.reader.keys) > 0 && len(it.reader.expiries) == len(it.reader.keys) {
					idx := sort.Search(len(it.reader.keys), func(i int) bool { return bytes.Compare(it.reader.keys[i], k) >= 0 })
					if idx < len(it.reader.keys) && bytes.Equal(it.reader.keys[idx], k) && it.reader.isExpiredKeyIndex(idx) {
						continue
					}
				}
				it.current = iteratorState{node: 0, key: k}
				return true
			}
		}
	}

	return false
}

// Key returns the current key.
func (it *SegmentIterator) Key() []byte { return it.current.key }

// Value returns the current value.
func (it *SegmentIterator) Value() []byte {
	if it.reader == nil || it.reader.louds == nil {
		return nil
	}
	return it.reader.louds.GetValue(it.current.node)
}

// Error returns any error encountered during iteration.
func (it *SegmentIterator) Error() error { return it.err }

// Close closes the iterator.
// Close closes the iterator and any streaming resources.
func (it *SegmentIterator) Close() error {
	it.stack = nil
	if it.streamFile != nil {
		_ = it.streamFile.Close()
		it.streamFile = nil
		it.streamReader = nil
	}
	return nil
}
