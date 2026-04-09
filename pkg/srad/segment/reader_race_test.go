package segment

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/CVDpl/go-live-srad/internal/common"
)

// writeKeysFile creates a minimal keys.dat with the given keys.
func writeKeysFile(t *testing.T, path string, keys [][]byte) {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteCommonHeader(&buf, common.MagicKeys, common.VersionSegment); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(keys))); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(k))); err != nil {
			t.Fatal(err)
		}
		buf.Write(k)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// setupMinimalSegment creates the minimum files needed for a Reader.
func setupMinimalSegment(t *testing.T, dir string, segID uint64, keys [][]byte) string {
	t.Helper()
	segDir := filepath.Join(dir, "segments", segDirName(segID))
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatal(err)
	}

	meta := NewMetadata(segID, 0)
	metaJSON, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(segDir, "segment.json"), metaJSON, 0o644); err != nil {
		t.Fatal(err)
	}

	// Empty LOUDS file (Reader tolerates missing/empty LOUDS data)
	loudsPath := filepath.Join(segDir, meta.Files.Louds)
	if err := os.WriteFile(loudsPath, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	writeKeysFile(t, filepath.Join(segDir, "keys.dat"), keys)
	return filepath.Join(dir, "segments")
}

func segDirName(id uint64) string {
	return fmt.Sprintf("%016d", id)
}

// TestAllKeysConcurrentWithCache verifies that calling AllKeys() concurrently
// on the same Reader (with MmapCache) does not cause data races or panics.
// Before the fix, this would SIGSEGV due to cache refcount underflow.
func TestAllKeysConcurrentWithCache(t *testing.T) {
	dir := t.TempDir()
	testKeys := [][]byte{
		[]byte("alpha"), []byte("bravo"), []byte("charlie"),
		[]byte("delta"), []byte("echo"), []byte("foxtrot"),
	}
	segID := uint64(1)

	baseDir := setupMinimalSegment(t, dir, segID, testKeys)
	cache := NewMmapCacheWithTimeout(4, 0, common.NewNullLogger())
	defer cache.Close()

	reader, err := NewReaderWithCache(segID, baseDir, common.NewNullLogger(), false, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			keys := reader.AllKeys()
			if len(keys) != len(testKeys) {
				t.Errorf("expected %d keys, got %d", len(testKeys), len(keys))
			}
		}()
	}
	wg.Wait()

	// Verify cache refcount is balanced (no leaks)
	hits, _, _, _, currentSize := cache.Stats()
	if currentSize < 0 {
		t.Errorf("cache size went negative: %d", currentSize)
	}
	t.Logf("cache stats: hits=%d, currentSize=%d", hits, currentSize)
}

// TestAllKeysConcurrentRepeated exercises AllKeys in a tighter loop to increase
// the window for a race between acquire and release.
func TestAllKeysConcurrentRepeated(t *testing.T) {
	dir := t.TempDir()
	keys := make([][]byte, 100)
	for i := range keys {
		keys[i] = []byte{byte(i / 26), byte('a' + i%26)}
	}
	segID := uint64(42)

	baseDir := setupMinimalSegment(t, dir, segID, keys)
	cache := NewMmapCacheWithTimeout(2, 0, common.NewNullLogger())
	defer cache.Close()

	reader, err := NewReaderWithCache(segID, baseDir, common.NewNullLogger(), false, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	const goroutines = 16
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				got := reader.AllKeys()
				if len(got) != len(keys) {
					t.Errorf("expected %d keys, got %d", len(keys), len(got))
					return
				}
			}
		}()
	}
	wg.Wait()
}
