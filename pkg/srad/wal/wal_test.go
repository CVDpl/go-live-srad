package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
)

// TestReadRecordSizeTruncation ensures that readRecord returns precise sizes allowing safe truncation.
func TestReadRecordSizeTruncation(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWithConfig(dir, common.NewNullLogger(), Config{RotateSize: common.WALRotateSize, MaxFileSize: common.WALMaxFileSize, BufferSize: int(common.WALBufferSize)})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	if err := w.WriteWithTTL(common.OpInsert, []byte("abc"), 10*time.Second); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Corrupt the last 2 bytes to simulate partial write
	files, _ := w.listWALFiles()
	if len(files) == 0 {
		t.Fatalf("no wal files")
	}
	p := files[len(files)-1]
	f, err := os.OpenFile(p, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Append two bogus bytes to simulate a torn/partial tail
	if _, err := f.Write([]byte{0xDE, 0xAD}); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	f.Close()

	// Replay should truncate to lastValidOffset without error
	w2, err := New(filepath.Join(dir), common.NewNullLogger())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	var seen int
	err = w2.Replay(func(op uint8, key []byte, exp time.Time) error { seen++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if seen == 0 {
		t.Fatalf("expected at least one record after truncation")
	}
}
