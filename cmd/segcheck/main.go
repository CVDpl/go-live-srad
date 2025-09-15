package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/pkg/srad/segment"
	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
)

func checkFile(path string, expectedMagic uint32) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < 6 {
		return fmt.Errorf("%s: size < 6", path)
	}
	buf := make([]byte, 6)
	if _, err := f.Read(buf); err != nil {
		return err
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != expectedMagic {
		return fmt.Errorf("%s: bad magic 0x%x", path, magic)
	}
	version := binary.LittleEndian.Uint16(buf[4:6])
	if version != common.VersionSegment {
		return fmt.Errorf("%s: bad version 0x%x", path, version)
	}
	return nil
}

func main() {
	dir := flag.String("dir", "", "segment directory (e.g., /path/segments/123)")
	flag.Parse()
	if *dir == "" {
		fmt.Println("-dir is required")
		os.Exit(2)
	}

	// LOUDS
	if err := checkFile(filepath.Join(*dir, "index.louds"), common.MagicLouds); err != nil {
		fmt.Println("LOUDS:", err)
		os.Exit(1)
	} else {
		fmt.Println("LOUDS: OK")
	}
	// Bloom (optional)
	_ = checkFile(filepath.Join(*dir, "filters", "prefix.bf"), common.MagicBloom)
	// Trigram (optional)
	if err := checkFile(filepath.Join(*dir, "filters", "tri.bits"), common.MagicTrigram); err == nil {
		fmt.Println("Trigram: OK")
	}
	// Keys (optional)
	if err := checkFile(filepath.Join(*dir, "keys.dat"), common.MagicKeys); err == nil {
		fmt.Println("KEYS: OK")
	}
	// Tombstones (optional, may be absent after full deletion compaction)
	if err := checkFile(filepath.Join(*dir, "tombstones.dat"), common.MagicTombs); err == nil {
		fmt.Println("TOMBSTONES: OK")
	}

	// BLAKE3 verification from segment.json if present
	metaPath := filepath.Join(*dir, "segment.json")
	if data, err := os.ReadFile(metaPath); err == nil {
		var m segment.Metadata
		if err := json.Unmarshal(data, &m); err == nil && len(m.Blake3) > 0 {
			ok := true
			for rel, want := range m.Blake3 {
				p := filepath.Join(*dir, rel)
				if got, err := utils.ComputeBLAKE3File(p); err != nil || got != want {
					fmt.Printf("BLAKE3 mismatch %s: want=%s got=%s err=%v\n", rel, want, got, err)
					ok = false
				}
			}
			if ok {
				fmt.Println("BLAKE3: OK")
			}
		}
	}
}
