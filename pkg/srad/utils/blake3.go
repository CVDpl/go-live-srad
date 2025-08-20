package utils

import (
	"fmt"
	"io"
	"os"

	blake3 "lukechampine.com/blake3"
)

// ComputeBLAKE3 computes the BLAKE3 hash of the given bytes and returns a hex string.
func ComputeBLAKE3(data []byte) string {
	sum := blake3.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

// ComputeBLAKE3File computes the BLAKE3 hash of a file path and returns a hex string.
func ComputeBLAKE3File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3.New(32, nil)
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
