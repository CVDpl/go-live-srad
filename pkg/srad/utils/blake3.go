package utils

import (
	"fmt"
	"io"
	"os"
	"sync"

	blake3 "github.com/zeebo/blake3"
)

var blake3Pool = sync.Pool{New: func() any { return blake3.New() }}

// ComputeBLAKE3 computes the BLAKE3 hash of the given bytes and returns a hex string.
func ComputeBLAKE3(data []byte) string {
	h := blake3Pool.Get()
	if h == nil {
		h = blake3.New()
	}
	hasher := h.(*blake3.Hasher)
	hasher.Reset()
	_, _ = hasher.Write(data)
	sum := hasher.Sum(nil)
	blake3Pool.Put(hasher)
	return fmt.Sprintf("%x", sum)
}

// ComputeBLAKE3File computes the BLAKE3 hash of a file path and returns a hex string.
func ComputeBLAKE3File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3Pool.Get()
	if h == nil {
		h = blake3.New()
	}
	hasher := h.(*blake3.Hasher)
	hasher.Reset()
	if _, err := io.Copy(hasher, f); err != nil {
		blake3Pool.Put(hasher)
		return "", err
	}
	sum := hasher.Sum(nil)
	blake3Pool.Put(hasher)
	return fmt.Sprintf("%x", sum), nil
}
