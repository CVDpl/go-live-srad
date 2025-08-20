package utils

import (
	"encoding/binary"
	"hash/crc32"
)

// CRC32C uses the Castagnoli polynomial for better error detection.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ComputeCRC32C computes CRC32C checksum for the given data.
func ComputeCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// ComputeCRC32CMulti computes CRC32C checksum for multiple data slices.
func ComputeCRC32CMulti(data ...[]byte) uint32 {
	h := crc32.New(crcTable)
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum32()
}

// VerifyCRC32C verifies that the given CRC matches the data.
func VerifyCRC32C(data []byte, expected uint32) bool {
	return ComputeCRC32C(data) == expected
}

// ComputeFileCRC32C computes CRC32C for a file with the CRC field zeroed.
// The CRC field is assumed to be at the specified offset and 8 bytes long.
func ComputeFileCRC32C(data []byte, crcOffset int) uint64 {
	if crcOffset < 0 || crcOffset+8 > len(data) {
		return 0
	}

	// Create a copy to avoid modifying the original
	temp := make([]byte, len(data))
	copy(temp, data)

	// Zero out the CRC field
	for i := 0; i < 8; i++ {
		temp[crcOffset+i] = 0
	}

	// Compute CRC32C
	crc32Val := ComputeCRC32C(temp)

	// Return as 64-bit value (low 32 bits are CRC, high 32 bits are zero)
	return uint64(crc32Val)
}

// WriteCRC32C writes a CRC32C value to a byte slice at the specified offset.
func WriteCRC32C(data []byte, offset int, crc uint32) {
	if offset < 0 || offset+4 > len(data) {
		return
	}
	binary.LittleEndian.PutUint32(data[offset:], crc)
}

// ReadCRC32C reads a CRC32C value from a byte slice at the specified offset.
func ReadCRC32C(data []byte, offset int) uint32 {
	if offset < 0 || offset+4 > len(data) {
		return 0
	}
	return binary.LittleEndian.Uint32(data[offset:])
}

// WriteCRC64 writes a 64-bit CRC value (for file CRCs).
func WriteCRC64(data []byte, offset int, crc uint64) {
	if offset < 0 || offset+8 > len(data) {
		return
	}
	binary.LittleEndian.PutUint64(data[offset:], crc)
}

// ReadCRC64 reads a 64-bit CRC value.
func ReadCRC64(data []byte, offset int) uint64 {
	if offset < 0 || offset+8 > len(data) {
		return 0
	}
	return binary.LittleEndian.Uint64(data[offset:])
}
