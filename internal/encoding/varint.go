package encoding

import (
	"encoding/binary"
	"io"
)

// WriteVarint writes a variable-length encoded integer.
func WriteVarint(w io.Writer, v int64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	_, err := w.Write(buf[:n])
	return err
}

// WriteUvarint writes a variable-length encoded unsigned integer.
func WriteUvarint(w io.Writer, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	_, err := w.Write(buf[:n])
	return err
}

// ReadVarint reads a variable-length encoded integer.
func ReadVarint(r io.ByteReader) (int64, error) {
	return binary.ReadVarint(r)
}

// ReadUvarint reads a variable-length encoded unsigned integer.
func ReadUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// SizeVarint returns the number of bytes required to encode v.
func SizeVarint(v int64) int {
	buf := make([]byte, binary.MaxVarintLen64)
	return binary.PutVarint(buf, v)
}

// SizeUvarint returns the number of bytes required to encode v.
func SizeUvarint(v uint64) int {
	buf := make([]byte, binary.MaxVarintLen64)
	return binary.PutUvarint(buf, v)
}

// AppendVarint appends a variable-length encoded integer to dst.
func AppendVarint(dst []byte, v int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	return append(dst, buf[:n]...)
}

// AppendUvarint appends a variable-length encoded unsigned integer to dst.
func AppendUvarint(dst []byte, v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return append(dst, buf[:n]...)
}
