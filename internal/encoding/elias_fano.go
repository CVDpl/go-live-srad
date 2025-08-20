package encoding

import (
	"encoding/binary"
	"math/bits"
)

// EliasFano implements Elias-Fano encoding for monotone sequences.
type EliasFano struct {
	n         uint64   // number of elements
	u         uint64   // universe size (max value + 1)
	l         uint32   // lower bits count
	lowerBits []uint64 // packed lower bits
	upperBits []uint64 // unary encoded upper bits
}

// NewEliasFano creates a new Elias-Fano encoding from a sorted sequence.
func NewEliasFano(values []uint64) *EliasFano {
	if len(values) == 0 {
		return &EliasFano{}
	}

	n := uint64(len(values))
	u := values[len(values)-1] + 1 // universe size

	// Calculate optimal l (number of lower bits)
	var l uint32
	if n > 0 {
		l = uint32(bits.Len64(u / n))
		if l > 0 {
			l--
		}
	}

	// Calculate sizes
	lowerSize := (n*uint64(l) + 63) / 64
	upperSize := (n + (u >> l) + 63) / 64

	ef := &EliasFano{
		n:         n,
		u:         u,
		l:         l,
		lowerBits: make([]uint64, lowerSize),
		upperBits: make([]uint64, upperSize),
	}

	// Encode values
	upperPos := uint64(0)
	for i, val := range values {
		// Lower bits
		if l > 0 {
			lower := val & ((1 << l) - 1)
			ef.setLower(uint64(i), lower)
		}

		// Upper bits (unary)
		upper := val >> l
		for j := upperPos; j < upper+uint64(i); j++ {
			ef.clearUpperBit(j)
		}
		ef.setUpperBit(upper + uint64(i))
		upperPos = upper + uint64(i) + 1
	}

	return ef
}

// Get returns the i-th value.
func (ef *EliasFano) Get(i uint64) uint64 {
	if i >= ef.n {
		return 0
	}

	// Get lower bits
	lower := uint64(0)
	if ef.l > 0 {
		lower = ef.getLower(i)
	}

	// Find upper bits using select
	upper := ef.select1(i)
	if upper > i {
		upper = upper - i
	} else {
		upper = 0
	}

	return (upper << ef.l) | lower
}

// Size returns the number of elements.
func (ef *EliasFano) Size() uint64 {
	return ef.n
}

// setLower sets the lower bits for position i.
func (ef *EliasFano) setLower(i uint64, val uint64) {
	if ef.l == 0 {
		return
	}

	bitPos := i * uint64(ef.l)
	wordIdx := bitPos / 64
	bitOffset := bitPos % 64

	// Clear and set bits
	mask := (uint64(1) << ef.l) - 1
	ef.lowerBits[wordIdx] &= ^(mask << bitOffset)
	ef.lowerBits[wordIdx] |= (val & mask) << bitOffset

	// Handle overflow to next word
	if bitOffset+uint64(ef.l) > 64 && wordIdx+1 < uint64(len(ef.lowerBits)) {
		overflow := bitOffset + uint64(ef.l) - 64
		ef.lowerBits[wordIdx+1] &= ^(mask >> (uint64(ef.l) - overflow))
		ef.lowerBits[wordIdx+1] |= val >> (uint64(ef.l) - overflow)
	}
}

// getLower gets the lower bits for position i.
func (ef *EliasFano) getLower(i uint64) uint64 {
	if ef.l == 0 {
		return 0
	}

	bitPos := i * uint64(ef.l)
	wordIdx := bitPos / 64
	bitOffset := bitPos % 64

	mask := (uint64(1) << ef.l) - 1
	result := (ef.lowerBits[wordIdx] >> bitOffset) & mask

	// Handle overflow from next word
	if bitOffset+uint64(ef.l) > 64 && wordIdx+1 < uint64(len(ef.lowerBits)) {
		overflow := bitOffset + uint64(ef.l) - 64
		nextBits := ef.lowerBits[wordIdx+1] & ((uint64(1) << overflow) - 1)
		result |= nextBits << (uint64(ef.l) - overflow)
	}

	return result
}

// setUpperBit sets bit at position i in upper bits.
func (ef *EliasFano) setUpperBit(i uint64) {
	wordIdx := i / 64
	bitIdx := i % 64
	if wordIdx < uint64(len(ef.upperBits)) {
		ef.upperBits[wordIdx] |= uint64(1) << bitIdx
	}
}

// clearUpperBit clears bit at position i in upper bits.
func (ef *EliasFano) clearUpperBit(i uint64) {
	wordIdx := i / 64
	bitIdx := i % 64
	if wordIdx < uint64(len(ef.upperBits)) {
		ef.upperBits[wordIdx] &= ^(uint64(1) << bitIdx)
	}
}

// select1 finds the position of the i-th set bit.
func (ef *EliasFano) select1(i uint64) uint64 {
	count := uint64(0)
	for wordIdx, word := range ef.upperBits {
		popcount := uint64(bits.OnesCount64(word))
		if count+popcount > i {
			// The i-th bit is in this word
			for bitIdx := uint64(0); bitIdx < 64; bitIdx++ {
				if word&(1<<bitIdx) != 0 {
					if count == i {
						return uint64(wordIdx)*64 + bitIdx
					}
					count++
				}
			}
		}
		count += popcount
	}
	return 0
}

// Marshal serializes the Elias-Fano structure.
func (ef *EliasFano) Marshal() []byte {
	size := 24 + len(ef.lowerBits)*8 + len(ef.upperBits)*8
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], ef.n)
	binary.LittleEndian.PutUint64(buf[8:16], ef.u)
	binary.LittleEndian.PutUint32(buf[16:20], ef.l)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(len(ef.lowerBits)))

	offset := 24
	for _, v := range ef.lowerBits {
		binary.LittleEndian.PutUint64(buf[offset:], v)
		offset += 8
	}

	for _, v := range ef.upperBits {
		binary.LittleEndian.PutUint64(buf[offset:], v)
		offset += 8
	}

	return buf
}

// Unmarshal deserializes the Elias-Fano structure.
func UnmarshalEliasFano(data []byte) *EliasFano {
	if len(data) < 24 {
		return nil
	}

	ef := &EliasFano{
		n: binary.LittleEndian.Uint64(data[0:8]),
		u: binary.LittleEndian.Uint64(data[8:16]),
		l: binary.LittleEndian.Uint32(data[16:20]),
	}

	lowerSize := binary.LittleEndian.Uint32(data[20:24])
	upperSize := (ef.n + (ef.u >> ef.l) + 63) / 64

	ef.lowerBits = make([]uint64, lowerSize)
	ef.upperBits = make([]uint64, upperSize)

	offset := 24
	for i := range ef.lowerBits {
		if offset+8 <= len(data) {
			ef.lowerBits[i] = binary.LittleEndian.Uint64(data[offset:])
			offset += 8
		}
	}

	for i := range ef.upperBits {
		if offset+8 <= len(data) {
			ef.upperBits[i] = binary.LittleEndian.Uint64(data[offset:])
			offset += 8
		}
	}

	return ef
}
