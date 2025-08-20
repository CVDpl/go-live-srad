package encoding

import (
	"encoding/binary"
	"math/bits"
)

// BitVector is a compact bit array with rank/select support.
type BitVector struct {
	bits   []uint64
	length uint64
}

// NewBitVector creates a new bit vector with the given length.
func NewBitVector(length uint64) *BitVector {
	numWords := (length + 63) / 64
	return &BitVector{
		bits:   make([]uint64, numWords),
		length: length,
	}
}

// Set sets the bit at position i to 1.
func (bv *BitVector) Set(i uint64) {
	if i >= bv.length {
		return
	}
	wordIdx := i / 64
	bitIdx := i % 64
	bv.bits[wordIdx] |= uint64(1) << bitIdx
}

// Clear sets the bit at position i to 0.
func (bv *BitVector) Clear(i uint64) {
	if i >= bv.length {
		return
	}
	wordIdx := i / 64
	bitIdx := i % 64
	bv.bits[wordIdx] &= ^(uint64(1) << bitIdx)
}

// Get returns the bit at position i.
func (bv *BitVector) Get(i uint64) bool {
	if i >= bv.length {
		return false
	}
	wordIdx := i / 64
	bitIdx := i % 64
	return (bv.bits[wordIdx] & (uint64(1) << bitIdx)) != 0
}

// Rank1 returns the number of 1-bits up to position i (exclusive).
func (bv *BitVector) Rank1(i uint64) uint64 {
	if i == 0 {
		return 0
	}
	if i > bv.length {
		i = bv.length
	}

	count := uint64(0)
	fullWords := i / 64

	// Count full words
	for j := uint64(0); j < fullWords; j++ {
		count += uint64(bits.OnesCount64(bv.bits[j]))
	}

	// Count remaining bits in partial word
	remainder := i % 64
	if remainder > 0 && fullWords < uint64(len(bv.bits)) {
		mask := (uint64(1) << remainder) - 1
		count += uint64(bits.OnesCount64(bv.bits[fullWords] & mask))
	}

	return count
}

// Rank0 returns the number of 0-bits up to position i (exclusive).
func (bv *BitVector) Rank0(i uint64) uint64 {
	if i > bv.length {
		i = bv.length
	}
	return i - bv.Rank1(i)
}

// Select1 returns the position of the n-th 1-bit (0-indexed).
func (bv *BitVector) Select1(n uint64) uint64 {
	if n == 0 {
		return bv.length // Not found
	}

	count := uint64(0)
	for wordIdx, word := range bv.bits {
		wordCount := uint64(bits.OnesCount64(word))
		if count+wordCount >= n {
			// The n-th bit is in this word
			for bitIdx := uint64(0); bitIdx < 64; bitIdx++ {
				if (word & (1 << bitIdx)) != 0 {
					count++
					if count == n {
						return uint64(wordIdx)*64 + bitIdx
					}
				}
			}
		}
		count += wordCount
	}

	return bv.length // Not found
}

// Select0 returns the position of the n-th 0-bit (0-indexed).
func (bv *BitVector) Select0(n uint64) uint64 {
	if n == 0 {
		return bv.length // Not found
	}

	count := uint64(0)
	for wordIdx, word := range bv.bits {
		wordCount := uint64(bits.OnesCount64(^word))
		if wordIdx == len(bv.bits)-1 {
			// Last word may be partial
			lastBits := bv.length % 64
			if lastBits > 0 {
				mask := (uint64(1) << lastBits) - 1
				wordCount = uint64(bits.OnesCount64((^word) & mask))
			}
		}

		if count+wordCount >= n {
			// The n-th bit is in this word
			for bitIdx := uint64(0); bitIdx < 64; bitIdx++ {
				pos := uint64(wordIdx)*64 + bitIdx
				if pos >= bv.length {
					break
				}
				if (word & (1 << bitIdx)) == 0 {
					count++
					if count == n {
						return pos
					}
				}
			}
		}
		count += wordCount
	}

	return bv.length // Not found
}

// Length returns the length of the bit vector.
func (bv *BitVector) Length() uint64 {
	return bv.length
}

// PopCount returns the total number of 1-bits.
func (bv *BitVector) PopCount() uint64 {
	count := uint64(0)
	for _, word := range bv.bits {
		count += uint64(bits.OnesCount64(word))
	}
	return count
}

// Marshal serializes the bit vector.
func (bv *BitVector) Marshal() []byte {
	size := 8 + len(bv.bits)*8
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], bv.length)

	offset := 8
	for _, word := range bv.bits {
		binary.LittleEndian.PutUint64(buf[offset:], word)
		offset += 8
	}

	return buf
}

// UnmarshalBitVector deserializes a bit vector.
func UnmarshalBitVector(data []byte) *BitVector {
	if len(data) < 8 {
		return nil
	}

	length := binary.LittleEndian.Uint64(data[0:8])
	numWords := (length + 63) / 64

	bv := &BitVector{
		bits:   make([]uint64, numWords),
		length: length,
	}

	offset := 8
	for i := range bv.bits {
		if offset+8 <= len(data) {
			bv.bits[i] = binary.LittleEndian.Uint64(data[offset:])
			offset += 8
		}
	}

	return bv
}

// RankSelect provides accelerated rank/select operations using auxiliary structures.
type RankSelect struct {
	bv          *BitVector
	rankSuper   []uint64 // Superblock ranks (every 512 bits)
	rankBlock   []uint16 // Block ranks (every 64 bits)
	select1Hint []uint64 // Hints for select1 (sampled positions)
	select0Hint []uint64 // Hints for select0 (sampled positions)
}

// NewRankSelect creates a new RankSelect structure for the given bit vector.
func NewRankSelect(bv *BitVector) *RankSelect {
	rs := &RankSelect{
		bv: bv,
	}
	rs.buildRankStructures()
	rs.buildSelectHints()
	return rs
}

// buildRankStructures builds the rank acceleration structures.
func (rs *RankSelect) buildRankStructures() {
	superBlockSize := uint64(512)
	blockSize := uint64(64)

	numSuperBlocks := (rs.bv.length + superBlockSize - 1) / superBlockSize
	numBlocks := (rs.bv.length + blockSize - 1) / blockSize

	rs.rankSuper = make([]uint64, numSuperBlocks)
	rs.rankBlock = make([]uint16, numBlocks)

	rank := uint64(0)
	for i := uint64(0); i < numBlocks; i++ {
		if i%8 == 0 {
			// New superblock
			rs.rankSuper[i/8] = rank
		}

		// Store relative rank within superblock
		superRank := rs.rankSuper[i/8]
		rs.rankBlock[i] = uint16(rank - superRank)

		// Count bits in this block
		if i < uint64(len(rs.bv.bits)) {
			rank += uint64(bits.OnesCount64(rs.bv.bits[i]))
		}
	}
}

// buildSelectHints builds select acceleration hints.
func (rs *RankSelect) buildSelectHints() {
	sampleRate := uint64(4096) // Sample every 4096 1-bits

	ones := rs.bv.PopCount()
	numSamples1 := (ones + sampleRate - 1) / sampleRate
	rs.select1Hint = make([]uint64, numSamples1)

	zeros := rs.bv.length - ones
	numSamples0 := (zeros + sampleRate - 1) / sampleRate
	rs.select0Hint = make([]uint64, numSamples0)

	// Build select1 hints
	count1 := uint64(0)
	sample1 := uint64(0)
	for i := uint64(0); i < rs.bv.length && sample1 < numSamples1; i++ {
		if rs.bv.Get(i) {
			count1++
			if count1%sampleRate == 1 {
				rs.select1Hint[sample1] = i
				sample1++
			}
		}
	}

	// Build select0 hints
	count0 := uint64(0)
	sample0 := uint64(0)
	for i := uint64(0); i < rs.bv.length && sample0 < numSamples0; i++ {
		if !rs.bv.Get(i) {
			count0++
			if count0%sampleRate == 1 {
				rs.select0Hint[sample0] = i
				sample0++
			}
		}
	}
}

// Rank1 returns the number of 1-bits up to position i (exclusive).
func (rs *RankSelect) Rank1(i uint64) uint64 {
	if i == 0 {
		return 0
	}
	if i > rs.bv.length {
		i = rs.bv.length
	}

	blockIdx := i / 64
	superBlockIdx := blockIdx / 8

	// Start with superblock rank
	rank := uint64(0)
	if superBlockIdx < uint64(len(rs.rankSuper)) {
		rank = rs.rankSuper[superBlockIdx]
	}

	// Add block rank
	if blockIdx < uint64(len(rs.rankBlock)) {
		rank += uint64(rs.rankBlock[blockIdx])
	}

	// Add remaining bits in the block
	remainder := i % 64
	if remainder > 0 && blockIdx < uint64(len(rs.bv.bits)) {
		mask := (uint64(1) << remainder) - 1
		rank += uint64(bits.OnesCount64(rs.bv.bits[blockIdx] & mask))
	}

	return rank
}

// Rank0 returns the number of 0-bits up to position i (exclusive).
func (rs *RankSelect) Rank0(i uint64) uint64 {
	if i > rs.bv.length {
		i = rs.bv.length
	}
	return i - rs.Rank1(i)
}

// Select1 returns the position of the n-th 1-bit (0-indexed).
func (rs *RankSelect) Select1(n uint64) uint64 {
	// Use hint to find starting position
	sampleRate := uint64(4096)
	hintIdx := (n - 1) / sampleRate

	startPos := uint64(0)
	startRank := uint64(0)

	if hintIdx < uint64(len(rs.select1Hint)) {
		startPos = rs.select1Hint[hintIdx]
		startRank = hintIdx * sampleRate
	}

	// Linear search from hint position
	for i := startPos; i < rs.bv.length; i++ {
		if rs.bv.Get(i) {
			startRank++
			if startRank == n {
				return i
			}
		}
	}

	return rs.bv.length // Not found
}

// Select0 returns the position of the n-th 0-bit (0-indexed).
func (rs *RankSelect) Select0(n uint64) uint64 {
	// Use hint to find starting position
	sampleRate := uint64(4096)
	hintIdx := (n - 1) / sampleRate

	startPos := uint64(0)
	startRank := uint64(0)

	if hintIdx < uint64(len(rs.select0Hint)) {
		startPos = rs.select0Hint[hintIdx]
		startRank = hintIdx * sampleRate
	}

	// Linear search from hint position
	for i := startPos; i < rs.bv.length; i++ {
		if !rs.bv.Get(i) {
			startRank++
			if startRank == n {
				return i
			}
		}
	}

	return rs.bv.length // Not found
}
