package filters

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic data structure for membership testing.
type BloomFilter struct {
	bits     []uint64
	numBits  uint64
	numHash  uint32
	hashFunc hash.Hash64
}

// NewBloomFilter creates a new Bloom filter with the given parameters.
func NewBloomFilter(numElements uint64, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal number of bits
	// m = -n * ln(p) / (ln(2)^2)
	m := uint64(math.Ceil(-float64(numElements) * math.Log(falsePositiveRate) / math.Pow(math.Ln2, 2)))

	// Round up to nearest multiple of 64
	m = ((m + 63) / 64) * 64

	// Calculate optimal number of hash functions
	// k = (m/n) * ln(2)
	k := uint32(math.Ceil(float64(m) / float64(numElements) * math.Ln2))

	// Limit k to reasonable range
	if k < 1 {
		k = 1
	}
	if k > 10 {
		k = 10
	}

	return &BloomFilter{
		bits:     make([]uint64, m/64),
		numBits:  m,
		numHash:  k,
		hashFunc: fnv.New64a(),
	}
}

// Add adds an element to the Bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := bf.hash(data)

	for i := uint32(0); i < bf.numHash; i++ {
		// Double hashing: h(i) = h1 + i*h2
		pos := (h1 + uint64(i)*h2) % bf.numBits
		bf.setBit(pos)
	}
}

// Contains checks if an element might be in the set.
func (bf *BloomFilter) Contains(data []byte) bool {
	h1, h2 := bf.hash(data)

	for i := uint32(0); i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		if !bf.getBit(pos) {
			return false
		}
	}

	return true
}

// AddPrefix adds a prefix to the filter.
func (bf *BloomFilter) AddPrefix(prefix []byte, maxLen int) {
	// Add the prefix itself
	bf.Add(prefix)

	// Add all possible prefixes up to maxLen
	for i := 1; i <= len(prefix) && i <= maxLen; i++ {
		bf.Add(prefix[:i])
	}
}

// ContainsPrefix checks if any prefix might be in the set.
func (bf *BloomFilter) ContainsPrefix(prefix []byte) bool {
	// Check all prefixes
	for i := 1; i <= len(prefix); i++ {
		if bf.Contains(prefix[:i]) {
			return true
		}
	}
	return false
}

// hash computes two independent hash values using double hashing.
func (bf *BloomFilter) hash(data []byte) (uint64, uint64) {
	bf.hashFunc.Reset()
	bf.hashFunc.Write(data)
	h1 := bf.hashFunc.Sum64()

	// Second hash using a different seed
	bf.hashFunc.Reset()
	bf.hashFunc.Write([]byte{0x42}) // seed
	bf.hashFunc.Write(data)
	h2 := bf.hashFunc.Sum64()

	return h1, h2
}

// setBit sets the bit at position pos.
func (bf *BloomFilter) setBit(pos uint64) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	bf.bits[wordIdx] |= uint64(1) << bitIdx
}

// getBit returns the bit at position pos.
func (bf *BloomFilter) getBit(pos uint64) bool {
	wordIdx := pos / 64
	bitIdx := pos % 64
	return (bf.bits[wordIdx] & (uint64(1) << bitIdx)) != 0
}

// EstimateFalsePositiveRate estimates the current false positive rate.
func (bf *BloomFilter) EstimateFalsePositiveRate() float64 {
	// Count set bits
	setBits := uint64(0)
	for _, word := range bf.bits {
		setBits += uint64(popcount(word))
	}

	// Estimate FPR: (1 - e^(-k*n/m))^k
	// Where n is estimated from set bits
	fillRatio := float64(setBits) / float64(bf.numBits)
	return math.Pow(fillRatio, float64(bf.numHash))
}

// Reset clears the filter.
func (bf *BloomFilter) Reset() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

// SizeInBytes returns the size of the filter in bytes.
func (bf *BloomFilter) SizeInBytes() int {
	return len(bf.bits) * 8
}

// Marshal serializes the Bloom filter.
func (bf *BloomFilter) Marshal() []byte {
	size := 16 + len(bf.bits)*8
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint32(buf[8:12], bf.numHash)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(bf.bits)))

	offset := 16
	for _, word := range bf.bits {
		binary.LittleEndian.PutUint64(buf[offset:], word)
		offset += 8
	}

	return buf
}

// Unmarshal deserializes a Bloom filter.
func UnmarshalBloomFilter(data []byte) *BloomFilter {
	if len(data) < 16 {
		return nil
	}

	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHash := binary.LittleEndian.Uint32(data[8:12])
	numWords := binary.LittleEndian.Uint32(data[12:16])

	bits := make([]uint64, numWords)
	offset := 16
	for i := range bits {
		if offset+8 <= len(data) {
			bits[i] = binary.LittleEndian.Uint64(data[offset:])
			offset += 8
		}
	}

	return &BloomFilter{
		bits:     bits,
		numBits:  numBits,
		numHash:  numHash,
		hashFunc: fnv.New64a(),
	}
}

// Merge merges another Bloom filter into this one.
func (bf *BloomFilter) Merge(other *BloomFilter) error {
	if bf.numBits != other.numBits || bf.numHash != other.numHash {
		return fmt.Errorf("incompatible Bloom filters")
	}

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}

	return nil
}

// popcount counts the number of set bits.
func popcount(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}
