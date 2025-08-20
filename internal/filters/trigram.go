package filters

// TrigramFilter is a simple 24-bit (16,777,216 bits) bitset filter over all possible
// byte trigrams. It is memory heavy (~2MB) but fast and simple for substring pruning.
type TrigramFilter struct {
	bits []byte // length = 1<<21 (16,777,216 / 8)
}

const trigramBitsetSize = 1 << 21 // 2,097,152 bytes

// NewTrigramFilter creates an empty trigram filter bitset.
func NewTrigramFilter() *TrigramFilter {
	return &TrigramFilter{bits: make([]byte, trigramBitsetSize)}
}

// AddString adds all trigrams from s to the filter.
func (t *TrigramFilter) AddString(s []byte) {
	if len(s) < 3 {
		return
	}
	for i := 0; i+2 < len(s); i++ {
		tri := int(s[i])<<16 | int(s[i+1])<<8 | int(s[i+2])
		t.set(tri)
	}
}

// MayContainLiteral returns true if all trigrams of the literal are present.
// Returns true for literals shorter than 3.
func (t *TrigramFilter) MayContainLiteral(lit []byte) bool {
	if t == nil || len(t.bits) == 0 {
		return true
	}
	if len(lit) < 3 {
		return true
	}
	for i := 0; i+2 < len(lit); i++ {
		tri := int(lit[i])<<16 | int(lit[i+1])<<8 | int(lit[i+2])
		if !t.get(tri) {
			return false
		}
	}
	return true
}

func (t *TrigramFilter) set(tri int) {
	byteIdx := tri >> 3
	bit := uint8(1 << (tri & 7))
	t.bits[byteIdx] |= bit
}

func (t *TrigramFilter) get(tri int) bool {
	byteIdx := tri >> 3
	bit := uint8(1 << (tri & 7))
	return (t.bits[byteIdx] & bit) != 0
}

// Marshal returns the raw bitset bytes.
func (t *TrigramFilter) Marshal() []byte {
	out := make([]byte, len(t.bits))
	copy(out, t.bits)
	return out
}

// UnmarshalTrigram creates a filter from raw bytes; returns nil if size invalid.
func UnmarshalTrigram(data []byte) *TrigramFilter {
	if len(data) != trigramBitsetSize {
		return nil
	}
	out := make([]byte, trigramBitsetSize)
	copy(out, data)
	return &TrigramFilter{bits: out}
}
