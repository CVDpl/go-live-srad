package segment

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"time"

	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
)

// Metadata represents segment metadata stored in segment.json
type Metadata struct {
	Format        string            `json:"format"`
	Version       string            `json:"version"`
	SegmentID     uint64            `json:"segmentID"`
	Level         int               `json:"level"`
	MinKeyHex     string            `json:"minKeyHex"`
	MaxKeyHex     string            `json:"maxKeyHex"`
	Counts        Counts            `json:"counts"`
	Encodings     Encodings         `json:"encodings"`
	Filters       Filters           `json:"filters"`
	Files         Files             `json:"files"`
	CreatedAtUnix int64             `json:"createdAtUnix"`
	Parents       []uint64          `json:"parents,omitempty"`
	Blake3        map[string]string `json:"blake3,omitempty"`
}

// Counts contains element counts in the segment.
type Counts struct {
	Nodes    uint64 `json:"nodes"`
	Edges    uint64 `json:"edges"`
	Tails    uint64 `json:"tails"`
	Accepted uint64 `json:"accepted"`
}

// Encodings describes encoding methods used.
type Encodings struct {
	Labels     string `json:"labels"`
	Targets    string `json:"targets"`
	AcceptBits string `json:"acceptBits"`
	IDBits     int    `json:"idBits,omitempty"`
}

// Filters describes filter configurations.
type Filters struct {
	PrefixBloom *BloomFilter   `json:"prefixBloom,omitempty"`
	Trigram     *TrigramFilter `json:"trigram,omitempty"`
}

// BloomFilter configuration.
type BloomFilter struct {
	Bits uint64  `json:"bits"`
	K    uint32  `json:"k"`
	FPR  float64 `json:"fpr"`
}

// TrigramFilter configuration.
type TrigramFilter struct {
	Bits   uint64 `json:"bits"`
	Scheme string `json:"scheme"`
}

// Files lists all segment files.
type Files struct {
	Louds  string `json:"louds"`
	Edges  string `json:"edges"`
	Accept string `json:"accept"`
	TMap   string `json:"tmap,omitempty"`
	Tails  string `json:"tails,omitempty"`
}

// NewMetadata creates a new segment metadata.
func NewMetadata(segmentID uint64, level int) *Metadata {
	return &Metadata{
		Format:        "srad-segment",
		Version:       "1.0.0",
		SegmentID:     segmentID,
		Level:         level,
		CreatedAtUnix: time.Now().Unix(),
		Counts:        Counts{},
		Encodings: Encodings{
			Labels:     "raw",
			Targets:    "elias-fano",
			AcceptBits: "rrr",
		},
		Files: Files{
			Louds:  "index.louds",
			Edges:  "index.edges",
			Accept: "index.accept",
			TMap:   "index.tmap",
			Tails:  "tails.dat",
		},
	}
}

// SetKeyRange sets the min and max keys.
func (m *Metadata) SetKeyRange(minKey, maxKey []byte) {
	m.MinKeyHex = hex.EncodeToString(minKey)
	m.MaxKeyHex = hex.EncodeToString(maxKey)
}

// GetMinKey returns the minimum key as bytes.
func (m *Metadata) GetMinKey() ([]byte, error) {
	return hex.DecodeString(m.MinKeyHex)
}

// GetMaxKey returns the maximum key as bytes.
func (m *Metadata) GetMaxKey() ([]byte, error) {
	return hex.DecodeString(m.MaxKeyHex)
}

// SaveToFile saves metadata to a JSON file.
func (m *Metadata) SaveToFile(path string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	af, err := utils.NewAtomicFile(path)
	if err != nil {
		return err
	}
	defer af.Close()
	if _, err := af.Write(data); err != nil {
		return err
	}
	return af.Commit()
}

// LoadFromFile loads metadata from a JSON file.
func LoadFromFile(path string) (*Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var m Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	return &m, nil
}
