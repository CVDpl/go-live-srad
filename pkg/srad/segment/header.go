package segment

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/CVDpl/go-live-srad/internal/common"
	"github.com/CVDpl/go-live-srad/pkg/srad/utils"
)

// CommonHeader is the common header for all segment files.
type CommonHeader struct {
	Magic   uint32
	Version uint16
}

// LoudsHeader is the header for LOUDS index files.
type LoudsHeader struct {
	CommonHeader
	TotalNodes          uint64
	BitsLen             uint64
	RankSuperStrideBits uint32
	RankBlockStrideBits uint32
	Select1Sample       uint32
	Select0Sample       uint32
	OffBV               uint64
	OffRank             uint64
	OffSelect1          uint64
	OffSelect0          uint64
	FileCRC32C          uint64
}

// EdgesHeader is the header for edge index files.
type EdgesHeader struct {
	CommonHeader
	TotalEdges          uint64
	LabelsBytes         uint64
	CutsCount           uint64
	TargetsCount        uint64
	NodesCount          uint64
	OffLabelsBlob       uint64
	OffLabelsCutsEF     uint64
	OffTargets          uint64
	OffFirstEdgeEF      uint64
	OffEdgeCnt          uint64
	OffTargetsOffsetsEF uint64
	FileCRC32C          uint64
}

// AcceptHeader is the header for accept state files.
type AcceptHeader struct {
	CommonHeader
	NodesCount uint64
	Encoding   uint32 // RRR = 1
	OffBits    uint64
	OffCounts  uint64
	FileCRC32C uint64
}

// TMapHeader is the header for tail mapping files.
type TMapHeader struct {
	CommonHeader
	TailNodesCount uint64
	OffTailNodesEF uint64
	OffTailIDs     uint64
	FileCRC32C     uint64
}

// TailsHeader is the header for tails data files.
type TailsHeader struct {
	CommonHeader
	BlockSize      uint32
	TailsCount     uint64
	BlocksCount    uint64
	OffBlocksIndex uint64
	OffMap         uint64
	OffFrames      uint64
	FileCRC32C     uint64
}

// BloomHeader is the header for bloom filter files.
type BloomHeader struct {
	CommonHeader
	Bits       uint64
	K          uint32
	Hash       uint32 // BLAKE3 = 1
	OffBitset  uint64
	FileCRC32C uint64
}

// TrigramHeader is the header for trigram filter files.
type TrigramHeader struct {
	CommonHeader
	Scheme     uint32
	Bits       uint64
	OffBitset  uint64
	FileCRC32C uint64
}

// WriteCommonHeader writes a common header to a writer.
func WriteCommonHeader(w io.Writer, magic uint32, version uint16) error {
	h := CommonHeader{
		Magic:   magic,
		Version: version,
	}

	if err := binary.Write(w, binary.LittleEndian, h.Magic); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Version); err != nil {
		return err
	}

	// No padding: 6-byte header (magic + version)
	return nil
}

// ReadCommonHeader reads a common header from a reader.
func ReadCommonHeader(r io.Reader) (*CommonHeader, error) {
	var h CommonHeader

	if err := binary.Read(r, binary.LittleEndian, &h.Magic); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Version); err != nil {
		return nil, err
	}

	// No padding to consume
	return &h, nil
}

// ValidateHeader validates a common header.
func ValidateHeader(h *CommonHeader, expectedMagic uint32, expectedVersion uint16) error {
	if h.Magic != expectedMagic {
		return fmt.Errorf("%w: got 0x%08x, expected 0x%08x",
			common.ErrInvalidMagic, h.Magic, expectedMagic)
	}

	if h.Version != expectedVersion {
		return fmt.Errorf("%w: got 0x%04x, expected 0x%04x",
			common.ErrUnsupportedVersion, h.Version, expectedVersion)
	}

	return nil
}

// WriteLoudsHeader writes a LOUDS header.
func WriteLoudsHeader(w io.Writer, h *LoudsHeader) error {
	// Write common header
	if err := WriteCommonHeader(w, common.MagicLouds, common.VersionSegment); err != nil {
		return err
	}

	// Write LOUDS-specific fields
	if err := binary.Write(w, binary.LittleEndian, h.TotalNodes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.BitsLen); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.RankSuperStrideBits); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.RankBlockStrideBits); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Select1Sample); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Select0Sample); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.OffBV); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.OffRank); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.OffSelect1); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.OffSelect0); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.FileCRC32C); err != nil {
		return err
	}

	return nil
}

// ReadLoudsHeader reads a LOUDS header.
func ReadLoudsHeader(r io.Reader) (*LoudsHeader, error) {
	commonHdr, err := ReadCommonHeader(r)
	if err != nil {
		return nil, err
	}

	if err := ValidateHeader(commonHdr, common.MagicLouds, common.VersionSegment); err != nil {
		return nil, err
	}

	h := &LoudsHeader{CommonHeader: *commonHdr}

	if err := binary.Read(r, binary.LittleEndian, &h.TotalNodes); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.BitsLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.RankSuperStrideBits); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.RankBlockStrideBits); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Select1Sample); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Select0Sample); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.OffBV); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.OffRank); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.OffSelect1); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.OffSelect0); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.FileCRC32C); err != nil {
		return nil, err
	}

	return h, nil
}

// ComputeFileCRC computes CRC for a file with the CRC field zeroed.
func ComputeFileCRC(data []byte, crcOffset int) uint64 {
	return utils.ComputeFileCRC32C(data, crcOffset)
}

// VerifyFileCRC verifies the CRC of a file.
func VerifyFileCRC(data []byte, crcOffset int, expected uint64) bool {
	actual := ComputeFileCRC(data, crcOffset)
	return actual == expected
}
