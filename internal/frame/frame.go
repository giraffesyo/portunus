// Package frame implements the mux wire format: a fixed 8-byte header and the
// control-frame payload codecs. See DESIGN.md "Wire protocol (clean slate)".
//
// Header layout (big-endian):
//
//	byte 0            bytes 1-4        bytes 5-7
//	+--------------+----------------+-------------+
//	| type | flags |  stream ID u32 | length u24  |
//	+--------------+----------------+-------------+
//	  bits 0-3: type   bits 4-7: flags
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// HeaderSize is the fixed wire header size in bytes.
	HeaderSize = 8

	// MaxLength is the largest payload the 24-bit length field can carry.
	MaxLength = 1<<24 - 1

	// MaxGoAwayReason bounds GOAWAY reason bytes, enforced on send and receive.
	MaxGoAwayReason = 1024

	// MaxSettings bounds the number of entries in one SETTINGS frame.
	MaxSettings = 64

	// Protocol floors: a sender uses these limits until the peer's SETTINGS
	// frame has been processed, so advertised values must never go below
	// them (enforced in config validation and on SETTINGS receipt).
	FloorInitialWindow = 64 << 10
	FloorMaxFrameSize  = 16 << 10

	// ProtocolVersion is the wire protocol version carried in SETTINGS.
	ProtocolVersion = 1
)

// Type identifies a frame. Values 8-15 are unused and a protocol error in v1;
// TypePadding is reserved for v2 and also a protocol error when received.
type Type uint8

const (
	TypeData         Type = 0
	TypeWindowUpdate Type = 1
	TypeRST          Type = 2
	TypeStopSending  Type = 3
	TypePing         Type = 4
	TypeGoAway       Type = 5
	TypeSettings     Type = 6
	TypePadding      Type = 7 // reserved for v2
)

func (t Type) String() string {
	switch t {
	case TypeData:
		return "DATA"
	case TypeWindowUpdate:
		return "WINDOW_UPDATE"
	case TypeRST:
		return "RST"
	case TypeStopSending:
		return "STOP_SENDING"
	case TypePing:
		return "PING"
	case TypeGoAway:
		return "GOAWAY"
	case TypeSettings:
		return "SETTINGS"
	case TypePadding:
		return "PADDING"
	}
	return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
}

// Flags occupy the high nibble of byte 0. FlagSYN opens a stream and rides
// its first frame; FlagFIN half-closes; FlagACK marks PING/SETTINGS replies.
// The fourth bit is reserved (piggybacked credit on DATA, reliable size on
// RST — v2 candidates).
type Flags uint8

const (
	FlagSYN Flags = 1 << iota
	FlagFIN
	FlagACK
	flagReserved // claimed for v2; senders must not set it
)

// Header is a decoded frame header.
type Header struct {
	Type     Type
	Flags    Flags
	StreamID uint32
	Length   uint32 // 24-bit on the wire
}

// Encode writes the 8-byte wire form into b. b must be at least HeaderSize
// bytes; Length must not exceed MaxLength.
func (h Header) Encode(b []byte) {
	if h.Length > MaxLength {
		panic("frame: header length exceeds 24 bits")
	}
	_ = b[7]
	b[0] = byte(h.Type)&0x0f | byte(h.Flags)<<4
	binary.BigEndian.PutUint32(b[1:5], h.StreamID)
	b[5] = byte(h.Length >> 16)
	b[6] = byte(h.Length >> 8)
	b[7] = byte(h.Length)
}

// ParseHeader decodes an 8-byte wire header. b must be at least HeaderSize
// bytes. All bit patterns decode; semantic validation is the session's job.
func ParseHeader(b []byte) Header {
	_ = b[7]
	return Header{
		Type:     Type(b[0] & 0x0f),
		Flags:    Flags(b[0] >> 4),
		StreamID: binary.BigEndian.Uint32(b[1:5]),
		Length:   uint32(b[5])<<16 | uint32(b[6])<<8 | uint32(b[7]),
	}
}

// Fixed control-frame payload sizes.
const (
	WindowUpdateLen = 8
	RSTLen          = 16
	StopSendingLen  = 8
	PingLen         = 8
	GoAwayMinLen    = 12
	settingLen      = 10
)

var (
	ErrPayloadSize = errors.New("frame: bad control payload size")
	ErrReasonSize  = errors.New("frame: GOAWAY reason too long")
	ErrTooMany     = errors.New("frame: too many SETTINGS entries")
)

// AppendWindowUpdate appends the payload for a WINDOW_UPDATE carrying an
// absolute cumulative credit limit.
func AppendWindowUpdate(b []byte, limit uint64) []byte {
	return binary.BigEndian.AppendUint64(b, limit)
}

func ParseWindowUpdate(p []byte) (limit uint64, err error) {
	if len(p) != WindowUpdateLen {
		return 0, ErrPayloadSize
	}
	return binary.BigEndian.Uint64(p), nil
}

// AppendRST appends the payload for an RST: application error code and the
// final size (cumulative bytes sent on the stream before the reset).
func AppendRST(b []byte, code, finalSize uint64) []byte {
	b = binary.BigEndian.AppendUint64(b, code)
	return binary.BigEndian.AppendUint64(b, finalSize)
}

func ParseRST(p []byte) (code, finalSize uint64, err error) {
	if len(p) != RSTLen {
		return 0, 0, ErrPayloadSize
	}
	return binary.BigEndian.Uint64(p), binary.BigEndian.Uint64(p[8:]), nil
}

func AppendStopSending(b []byte, code uint64) []byte {
	return binary.BigEndian.AppendUint64(b, code)
}

func ParseStopSending(p []byte) (code uint64, err error) {
	if len(p) != StopSendingLen {
		return 0, ErrPayloadSize
	}
	return binary.BigEndian.Uint64(p), nil
}

func AppendPing(b []byte, opaque uint64) []byte {
	return binary.BigEndian.AppendUint64(b, opaque)
}

func ParsePing(p []byte) (opaque uint64, err error) {
	if len(p) != PingLen {
		return 0, ErrPayloadSize
	}
	return binary.BigEndian.Uint64(p), nil
}

// AppendGoAway appends the payload for a GOAWAY: last processed remote stream
// ID, session error code, and optional reason bytes (capped).
func AppendGoAway(b []byte, lastID uint32, code uint64, reason []byte) ([]byte, error) {
	if len(reason) > MaxGoAwayReason {
		return b, ErrReasonSize
	}
	b = binary.BigEndian.AppendUint32(b, lastID)
	b = binary.BigEndian.AppendUint64(b, code)
	return append(b, reason...), nil
}

func ParseGoAway(p []byte) (lastID uint32, code uint64, reason []byte, err error) {
	if len(p) < GoAwayMinLen || len(p) > GoAwayMinLen+MaxGoAwayReason {
		return 0, 0, nil, ErrPayloadSize
	}
	return binary.BigEndian.Uint32(p), binary.BigEndian.Uint64(p[4:]), p[GoAwayMinLen:], nil
}

// Setting IDs. Unknown IDs are ignored by receivers (forward compatibility);
// feature bits gate any behavior change.
const (
	SettingVersion              uint16 = 1
	SettingInitialWindow        uint16 = 2
	SettingMaxFrameSize         uint16 = 3
	SettingMaxConcurrentStreams uint16 = 4
	SettingFeatureBits          uint16 = 5
	SettingPingMinInterval      uint16 = 6 // milliseconds
)

// Setting is one SETTINGS entry.
type Setting struct {
	ID    uint16
	Value uint64
}

// AppendSettings appends the payload for a SETTINGS frame.
func AppendSettings(b []byte, settings []Setting) ([]byte, error) {
	if len(settings) > MaxSettings {
		return b, ErrTooMany
	}
	for _, s := range settings {
		b = binary.BigEndian.AppendUint16(b, s.ID)
		b = binary.BigEndian.AppendUint64(b, s.Value)
	}
	return b, nil
}

// ParseSettings decodes a SETTINGS payload. Duplicate IDs are allowed on the
// wire; the last occurrence wins at the session layer.
func ParseSettings(p []byte) ([]Setting, error) {
	if len(p)%settingLen != 0 || len(p) > MaxSettings*settingLen {
		return nil, ErrPayloadSize
	}
	out := make([]Setting, 0, len(p)/settingLen)
	for len(p) > 0 {
		out = append(out, Setting{
			ID:    binary.BigEndian.Uint16(p),
			Value: binary.BigEndian.Uint64(p[2:]),
		})
		p = p[settingLen:]
	}
	return out, nil
}
