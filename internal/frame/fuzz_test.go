package frame

import (
	"bytes"
	"testing"
)

// FuzzParseHeader asserts every 8-byte pattern decodes and re-encodes to the
// identical bytes when the reserved type nibble round-trips (it always does:
// all 4-bit types and 4-bit flags are representable).
func FuzzParseHeader(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0x21, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x16, 0, 0, 0, 1, 0, 0, 8})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < HeaderSize {
			return
		}
		h := ParseHeader(b)
		var out [HeaderSize]byte
		h.Encode(out[:])
		if !bytes.Equal(b[:HeaderSize], out[:]) {
			t.Fatalf("header not canonical: in %x out %x (%+v)", b[:HeaderSize], out, h)
		}
	})
}

// FuzzParseSettings asserts decode never panics and that accepted payloads
// re-encode to the identical bytes.
func FuzzParseSettings(f *testing.F) {
	seed, _ := AppendSettings(nil, []Setting{{SettingVersion, 1}, {SettingInitialWindow, 1 << 20}})
	f.Add(seed)
	f.Add([]byte{})
	f.Add(make([]byte, 10))
	f.Fuzz(func(t *testing.T, p []byte) {
		s, err := ParseSettings(p)
		if err != nil {
			return
		}
		out, err := AppendSettings(nil, s)
		if err != nil || !bytes.Equal(out, p) {
			t.Fatalf("settings not canonical: %x -> %x (%v)", p, out, err)
		}
	})
}

// FuzzParseGoAway asserts decode never panics and accepted payloads
// round-trip.
func FuzzParseGoAway(f *testing.F) {
	seed, _ := AppendGoAway(nil, 7, 2, []byte("bye"))
	f.Add(seed)
	f.Add(make([]byte, GoAwayMinLen))
	f.Fuzz(func(t *testing.T, p []byte) {
		last, code, reason, err := ParseGoAway(p)
		if err != nil {
			return
		}
		out, err := AppendGoAway(nil, last, code, reason)
		if err != nil || !bytes.Equal(out, p) {
			t.Fatalf("goaway not canonical: %x -> %x (%v)", p, out, err)
		}
	})
}
