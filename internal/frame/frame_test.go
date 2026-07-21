package frame

import (
	"bytes"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	cases := []Header{
		{},
		{Type: TypeData, Flags: FlagSYN | FlagFIN, StreamID: 1, Length: 5},
		{Type: TypeSettings, Flags: FlagACK, StreamID: 0, Length: 0},
		{Type: TypeGoAway, StreamID: 0, Length: MaxLength},
		{Type: TypePadding, Flags: 0x0f, StreamID: 1<<32 - 1, Length: 1<<24 - 1},
		{Type: TypeWindowUpdate, Flags: FlagSYN, StreamID: 2, Length: 8},
	}
	for _, h := range cases {
		var b [HeaderSize]byte
		h.Encode(b[:])
		got := ParseHeader(b[:])
		if got != h {
			t.Errorf("round trip: got %+v want %+v", got, h)
		}
	}
}

func TestHeaderEncodePanicsOnOversize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for Length > MaxLength")
		}
	}()
	var b [HeaderSize]byte
	Header{Length: MaxLength + 1}.Encode(b[:])
}

func TestControlPayloadRoundTrips(t *testing.T) {
	if p := AppendWindowUpdate(nil, 1<<40); len(p) != WindowUpdateLen {
		t.Fatalf("window update len %d", len(p))
	} else if v, err := ParseWindowUpdate(p); err != nil || v != 1<<40 {
		t.Fatalf("window update: %v %v", v, err)
	}
	if _, err := ParseWindowUpdate(make([]byte, 7)); err == nil {
		t.Fatal("short window update accepted")
	}

	p := AppendRST(nil, 42, 9999)
	if code, final, err := ParseRST(p); err != nil || code != 42 || final != 9999 {
		t.Fatalf("rst: %v %v %v", code, final, err)
	}
	if _, _, err := ParseRST(make([]byte, 15)); err == nil {
		t.Fatal("short rst accepted")
	}

	p = AppendStopSending(nil, 7)
	if code, err := ParseStopSending(p); err != nil || code != 7 {
		t.Fatalf("stop sending: %v %v", code, err)
	}

	p = AppendPing(nil, 0xdeadbeef)
	if v, err := ParsePing(p); err != nil || v != 0xdeadbeef {
		t.Fatalf("ping: %v %v", v, err)
	}
}

func TestGoAwayRoundTrip(t *testing.T) {
	reason := []byte("shutting down: deploy")
	p, err := AppendGoAway(nil, 41, 3, reason)
	if err != nil {
		t.Fatal(err)
	}
	last, code, r, err := ParseGoAway(p)
	if err != nil || last != 41 || code != 3 || !bytes.Equal(r, reason) {
		t.Fatalf("goaway: %v %v %q %v", last, code, r, err)
	}

	if _, err := AppendGoAway(nil, 0, 0, make([]byte, MaxGoAwayReason+1)); err == nil {
		t.Fatal("oversized reason accepted on send")
	}
	if _, _, _, err := ParseGoAway(make([]byte, GoAwayMinLen+MaxGoAwayReason+1)); err == nil {
		t.Fatal("oversized reason accepted on receive")
	}
	if _, _, _, err := ParseGoAway(make([]byte, GoAwayMinLen-1)); err == nil {
		t.Fatal("short goaway accepted")
	}
	// Empty reason is valid.
	if _, _, r, err := ParseGoAway(make([]byte, GoAwayMinLen)); err != nil || len(r) != 0 {
		t.Fatalf("empty reason: %q %v", r, err)
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	in := []Setting{
		{SettingVersion, ProtocolVersion},
		{SettingInitialWindow, 256 << 10},
		{SettingMaxFrameSize, 64 << 10},
		{SettingMaxConcurrentStreams, 1024},
	}
	p, err := AppendSettings(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := ParseSettings(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d settings", len(out))
	}
	for i := range in {
		if out[i] != in[i] {
			t.Errorf("setting %d: got %+v want %+v", i, out[i], in[i])
		}
	}

	if _, err := ParseSettings(make([]byte, 11)); err == nil {
		t.Fatal("misaligned settings accepted")
	}
	if _, err := ParseSettings(make([]byte, (MaxSettings+1)*10)); err == nil {
		t.Fatal("oversized settings accepted")
	}
	if _, err := AppendSettings(nil, make([]Setting, MaxSettings+1)); err == nil {
		t.Fatal("too many settings accepted on send")
	}
	if out, err := ParseSettings(nil); err != nil || len(out) != 0 {
		t.Fatalf("empty settings: %v %v", out, err)
	}
}

// Frame types are named in protocol errors and diagnostics, so the mapping is
// worth pinning: a wrong or missing name turns a clear error into a puzzle.
func TestTypeString(t *testing.T) {
	for _, tc := range []struct {
		typ  Type
		want string
	}{
		{TypeData, "DATA"},
		{TypeWindowUpdate, "WINDOW_UPDATE"},
		{TypeRST, "RST"},
		{TypeStopSending, "STOP_SENDING"},
		{TypePing, "PING"},
		{TypeGoAway, "GOAWAY"},
		{TypeSettings, "SETTINGS"},
		{TypePadding, "PADDING"},
	} {
		if got := tc.typ.String(); got != tc.want {
			t.Errorf("Type(%d).String() = %q, want %q", tc.typ, got, tc.want)
		}
	}
	// Values 8-15 fit the 4-bit type field but carry no meaning in v1; they
	// must still render legibly rather than as an empty string.
	if got := Type(9).String(); got != "UNKNOWN(9)" {
		t.Errorf("Type(9).String() = %q, want %q", got, "UNKNOWN(9)")
	}
}

// Short control payloads must be rejected rather than read past their end.
// These are the sizes a hostile or buggy peer produces most easily.
func TestShortControlPayloadsRejected(t *testing.T) {
	for _, n := range []int{0, 1, 7} {
		if _, err := ParseStopSending(make([]byte, n)); err == nil {
			t.Errorf("ParseStopSending accepted %d bytes", n)
		}
		if _, err := ParsePing(make([]byte, n)); err == nil {
			t.Errorf("ParsePing accepted %d bytes", n)
		}
	}
	// Over-long is equally wrong: these payloads are fixed width, so extra
	// bytes mean the sender and receiver disagree about the frame.
	if _, err := ParseStopSending(make([]byte, StopSendingLen+1)); err == nil {
		t.Error("ParseStopSending accepted an over-long payload")
	}
	if _, err := ParsePing(make([]byte, PingLen+1)); err == nil {
		t.Error("ParsePing accepted an over-long payload")
	}
}
