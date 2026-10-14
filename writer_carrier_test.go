package portunus

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type batchCaptureConn struct {
	net.Conn
	buf    bytes.Buffer
	writes int
	short  bool
	err    error
}

func (c *batchCaptureConn) Write(p []byte) (int, error) {
	c.writes++
	if c.err != nil {
		return 0, c.err
	}
	if c.short {
		p = p[:len(p)/2]
	}
	return c.buf.Write(p)
}

// Bulk wrappers receive complete batches with fewer writes. Interactive
// sessions retain the original chunking until the quiet period passes.
func TestCoalescedBatchWritesLargeChunks(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name    string
		smallAt time.Time
		writes  int
	}{
		{name: "bulk", writes: 4},
		{name: "interactive", smallAt: now, writes: 15},
		{name: "quiet", smallAt: now.Add(-burstQuiet), writes: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &batchCaptureConn{}
			w := newWriter(&NativeSession{conn: c})
			w.smallAt = tc.smallAt
			control := []byte("control")
			payload := bytes.Repeat([]byte("payload"), 32<<10)
			w.fctl = control
			w.fchunks = []chunk{{ref: payload}}
			if err := w.writeOut(now); err != nil {
				t.Fatal(err)
			}
			if c.writes != tc.writes {
				t.Fatalf("carrier got %d writes for a 224KB batch, want %d", c.writes, tc.writes)
			}
			want := append(append([]byte(nil), control...), payload...)
			if !bytes.Equal(c.buf.Bytes(), want) {
				t.Fatal("carrier did not receive the complete batch in order")
			}
		})
	}
}

func TestCoalescedBatchWriteErrors(t *testing.T) {
	carrierErr := errors.New("carrier failed")
	for _, tc := range []struct {
		name  string
		short bool
		err   error
		want  error
	}{
		{name: "short", short: true, want: io.ErrShortWrite},
		{name: "error", err: carrierErr, want: carrierErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &batchCaptureConn{short: tc.short, err: tc.err}
			w := newWriter(&NativeSession{conn: c})
			w.fctl = []byte("complete frame")
			if err := w.writeOut(time.Now()); !errors.Is(err, tc.want) {
				t.Fatalf("writeOut returned %v, want %v", err, tc.want)
			}
		})
	}
}
