package portunus

import (
	"net"
	"sync"
)

// Batched wraps a connection so that a session layered over it through TLS
// can put a whole batch on the wire in one write.
//
// crypto/tls issues one write on the underlying connection per record, and a
// record holds at most 16KB, so over TLS a 64KB frame costs five syscalls
// where plain TCP costs one — and a batch of many frames, which is one
// writev on TCP, costs one per record. The session cannot fix that from
// above the tls.Conn, because the records are cut and written below it. It
// can from underneath: wrap the transport before handing it to TLS,
//
//	conn := tls.Client(portunus.Batched(tcp), tlsConfig)
//	sess, err := portunus.Client(conn, nil)
//
// and the session, which finds the wrapper through tls.Conn's NetConn, holds
// the records of each batch back and releases them together. Outside a
// batch the wrapper passes writes straight through, and the session finishes
// the handshake before its first batch, so the handshake is unaffected.
//
// The cost is one more copy of the ciphertext and a buffer of up to
// batchedFlush bytes per session. A connection that is not wrapped behaves
// exactly as before. TLS 1.2 renegotiation (off unless a client enables it)
// must stay off: a renegotiation that starts mid-batch would have its
// opening flight held behind the batch that is waiting on it.
//
// EXPERIMENTAL: not yet part of the stable API.
func Batched(conn net.Conn) net.Conn {
	return &batchedConn{Conn: conn}
}

// batchedFlush bounds how much a held batch may buffer before it is written
// out early. It caps the wrapper's memory at a fraction of the largest batch
// while still cutting the write count by an order of magnitude.
const batchedFlush = 256 << 10

type batchedConn struct {
	net.Conn

	mu      sync.Mutex
	holding bool
	buf     []byte
	err     error // from a write-out whose bytes were already acknowledged
}

// Write passes through unless a batch is being held, in which case the bytes
// are buffered and reported written. An error writing them out later is
// returned by release and by every Write after it.
func (c *batchedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return 0, c.err
	}
	if !c.holding {
		return c.Conn.Write(p)
	}
	c.buf = append(c.buf, p...)
	if len(c.buf) >= batchedFlush {
		if err := c.flushLocked(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// hold starts buffering writes; release writes them out and returns to
// passing writes through.
//
// Nothing may be held that the peer must answer before release is reached,
// or the answer is waited for with the question still in the buffer. A TLS
// handshake is exactly that — it runs inside the first Write and alternates
// writing with reading — which is why the writer completes the handshake
// before it ever holds. Reads are deliberately left alone rather than made
// to flush: the session reader sits in Read for the life of the connection,
// and coupling it to this lock would park it behind a blocked write, the
// two-sided gridlock the reader is built never to enter.
func (c *batchedConn) hold() {
	c.mu.Lock()
	c.holding = true
	c.mu.Unlock()
}

func (c *batchedConn) release() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.holding = false
	if err := c.flushLocked(); err != nil {
		return err
	}
	return c.err
}

func (c *batchedConn) flushLocked() error {
	if len(c.buf) == 0 {
		return nil
	}
	_, err := c.Conn.Write(c.buf)
	c.buf = c.buf[:0]
	if err != nil && c.err == nil {
		c.err = err
	}
	return err
}

// netConner and handshaker are implemented by tls.Conn. Naming the methods
// rather than the type keeps crypto/tls out of the import graph of programs
// that do not use it.
type netConner interface{ NetConn() net.Conn }
type handshaker interface{ Handshake() error }

// batchedUnder finds a Batched wrapper that is the carrier or sits directly
// beneath it.
func batchedUnder(conn net.Conn) *batchedConn {
	if nc, ok := conn.(netConner); ok {
		conn = nc.NetConn()
	}
	b, _ := conn.(*batchedConn)
	return b
}

// tcpUnder finds the TCP connection that is the carrier or sits beneath it,
// through TLS and Batched, so socket options reach the socket whatever is
// layered on top. It returns nil if there is none to be found.
func tcpUnder(conn net.Conn) *net.TCPConn {
	if nc, ok := conn.(netConner); ok {
		conn = nc.NetConn()
	}
	if b, ok := conn.(*batchedConn); ok {
		conn = b.Conn
	}
	tcp, _ := conn.(*net.TCPConn)
	return tcp
}
