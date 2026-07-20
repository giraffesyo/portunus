//go:build !linux && !darwin

package portunus

import "net"

// applyNotSentLowat is a no-op where TCP_NOTSENT_LOWAT does not exist.
// Sessions still work correctly; their tail latency under bulk load just
// includes whatever the kernel chooses to buffer.
func applyNotSentLowat(net.Conn, int) error { return nil }
