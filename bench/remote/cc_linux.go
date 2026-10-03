package main

import (
	"net"
	"syscall"
)

// tcpCongestion is TCP_CONGESTION, absent from the frozen syscall package.
const tcpCongestion = 13

// setCongestion selects the socket's congestion-control algorithm. Unlike the
// library's best-effort Config.CongestionControl it fails loudly: a benchmark
// that silently ran under the wrong controller would report the wrong thing.
func setCongestion(c net.Conn, algo string) error {
	if algo == "" {
		return nil
	}
	tcp, ok := c.(*net.TCPConn)
	if !ok {
		return nil
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptString(int(fd), syscall.IPPROTO_TCP, tcpCongestion, algo)
	}); err != nil {
		return err
	}
	return setErr
}
