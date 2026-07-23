//go:build !linux

package portunus

import "net"

// applyCongestionControl is a no-op off Linux. Per-socket selection of the
// congestion-control algorithm by name is a Linux interface (TCP_CONGESTION
// with the allowed-list check); other platforms keep their system default.
func applyCongestionControl(net.Conn, string) error { return nil }
