//go:build darwin

package portunus

import "syscall"

// tcpNotSentLowat is TCP_NOTSENT_LOWAT, which differs from Linux's value
// (0x201 rather than 25) and is defined by the standard syscall package on
// Darwin. Sharing one constant across platforms would silently set an
// unrelated socket option on one of them.
const tcpNotSentLowat = syscall.TCP_NOTSENT_LOWAT
