//go:build linux

package mux

// tcpNotSentLowat is TCP_NOTSENT_LOWAT. The frozen syscall package does not
// define it on Linux, so it is declared here rather than taking a dependency
// on golang.org/x/sys.
const tcpNotSentLowat = 25
