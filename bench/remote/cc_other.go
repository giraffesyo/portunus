//go:build !linux

package main

import (
	"errors"
	"net"
)

func setCongestion(_ net.Conn, algo string) error {
	if algo == "" {
		return nil
	}
	return errors.New("selecting congestion control is Linux-only")
}
