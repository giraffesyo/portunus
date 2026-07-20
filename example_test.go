package mux_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/giraffesyo/mux"
)

// Multiplexing a request and response over one carrier.
func Example() {
	carrier, peer := net.Pipe()

	// The server side, echoing whatever each stream sends.
	go func() {
		sess, err := mux.Server(peer, nil)
		if err != nil {
			log.Print(err)
			return
		}
		defer sess.Close()
		for {
			st, err := sess.AcceptStream(context.Background())
			if err != nil {
				return
			}
			go func(st mux.Stream) {
				io.Copy(st, st)
				st.CloseWrite()
			}(st)
		}
	}()

	sess, err := mux.Client(carrier, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer sess.Close()

	st, err := sess.OpenStream(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if _, err := io.WriteString(st, "hello"); err != nil {
		log.Fatal(err)
	}
	// Half-close: the peer sees EOF and can reply, while we keep reading.
	if err := st.CloseWrite(); err != nil {
		log.Fatal(err)
	}
	reply, err := io.ReadAll(st)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", reply)
	// Output: hello
}

// A stream is a net.Conn, so a session can stand in for a net.Listener and
// serve anything built for one.
func ExampleSession_asListener() {
	carrier, peer := net.Pipe()

	go func() {
		sess, err := mux.Client(carrier, nil)
		if err != nil {
			return
		}
		defer sess.Close()
		st, err := sess.OpenStream(context.Background())
		if err != nil {
			return
		}
		io.WriteString(st, "request")
		st.CloseWrite()
		io.Copy(io.Discard, st)
	}()

	sess, err := mux.Server(peer, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer sess.Close()

	st, err := sess.AcceptStream(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	var conn net.Conn = st // usable anywhere a net.Conn is
	body, err := io.ReadAll(conn)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", body)
	conn.Close()
	// Output: request
}

// Cancelling a stream with an application error code, which the peer sees.
func ExampleStream_CancelWrite() {
	carrier, peer := net.Pipe()

	const codeQuotaExceeded = mux.CodeApp + 42

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sess, err := mux.Server(peer, nil)
		if err != nil {
			return
		}
		defer sess.Close()
		st, err := sess.AcceptStream(context.Background())
		if err != nil {
			return
		}
		_, err = io.ReadAll(st)

		var se *mux.StreamError
		if errors.As(err, &se) {
			fmt.Printf("peer aborted with code %d (remote=%v)\n", se.Code-mux.CodeApp, se.Remote)
		}
	}()

	sess, err := mux.Client(carrier, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer sess.Close()

	st, err := sess.OpenStream(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	io.WriteString(st, "partial data")
	st.CancelWrite(codeQuotaExceeded)

	wg.Wait()
	// Output: peer aborted with code 42 (remote=true)
}

// Draining a session gracefully: refuse new streams, let live ones finish.
func ExampleSession_Shutdown() {
	carrier, peer := net.Pipe()

	go func() {
		sess, err := mux.Server(peer, nil)
		if err != nil {
			return
		}
		defer sess.Close()
		for {
			st, err := sess.AcceptStream(context.Background())
			if err != nil {
				return
			}
			go func(st mux.Stream) { io.Copy(st, st); st.Close() }(st)
		}
	}()

	sess, err := mux.Client(carrier, nil)
	if err != nil {
		log.Fatal(err)
	}

	st, err := sess.OpenStream(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	io.WriteString(st, "in flight")
	st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sess.Shutdown(ctx); err != nil {
		log.Print(err)
	}

	// New streams are refused once draining has started.
	_, err = sess.OpenStream(context.Background())
	fmt.Println(err != nil)
	// Output: true
}
