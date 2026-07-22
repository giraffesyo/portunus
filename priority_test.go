package portunus

import (
	"bytes"
	"io"
	"testing"
)

// A priority stream must still deliver every byte, in order. Prioritization
// reorders frames of different streams against each other, never a stream's
// own frames against themselves, so a corrupted or reordered single-stream
// payload would be a routing bug in the new lane.
func TestPriorityStreamDeliversInOrder(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	cs, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	cs.(*NativeStream).SetPriority(true)

	// Many small writes, each a distinct value, so a swapped or dropped
	// frame shows up as an out-of-order or missing byte rather than needing
	// a checksum to notice.
	const n = 4096
	want := make([]byte, n)
	go func() {
		for i := range want {
			want[i] = byte(i*7 + i/251)
			if _, err := cs.Write(want[i : i+1]); err != nil {
				return
			}
		}
		cs.CloseWrite()
	}()

	ss, err := server.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(ss)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("priority stream delivered %d bytes, want %d; equal=%v",
			len(got), n, bytes.Equal(got, want))
	}
}

// A priority frame co-batched with bulk must be written ahead of it, since
// that ordering is the entire point of the lane. Rather than race a real flush,
// the flusher is pinned so both frames stage into one batch deterministically,
// and the batch's lanes are inspected directly: the priority payload must land
// in the priority lane and the bulk payload in the bulk staging area. writeOut
// concatenates the lanes control, then priority, then bulk, so lane placement
// is wire order.
func TestPriorityFrameRoutesToPriorityLane(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	// Two announced streams: a bulk one and a priority one. Announcing them
	// first means their later data frames carry no SYN and so are free to
	// take the data lanes rather than the control lane SYN rides.
	bulk, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	prio, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []Stream{bulk, prio} {
		if _, err := st.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		ss, err := server.AcceptStream(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(ss, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	}
	prio.(*NativeStream).SetPriority(true)

	w := client.w

	// Pin the flusher so appends stage without draining, guaranteeing both
	// frames share one batch. The announce writes above returned, which for a
	// lone writer means their inline flush already completed and the flusher
	// is idle, so this claim is uncontended.
	w.mu.Lock()
	if w.flushing {
		w.mu.Unlock()
		t.Fatal("flusher unexpectedly busy; announce flush should have drained")
	}
	w.flushing = true
	w.mu.Unlock()

	// Bulk first, then the priority frame: append order that, without the
	// lane, would put bulk on the wire first. copyOnly keeps the caller from
	// parking on a zero-copy reference while the flush is pinned.
	bulkPayload := bytes.Repeat([]byte{0xBB}, 8<<10)
	if err := w.appendData(bulk.(*NativeStream), bulkPayload, 0, true, nil); err != nil {
		t.Fatal(err)
	}
	prioPayload := bytes.Repeat([]byte{0xAA}, 64)
	if err := w.appendData(prio.(*NativeStream), prioPayload, 0, true, nil); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	prioLane := append([]byte(nil), w.prio...)
	stageLane := append([]byte(nil), w.stage...)
	// Release the pin and drain, so the streams and session shut down
	// cleanly rather than leaking a pinned flusher.
	w.flushing = false
	w.mu.Unlock()

	// The priority payload must be in the priority lane and nowhere else; the
	// bulk payload must be in the staging area and not in the priority lane.
	if !bytes.Contains(prioLane, prioPayload) {
		t.Error("priority frame did not land in the priority lane")
	}
	if bytes.Contains(prioLane, bulkPayload) {
		t.Error("bulk frame leaked into the priority lane")
	}
	if !bytes.Contains(stageLane, bulkPayload) {
		t.Error("bulk frame did not land in the staging area")
	}
	if bytes.Contains(stageLane, prioPayload) {
		t.Error("priority frame leaked into the bulk staging area")
	}
}
