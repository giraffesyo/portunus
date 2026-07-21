#!/bin/sh
# Drive the two-machine benchmark matrix and print one JSON result per line.
#
# Run the server on the other host first:
#
#	server$ ./remote -mode server -listen :7777 -procs 8
#	client$ ./run.sh 10.0.0.1:7777 > results.jsonl
#	client$ ./remote -mode aggregate < results.jsonl
#
# Repetitions are the outer loop and implementations the inner one, so the
# whole matrix is walked REPS times rather than each configuration being
# repeated in place. On a shared host that distinction decides the result: run
# implementation-major and each one occupies its own window of time, so any
# drift in how busy the machine is becomes an apparent difference between
# libraries. Interleaving spreads that drift evenly and the median then has
# something to average out.
set -eu

ADDR="${1:?usage: run.sh host:port [reps]}"
REPS="${2:-3}"
BIN="${BIN:-./remote}"
PROCS="${PROCS:-8}"

# PROFILE picks transfer sizes and round-trip counts to suit the path. The
# same numbers cannot serve both: a gigabyte takes under a second on a
# datacenter link and ten minutes on a WAN uplink, while a round-trip count
# that finishes instantly on a LAN takes a quarter of an hour at 55ms.
#
# Both profiles keep the same shape, so their tables can be read side by side.
PROFILE="${PROFILE:-lan}"
case "$PROFILE" in
lan)
	SHORT=$((128 * 1024 * 1024))
	LONG=$((1024 * 1024 * 1024))
	PARALLEL_EACH=$((128 * 1024 * 1024))
	MANY_EACH=$((16 * 1024 * 1024))
	RR_SMALL_COUNT=5000
	RR_LARGE_COUNT=2000
	OPEN_COUNT=5000
	;;
wan)
	# Sized so the slowest configuration (yamux at its default window, held
	# to window/RTT) still finishes in seconds rather than minutes.
	SHORT=$((4 * 1024 * 1024))
	LONG=$((64 * 1024 * 1024))
	PARALLEL_EACH=$((8 * 1024 * 1024))
	MANY_EACH=$((1024 * 1024))
	RR_SMALL_COUNT=100
	RR_LARGE_COUNT=100
	OPEN_COUNT=100
	;;
*)
	echo "unknown PROFILE $PROFILE" >&2
	exit 2
	;;
esac

# yamux cannot autotune, so it appears twice: once as it ships, and once with
# a window sized for this path by hand. The hand-tuned figure is the honest
# comparison — beating only the out-of-the-box default would say more about
# yamux's defaults than about this library.
YAMUX_TUNED=$((8 * 1024 * 1024))

run() {
	label="$1"
	shift
	# A failed run must not abort the matrix: one refused configuration is
	# worth less than the other twenty results.
	"$BIN" -mode client -addr "$ADDR" -procs "$PROCS" -label "$label" "$@" \
		|| echo "{\"label\":\"$label\",\"error\":true}"
	# Let the peer's sockets drain out of TIME_WAIT and the CPU settle, so
	# one run's tail does not become the next run's noise.
	sleep 2
}

# bulk_at runs one bulk configuration against every implementation, including
# yamux twice.
bulk_at() {
	label="$1"
	streams="$2"
	bytes="$3"
	for impl in portunus yamux raw; do
		run "$label" -impl "$impl" -scenario bulk -streams "$streams" -bytes "$bytes"
	done
	run "$label" -impl yamux -yamux-window "$YAMUX_TUNED" \
		-scenario bulk -streams "$streams" -bytes "$bytes"
}

matrix() {
	# Two single-stream sizes on purpose. A window that autotunes has to be
	# grown, and growing it costs round trips, so a short transfer reports
	# the ramp and a long one reports the steady state. Quoting only the long
	# one would hide a real cost; quoting only the short one would hide the
	# reason for paying it.
	bulk_at "bulk-1-short" 1 "$SHORT"
	bulk_at "bulk-1" 1 "$LONG"
	bulk_at "bulk-8" 8 "$PARALLEL_EACH"
	bulk_at "bulk-64" 64 "$MANY_EACH"

	for impl in portunus yamux raw; do
		run "rr-64" -impl "$impl" -scenario rr -size 64 -count "$RR_SMALL_COUNT"
		run "rr-16k" -impl "$impl" -scenario rr -size 16384 -count "$RR_LARGE_COUNT"
	done

	# Scenarios that need multiplexing, so bare TCP has nothing to say about
	# them.
	for impl in portunus yamux; do
		run "rrload-4" -impl "$impl" -scenario rrload -streams 4 \
			-count "$RR_SMALL_COUNT" -bytes "$LONG"
		run "open" -impl "$impl" -scenario open -count "$OPEN_COUNT"
	done
	run "rrload-4" -impl yamux -yamux-window "$YAMUX_TUNED" \
		-scenario rrload -streams 4 -count "$RR_SMALL_COUNT" -bytes "$LONG"
}

rep=1
while [ "$rep" -le "$REPS" ]; do
	matrix
	rep=$((rep + 1))
done
