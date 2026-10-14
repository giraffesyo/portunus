#!/bin/sh
# Run on the client worker after building both revisions with the same
# bench/remote driver. Start control on port 7778 and after on port 7779 on
# the other worker. Results include delivery confirmation, not just writes.
#
#   SERVER=10.0.0.1 ROOT=/bench REPS=6 sh compare.sh > remote.jsonl
set -eu

: "${SERVER:?set SERVER to the server worker IP}"
ROOT=${ROOT:-/bench}
REPS=${REPS:-6}
PROCS=${PROCS:-8}

run() {
	rev=$1
	carrier=$2
	label=$3
	shift 3
	case "$rev" in
	control) port=7778 ;;
	after) port=7779 ;;
	esac
	"$ROOT/remote-$rev" -addr "$SERVER:$port" -procs "$PROCS" \
		-carrier "$carrier" -label "$rev/$label" "$@"
}

rep=1
while [ "$rep" -le "$REPS" ]; do
	# Reverse the order on alternate passes to share drift and warmup.
	if [ $((rep % 2)) -eq 1 ]; then
		revisions="control after"
	else
		revisions="after control"
	fi
	for carrier in tcp wrapped tls tls-batched; do
		for scenario in bulk-1 bulk-8 rr-64 rrload-4 open; do
			for rev in $revisions; do
				case "$scenario" in
				bulk-1) run "$rev" "$carrier" "$scenario" -scenario bulk -streams 1 -bytes 1073741824 ;;
				bulk-8) run "$rev" "$carrier" "$scenario" -scenario bulk -streams 8 -bytes 134217728 ;;
				rr-64) run "$rev" "$carrier" "$scenario" -scenario rr -size 64 -count 2000 ;;
				rrload-4) run "$rev" "$carrier" "$scenario" -scenario rrload -streams 4 -count 2000 -bytes 68719476736 ;;
				open) run "$rev" "$carrier" "$scenario" -scenario open -count 2000 ;;
				esac
				sleep 0.1
			done
		done
	done
	rep=$((rep + 1))
done
