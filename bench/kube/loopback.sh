#!/bin/sh
# Same benchmark harness, two library revisions, alternating run order.
# Build bench-control.test and bench-after.test first, then run on an idle
# worker after the two-node comparison. Both revisions use eight CPUs.
set -eu
ROOT=${ROOT:-/bench}
REPS=${REPS:-6}
PROCS=${PROCS:-8}
BENCHTIME=${BENCHTIME:-1s}
pattern='^Benchmark(CarrierMux|SmallMsgsMux|BulkMux|BulkLargeMux|RelayMux|StreamChurnMux|EchoRTTMux)$'
: > "$ROOT/control.txt"
: > "$ROOT/after.txt"
rep=1
while [ "$rep" -le "$REPS" ]; do
	if [ $((rep % 2)) -eq 1 ]; then
		revisions="control after"
	else
		revisions="after control"
	fi
	for rev in $revisions; do
		"$ROOT/bench-$rev.test" -test.run '^$' -test.bench "$pattern" \
			-test.benchtime "$BENCHTIME" -test.cpu "$PROCS" >> "$ROOT/$rev.txt"
	done
	rep=$((rep + 1))
done
