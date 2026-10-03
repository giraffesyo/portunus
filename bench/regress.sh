#!/bin/sh
# regress.sh compares the loopback benchmarks of two checkouts on the same
# machine and fails if a gated benchmark got significantly slower.
#
#   bench/regress.sh <base-dir> <head-dir>
#
# Shared CI runners are noisy, so absolute numbers mean nothing there; only a
# comparison run on one machine, interleaved so both builds meet the same
# noise, does. A regression is a slowdown benchstat calls significant
# (p<0.05) that is also larger than THRESHOLD percent: real regressions on
# these rows have been tens of percent, and a gate that trips on runner
# jitter gets ignored.
set -eu

base=$1
head=$2
runs=${RUNS:-6}
threshold=${THRESHOLD:-15}
gated='^(SmallMsgs|Bulk|BulkLarge|Relay|StreamChurn|EchoRTT)Mux-'
out=${OUT:-$(mktemp -d)}

(cd "$base/bench" && go test -c -o "$out/base.test" .)
(cd "$head/bench" && go test -c -o "$out/head.test" .)

: > "$out/base.txt"
: > "$out/head.txt"
pattern='^Benchmark(SmallMsgs|Bulk|BulkLarge|Relay|StreamChurn|EchoRTT)Mux$'
for i in $(seq "$runs"); do
	for v in base head; do
		"$out/$v.test" -test.run '^$' -test.bench "$pattern" -test.benchtime=1s |
			grep '^Benchmark' >> "$out/$v.txt"
	done
done

benchstat "$out/base.txt" "$out/head.txt" | tee "$out/report.txt"

# The first table is sec/op. A row regresses when its delta is a positive
# percentage above the threshold; benchstat prints "~" when the difference
# is not significant, so those never match.
benchstat -format csv "$out/base.txt" "$out/head.txt" | awk -F, -v re="$gated" -v t="$threshold" '
	/^$/ { table++ }
	table == 0 && $1 ~ re && $6 ~ /^\+[0-9.]+%$/ {
		d = substr($6, 2) + 0
		if (d > t) { printf "REGRESSION: %s %s (%s)\n", $1, $6, $7; bad = 1 }
	}
	END { exit bad }
'
