#!/usr/bin/env bash
# Measure the Lua-level fixes of make.py against the unpatched tree.
#
#   MCC_SRC=/tmp/mcc-snapshot bench/variants/run.sh
#
# Variants: base (unpatched), every fix of make.py alone, all of them, and
# GC tuning: gc400 = base with collectgarbage('setpause', 400), all+gc400.
# Repetitions are the outer loop, so host noise spreads over all variants.
# The results go to OUT/<variant>/none/mcc/<workload>/; compare.py prints
# the table.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BENCH=$(cd "$HERE/.." && pwd)
SRC=${MCC_SRC:-$(cd "$BENCH/.." && pwd)}
VDIR=${VDIR:-/tmp/mcc-bench/variants}
OUT=${OUT:-/tmp/mcc-bench/results/variants-$(date +%Y%m%d-%H%M%S)}
VARIANTS=${VARIANTS:-base nola notxn nobatch noclosure ibuf waitfirst parse enc gc400 all all+gc400}
WORKLOADS=${WORKLOADS:-w1 w2}
REPS=${REPS:-2}
# REP_FIRST=3 REPS=4 adds repetitions 3 and 4 to an existing OUT.
REP_FIRST=${REP_FIRST:-1}
DURATION=${DURATION:-15}
WARMUP=${WARMUP:-5}

python3 -I "$HERE/make.py" "$SRC" "$VDIR" >/dev/null
mkdir -p "$OUT"
for rep in $(seq "$REP_FIRST" "$REPS"); do
    for v in $VARIANTS; do
        name=${v%+gc400}
        gc=
        [ "$v" != "$name" ] && gc=400
        if [ "$v" = gc400 ]; then
            name=base
            gc=400
        fi
        src=$SRC
        [ "$name" = base ] || src=$VDIR/$name
        echo "[$(date +%H:%M:%S)] variant=$v rep=$rep" >&2
        MCC_SRC=$src BENCH_GC_PAUSE=$gc OUT=$OUT/$v SERVERS=mcc \
            WORKLOADS="$WORKLOADS" REP_FIRST=$rep REPS=$rep \
            DURATION=$DURATION WARMUP=$WARMUP "$BENCH/run.sh" >/dev/null
    done
done
python3 -I "$HERE/compare.py" "$OUT" | tee "$OUT/compare.md"
