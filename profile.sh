#!/usr/bin/env bash
#
# Profile the DTA reader and writer with samply, then report the
# top functions from this crate by inclusive/self-time percentage.
#
# Usage:
#   ./profile.sh [--records N] [--top N]
#   ./profile.sh                        # 1M records, top 10 functions
#   ./profile.sh --records 500000       # 500K records, top 10 functions
#   ./profile.sh --top 20               # 1M records, top 20 functions
#
# Requirements:
#   cargo install --locked samply
#
#   On Linux/WSL, samply uses perf_event_open which requires
#   /proc/sys/kernel/perf_event_paranoid to be 1 or lower:
#       echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid

set -uo pipefail

RECORDS=1000000
TOP_N=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --records) RECORDS="$2"; shift 2 ;;
        --top)     TOP_N="$2";   shift 2 ;;
        *)         echo "Unknown option: $1"; exit 1 ;;
    esac
done

PROFILE_BIN="target/profiling/examples/profile"
REPORT_BIN="target/profiling/examples/profile_report"
DATA_FILE="target/profile_bench.dta"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

if ! command -v samply &>/dev/null; then
    echo "Error: 'samply' not found."
    echo "Install with: cargo install --locked samply"
    exit 1
fi

if [[ -r /proc/sys/kernel/perf_event_paranoid ]]; then
    paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
    if (( paranoid > 1 )); then
        echo "Warning: /proc/sys/kernel/perf_event_paranoid is $paranoid."
        echo "samply needs this to be 1 or lower. Lower it with:"
        echo "    echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid"
        echo ""
    fi
fi

echo "Building profiling binaries..."
if ! cargo build --example profile --example profile_report --profile profiling -p dta --all-features --quiet; then
    echo "Error: build failed."
    exit 1
fi

# ---------------------------------------------------------------------------
# Profile each phase
# ---------------------------------------------------------------------------

run_phase() {
    local phase="$1"
    local json_file="target/profile_${phase}.json.gz"
    local syms_file="${json_file%.gz}.syms.json"

    echo ""
    echo "Recording ${phase} phase ($RECORDS records)..."
    samply record --save-only --unstable-presymbolicate \
        -o "$json_file" -- "./$PROFILE_BIN" --phase "$phase" --records "$RECORDS" --file "$DATA_FILE"
    local samply_exit=$?

    if (( samply_exit != 0 )) || [[ ! -f "$json_file" ]]; then
        echo "Warning: samply did not produce '$json_file' for ${phase} phase (exit $samply_exit). Skipping report."
        rm -f "$json_file" "$syms_file"
        return
    fi

    echo ""
    echo "=== ${phase^^} ==="
    "./$REPORT_BIN" --input "$json_file" --top "$TOP_N" || true

    rm -f "$json_file" "$syms_file"
}

run_phase write
run_phase read
run_phase async-write
run_phase async-read

rm -f "$DATA_FILE"

echo ""
echo "Done."
