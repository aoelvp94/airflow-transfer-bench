#!/usr/bin/env bash
# Build the image, seed a small table, run the bench, write results/.
# GitHub Actions is the caller. A laptop run is `just up && just seed && just bench && just report`.
#
# The scheduler cap (BENCH_CPUS / BENCH_MEM) is tighter than the Apple M4
# sample's 2 CPU / 6 GB on purpose: a GitHub-hosted runner also has Postgres
# and the S3 gateway. The report records the cgroup limit it actually saw.
set -euo pipefail
cd "$(dirname "$0")/.."

export AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.4.0}"
export AIRFLOW_PYTHON="${AIRFLOW_PYTHON:-3.12}"
export GO_SDK_VERSION="${GO_SDK_VERSION:-1.0.0}"
export JAVA_SDK_VERSION="${JAVA_SDK_VERSION:-1.0.0}"
export TS_SDK_VERSION="${TS_SDK_VERSION:-1.0.0}"
export SUPERVISOR_SCHEMA_VERSION="${SUPERVISOR_SCHEMA_VERSION:-2026-10-30}"
export ROWS="${ROWS:-200000}"
export RUNS="${RUNS:-5}"
export BENCH_CPUS="${BENCH_CPUS:-2}"
export BENCH_MEM="${BENCH_MEM:-3g}"
export BENCH_ENV_SLUG="${BENCH_ENV_SLUG:-github-actions}"
export BENCH_CI="${BENCH_CI:-true}"
export AIRFLOW_UID="${AIRFLOW_UID:-$(id -u)}"

python3 - <<'PY'
import os, re, sys
def need(name, pattern):
    value = os.environ[name]
    if not re.fullmatch(pattern, value):
        sys.exit(f"{name}={value!r} does not match {pattern}")
need("AIRFLOW_VERSION", r"\d+\.\d+\.\d+")
need("AIRFLOW_PYTHON", r"\d+\.\d+")
need("GO_SDK_VERSION", r"\d+\.\d+\.\d+")
need("JAVA_SDK_VERSION", r"\d+\.\d+\.\d+")
need("TS_SDK_VERSION", r"\d+\.\d+\.\d+")
need("SUPERVISOR_SCHEMA_VERSION", r"\d{4}-\d{2}-\d{2}")
need("ROWS", r"[1-9]\d*")
need("RUNS", r"[1-9]\d*")
if int(os.environ["RUNS"]) < 5:
    sys.exit("RUNS must be at least 5 (one warm-up is discarded)")
PY

mkdir -p results
# The scheduler writes this tree as uid 50000 unless AIRFLOW_UID is the runner.
# RESULTS.md is a file mount; it has to be writable by that user too.
chmod -R a+rwX results
chmod a+rw RESULTS.md

echo "GitHub-hosted runners are noisy. This run tracks trends, not absolute speed."
command -v just >/dev/null || { echo "just is not installed (CI uses extractions/setup-just)" >&2; exit 1; }
just up
just seed "$ROWS"
just bench "$RUNS"
just report
