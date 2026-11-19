# Task runner for the transfer bench. `just` with no arguments lists the recipes.
#
# Override a version or a scheduler cap in
# the environment, and pass rows / runs as recipe arguments:
#
#   AIRFLOW_VERSION=3.4.0 just up
#   just seed 200000          # or: ROWS=200000 just seed
#   just bench 5              # or: RUNS=5 just bench
#   BENCH_CPUS=2 BENCH_MEM=6g just up
#   BENCH_ENV_SLUG=apple-m4 just report

set shell := ["bash", "-cu"]

compose := env_var_or_default("COMPOSE", "docker compose")
default_rows := env_var_or_default("ROWS", "1000000")
default_runs := env_var_or_default("RUNS", "5")

export AIRFLOW_VERSION := env_var_or_default("AIRFLOW_VERSION", "3.4.0")
export AIRFLOW_PYTHON := env_var_or_default("AIRFLOW_PYTHON", "3.12")
export GO_SDK_VERSION := env_var_or_default("GO_SDK_VERSION", "1.0.0")
export JAVA_SDK_VERSION := env_var_or_default("JAVA_SDK_VERSION", "1.0.0")
export TS_SDK_VERSION := env_var_or_default("TS_SDK_VERSION", "1.0.0")
export SUPERVISOR_SCHEMA_VERSION := env_var_or_default("SUPERVISOR_SCHEMA_VERSION", "2026-10-30")
export GO_VERSION := env_var_or_default("GO_VERSION", "1.25")
export JAVA_MAJOR := env_var_or_default("JAVA_MAJOR", "17")
export NODE_MAJOR := env_var_or_default("NODE_MAJOR", "22")
export BENCH_CPUS := env_var_or_default("BENCH_CPUS", "2")
export BENCH_MEM := env_var_or_default("BENCH_MEM", "6g")

# results/ is local raw output. A dirty results tree is not a dirty bench.
git_sha := `git rev-parse HEAD 2>/dev/null || echo unknown`
git_dirty := `git status --porcelain -- . ':(exclude)results' 2>/dev/null | grep -q . && echo true || echo false`

# list recipes
[default]
help:
    @just --list

# build the image without starting it
build:
    {{compose}} build

# build the image and start Airflow + Postgres + S3
up:
    {{compose}} up -d --build --wait --wait-timeout 600

# (re)create bench.events. `just seed 200000` or `ROWS=200000 just seed` (default 1000000)
seed rows=default_rows:
    {{compose}} exec -T postgres psql -U bench -d source -v rows={{rows}} -f /sql/seed.sql

# trigger N runs and wait. `just bench 5` or `RUNS=5 just bench` (default 5; first is a warm-up)
bench runs=default_runs:
    {{compose}} exec -T airflow-scheduler bash /opt/airflow/scripts/run_bench.sh {{runs}}

# write gitignored results/ and prepend RESULTS.md. Uses RUNS as the latest-run window
report runs=default_runs:
    #!/usr/bin/env bash
    set -euo pipefail
    chmod a+rw RESULTS.md
    args=(
        -e BENCH_GIT_SHA="{{git_sha}}"
        -e BENCH_GIT_DIRTY="{{git_dirty}}"
        -e BENCH_LATEST_RUNS="{{runs}}"
    )
    if [[ -n "${BENCH_ENV_SLUG:-}" ]]; then
        args+=(-e "BENCH_ENV_SLUG=${BENCH_ENV_SLUG}")
    fi
    if [[ -n "${BENCH_CI:-}" ]]; then
        args+=(-e "BENCH_CI=${BENCH_CI}")
    fi
    {{compose}} exec -T "${args[@]}" airflow-scheduler python /opt/airflow/scripts/report.py

# re-render RESULTS.md from the entries already in it
index:
    python3 scripts/report.py --index-only

# up + seed + bench + report
all: up seed bench report

# follow scheduler logs
logs:
    {{compose}} logs -f airflow-scheduler

# stop the stack
down:
    {{compose}} down

# stop and delete the Postgres and S3 volumes
clean:
    {{compose}} down -v

# build the three non-Python bundles on the host (Go 1.25+, Maven/JDK 17+, Node 22+)
bundles:
    cd impls/go && go tool airflow-go-pack --output dist/transfer_bench_go .
    cd impls/java && mvn -q -B package -DskipTests
    cd impls/typescript && npm ci && npm run bundle
