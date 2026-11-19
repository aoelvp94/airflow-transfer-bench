#!/usr/bin/env bash
# Trigger N runs of transfer_bench (they queue; max_active_runs=1) and wait for all to finish.
# The earliest run is a warm-up: `just report` keeps it in the raw files and
# drops it from the medians. Default N is 5. Task success and the MD5 gate
# are checked by `just report`, which exits non-zero on a mismatch.
# Runs inside the airflow-scheduler container:  just bench
set -euo pipefail

RUNS="${1:-5}"
CONF="${2:-{\}}"
DAG=transfer_bench

airflow dags unpause "$DAG" >/dev/null
for i in $(seq 1 "$RUNS"); do
  airflow dags trigger "$DAG" --conf "$CONF" >/dev/null
  echo "triggered run $i/$RUNS"
done

while true; do
  active=$(airflow dags list-runs "$DAG" --state queued -o plain 2>/dev/null | grep -c "$DAG" || true)
  running=$(airflow dags list-runs "$DAG" --state running -o plain 2>/dev/null | grep -c "$DAG" || true)
  if [[ "$active" -eq 0 && "$running" -eq 0 ]]; then
    break
  fi
  echo "$(date +%T) queued=$active running=$running"
  sleep 20
done
echo "all runs finished"
