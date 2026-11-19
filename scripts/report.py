#!/usr/bin/env python3
"""Collect transfer_bench results and write results/<airflow>/<date>-<slug>/.

Run inside the scheduler container (`make report`). The directory holds raw
per-run CSV/JSON, summary.md, and environment.json. This script also rewrites
results/README.md.

`--index-only` rebuilds the index from files already on disk and does not need
Airflow. The byte-identical MD5 check is a hard gate: a mismatch still writes
the raw files, then exits 1 so CI will not open a results PR.
"""

from __future__ import annotations

import csv
import json
import os
import platform
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from report_lib import (  # noqa: E402
    CSV_FIELDS,
    CUSTOM_TASK_IDS,
    TRANSFER_TASK_IDS,
    allocate_run_dir,
    benchmark_payload,
    choose_env_slug,
    cpu_limit_from_quota,
    evaluate_md5,
    human_bytes,
    parse_cpu_max,
    parse_cpu_model,
    parse_go_runtime,
    parse_java_version,
    parse_mem_max,
    parse_node_version,
    parse_numbered_runtime,
    render_summary_md,
    sanitize_run_id,
    split_warmup,
    summarize_tasks,
    write_index,
    write_text_atomic,
)

DAG_ID = "transfer_bench"
PREPARE_TASK = "prepare"
SUMMARIZE_TASK = "summarize"


def results_root() -> Path:
    override = os.environ.get("BENCH_RESULTS_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parent.parent / "results"


def decode(value):
    while isinstance(value, (str, bytes, bytearray)):
        value = json.loads(value)
    return value


def state_name(state) -> str:
    if state is None:
        return ""
    value = getattr(state, "value", state)
    return str(value)


def env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes"}


def _read(path: Path) -> str | None:
    try:
        return path.read_text()
    except OSError:
        return None


def cpu_model() -> str | None:
    text = _read(Path("/proc/cpuinfo"))
    if not text:
        return None
    return parse_cpu_model(text)


def cpu_limit() -> float | None:
    text = _read(Path("/sys/fs/cgroup/cpu.max"))
    if text:
        parsed = parse_cpu_max(text)
        if parsed is not None:
            return parsed
    quota = _read(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
    period = _read(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
    if quota and period:
        try:
            return cpu_limit_from_quota(int(quota.strip()), int(period.strip()))
        except ValueError:
            return None
    return None


def mem_limit() -> tuple[int | None, str | None]:
    for candidate in (
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ):
        text = _read(candidate)
        if text is None:
            continue
        parsed = parse_mem_max(text)
        if parsed is not None:
            return parsed, human_bytes(parsed)
    return None, None


def probe(cmd: list[str]) -> str:
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return ((completed.stdout or "") + (completed.stderr or "")).strip()


def load_build_manifest() -> dict:
    path = Path("/opt/airflow/bench-build.json")
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def bucket_name() -> str:
    candidates = [Path("/opt/airflow/dags"), Path(__file__).resolve().parent.parent / "dags"]
    for dags in candidates:
        if (dags / "bench" / "config.py").is_file():
            sys.path.insert(0, str(dags))
            from bench.config import BUCKET

            return BUCKET
    return "bench"


def object_md5(hook, bucket: str, key: str) -> tuple[str, int]:
    import hashlib

    obj = hook.get_key(key=key, bucket_name=bucket)
    if obj is None:
        raise ValueError(f"missing s3://{bucket}/{key}")
    digest = hashlib.md5()
    size = 0
    body = obj.get()["Body"]
    try:
        while chunk := body.read(8 * 1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    finally:
        close = getattr(body, "close", None)
        if close:
            close()
    return digest.hexdigest(), size


def collect() -> tuple[list[dict], list[dict], dict[str, str]]:
    from airflow.models.taskinstance import TaskInstance as TI
    from airflow.models.xcom import XComModel
    from airflow.utils.session import create_session
    from sqlalchemy import and_, select

    wanted = (*TRANSFER_TASK_IDS, PREPARE_TASK, SUMMARIZE_TASK)
    query = (
        select(TI.run_id, TI.task_id, TI.state, TI.duration, TI.start_date, TI.try_number, XComModel.value)
        .outerjoin(
            XComModel,
            and_(
                XComModel.dag_id == TI.dag_id,
                XComModel.run_id == TI.run_id,
                XComModel.task_id == TI.task_id,
                XComModel.key == "return_value",
            ),
        )
        .where(TI.dag_id == DAG_ID, TI.task_id.in_(wanted))
        .order_by(TI.start_date, TI.try_number)
    )
    with create_session() as session:
        rows = session.execute(query).all()

    # Keep the latest attempt of each task. Retries are off; this still drops
    # a duplicate row if the metadata DB has one.
    latest: dict[tuple[str, str], tuple] = {}
    for row in rows:
        latest[(row.run_id, row.task_id)] = row

    records = []
    prepares = []
    summarize_states: dict[str, str] = {}
    for run_id, task_id, state, duration, start_date, _try, value in latest.values():
        payload = decode(value) if value is not None else None
        if task_id == PREPARE_TASK:
            if isinstance(payload, dict):
                prepares.append({"run_id": run_id, **payload})
            continue
        if task_id == SUMMARIZE_TASK:
            summarize_states[run_id] = state_name(state)
            continue
        metrics = payload if isinstance(payload, dict) else {}
        work = metrics.get("duration_s")
        cpu = metrics.get("cpu_s")
        records.append(
            {
                "run_id": run_id,
                "task_id": task_id,
                "state": state_name(state),
                "language": metrics.get("language") or "",
                "impl": metrics.get("impl") or "",
                "runtime": metrics.get("runtime") or "",
                "rows": metrics.get("rows"),
                "bytes": metrics.get("bytes"),
                "md5": None,
                "task_s": round(float(duration), 4) if duration is not None else None,
                "work_s": round(float(work), 4) if work is not None else None,
                "cpu_s": round(float(cpu), 4) if cpu is not None else None,
                "overhead_s": (
                    round(float(duration) - float(work), 4) if duration is not None and work else None
                ),
                "mib_per_s": (
                    round(metrics["bytes"] / 1_048_576 / float(work), 4)
                    if work and metrics.get("bytes")
                    else None
                ),
                "first_row_s": metrics.get("first_row_s"),
                "peak_rss_mb": metrics.get("peak_rss_mb"),
                "key": metrics.get("key"),
                "started": start_date.isoformat() if start_date else "",
            }
        )
    return records, prepares, summarize_states


def attach_md5(records: list[dict]) -> list[str]:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="bench_s3")
    bucket = bucket_name()
    errors = []
    for record in records:
        key = record.get("key")
        if record["state"] != "success" or not key:
            continue
        try:
            digest, size = object_md5(hook, bucket, key)
        except Exception as exc:  # gateway or missing object: the gate fails
            errors.append(f"{record['run_id']} {record['task_id']}: {exc}")
            continue
        record["md5"] = digest
        if record.get("bytes") is not None and int(record["bytes"]) != size:
            errors.append(
                f"{record['run_id']} {record['task_id']}: reported {record['bytes']} bytes, object is {size}"
            )
    return errors


def _one(values: list, label: str, warnings: list[str]):
    present = [value for value in values if value is not None]
    if not present:
        return None
    unique = []
    for value in present:
        if value not in unique:
            unique.append(value)
    if len(unique) > 1:
        warnings.append(f"{label} differed across runs: {unique}")
    return unique[-1]


def _runtime(records: list[dict], task_id: str) -> str:
    for record in reversed(records):
        if record["task_id"] == task_id and record.get("runtime"):
            return record["runtime"]
    return ""


def build_environment(
    *,
    records: list[dict],
    prepares: list[dict],
    ordered_ids: list[str],
    warmup_ids: list[str],
    measured_ids: list[str],
    warnings: list[str],
) -> dict:
    import airflow

    manifest = load_build_manifest()
    airflow_version = getattr(airflow, "__version__", None) or manifest.get("airflow_version") or "unknown"
    if manifest.get("airflow_version") and manifest["airflow_version"] != airflow_version:
        warnings.append(
            f"image was asked for Airflow {manifest['airflow_version']} "
            f"but is running {airflow_version}"
        )
    sdk = (manifest.get("sdk") or {}).copy()
    go_runtime = parse_go_runtime(_runtime(records, "transfer_go"))
    jdk_runtime = parse_numbered_runtime(_runtime(records, "transfer_java")) or parse_java_version(probe(["java", "-version"]))
    node_runtime = parse_node_version(_runtime(records, "transfer_typescript")) or parse_node_version(probe(["node", "--version"]))
    cpython_runtime = parse_numbered_runtime(_runtime(records, "transfer_python")) or platform.python_version()
    mem_bytes, mem_human = mem_limit()
    started = [r["started"] for r in records if r.get("started")]
    date = min(started)[:10] if started else datetime.now(timezone.utc).date().isoformat()
    ci = env_bool("BENCH_CI")
    slug = choose_env_slug(cpu_model() or platform.machine(), override=os.environ.get("BENCH_ENV_SLUG"), ci=ci)
    git_sha = os.environ.get("BENCH_GIT_SHA") or "unknown"
    git_dirty = env_bool("BENCH_GIT_DIRTY")
    return {
        "airflow_version": airflow_version,
        "sdk": {
            "go": sdk.get("go"),
            "java": sdk.get("java"),
            "typescript": sdk.get("typescript"),
        },
        "runtimes": {
            "go": go_runtime,
            "jdk": jdk_runtime,
            "node": node_runtime,
            "cpython": cpython_runtime,
        },
        "cpu_model": cpu_model(),
        "cpu_limit": cpu_limit(),
        "mem_limit_bytes": mem_bytes,
        "mem_limit": mem_human,
        "rows": _one([p.get("row_count") for p in prepares], "rows", warnings),
        "fetch_size": _one([p.get("fetch_size") for p in prepares], "fetch_size", warnings),
        "part_size_mb": _one([p.get("part_size_mb") for p in prepares], "part_size", warnings),
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "n": len(measured_ids),
        "n_total": len(ordered_ids),
        "warmup_discarded": len(warmup_ids),
        "date": date,
        "env_slug": slug,
        "ci": ci,
        "arch": platform.machine(),
        "run_ids": ordered_ids,
        "warmup_run_ids": warmup_ids,
        "measured_run_ids": measured_ids,
        "build": manifest or None,
        "warnings": warnings,
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(CSV_FIELDS), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: ("" if row.get(key) is None else row.get(key)) for key in CSV_FIELDS})
    write_text_atomic(path, buffer.getvalue())


def order_runs(records: list[dict]) -> list[str]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[record["run_id"]].append(record)

    def start(run_id: str) -> str:
        times = [r["started"] for r in grouped[run_id] if r.get("started")]
        return min(times) if times else ""

    ordered = sorted(grouped, key=start)
    latest = os.environ.get("BENCH_LATEST_RUNS", "").strip()
    if latest and latest != "0":
        ordered = ordered[-int(latest) :]
    return ordered


def main() -> None:
    root = results_root()
    if "--index-only" in sys.argv:
        text = write_index(root)
        print(text)
        return

    records, prepares, summarize_states = collect()
    if not records:
        raise SystemExit("no transfer_bench task instances in the metadata DB")
    md5_errors = attach_md5(records)
    ordered_ids = order_runs(records)
    # Drop task rows that belong to runs outside the latest-N window.
    keep = set(ordered_ids)
    records = [r for r in records if r["run_id"] in keep]
    prepares = [p for p in prepares if p["run_id"] in keep]
    warmup_ids, measured_ids = split_warmup(ordered_ids, warmup=1)
    warmup = set(warmup_ids)
    for record in records:
        record["warmup"] = record["run_id"] in warmup

    warnings: list[str] = []
    meta = build_environment(
        records=records,
        prepares=prepares,
        ordered_ids=ordered_ids,
        warmup_ids=warmup_ids,
        measured_ids=measured_ids,
        warnings=warnings,
    )
    digests = {
        run_id: {r["task_id"]: r.get("md5") for r in records if r["run_id"] == run_id}
        for run_id in ordered_ids
    }
    md5_info = evaluate_md5(digests)
    for error in md5_errors:
        md5_info["ok"] = False
        md5_info["failures"].append(error)
        md5_info["all_five_match"] = False

    problems = []
    by_run: dict[str, dict[str, dict]] = defaultdict(dict)
    for record in records:
        by_run[record["run_id"]][record["task_id"]] = record
    for run_id in ordered_ids:
        states = {task: by_run[run_id].get(task, {}).get("state") for task in TRANSFER_TASK_IDS}
        for task, state in states.items():
            if state != "success":
                problems.append(f"{run_id} {task} state={state}")
        if summarize_states.get(run_id) != "success":
            problems.append(f"{run_id} summarize state={summarize_states.get(run_id)}")
    if not md5_info["ok"]:
        problems.append("MD5 gate failed")
    if meta["n_total"] < 5:
        warnings.append(
            f"only {meta['n_total']} run(s); the published-run protocol triggers at least 5 and discards 1 warm-up"
        )
    if meta["git_dirty"]:
        warnings.append("git tree was dirty; this run is not comparable with a clean commit")
    if meta["git_sha"] in ("", "unknown"):
        warnings.append("git SHA was not provided (make report passes BENCH_GIT_SHA)")
    missing_cpu = [
        task for task in TRANSFER_TASK_IDS
        if any(r["task_id"] == task and r["run_id"] in measured_ids and r.get("cpu_s") is None for r in records)
    ]
    if missing_cpu:
        warnings.append(f"cpu_s missing for: {', '.join(missing_cpu)}")

    measured = [r for r in records if r["run_id"] in set(measured_ids)]
    task_stats = summarize_tasks(measured)
    meta["warnings"] = warnings
    meta["md5"] = {
        "ok": md5_info["ok"],
        "digest": md5_info.get("digest"),
        "all_five_match": md5_info.get("all_five_match"),
        "failures": md5_info.get("failures") or [],
    }
    meta["protocol_ok"] = (
        not problems
        and md5_info["ok"]
        and meta["n_total"] >= 5
        and meta["warmup_discarded"] == 1
        and meta["n"] >= 4
        and not meta["git_dirty"]
        and meta["git_sha"] not in ("", "unknown")
    )

    run_dir = allocate_run_dir(root, meta["airflow_version"], meta["date"], meta["env_slug"], ordered_ids)
    runs_dir = run_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    def dump(path: Path, payload) -> None:
        write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")

    dump(run_dir / "environment.json", meta)
    stats_doc = {"tasks": task_stats, "md5": meta["md5"]}
    dump(run_dir / "stats.json", stats_doc)
    summary = render_summary_md(meta, task_stats, md5_info)
    write_text_atomic(run_dir / "summary.md", summary)
    ordered_records = sorted(records, key=lambda r: (r.get("started") or "", TRANSFER_TASK_IDS.index(r["task_id"]) if r["task_id"] in TRANSFER_TASK_IDS else 99))
    write_csv(run_dir / "results.csv", ordered_records)
    dump(run_dir / "benchmark.json", benchmark_payload(meta, task_stats))
    for run_id in ordered_ids:
        safe = sanitize_run_id(run_id)
        run_records = [r for r in ordered_records if r["run_id"] == run_id]
        dump(
            runs_dir / f"{safe}.json",
            {"run_id": run_id, "warmup": run_id in warmup, "tasks": run_records},
        )
        write_csv(runs_dir / f"{safe}.csv", run_records)

    index = write_index(root)
    rel = run_dir.relative_to(root.parent).as_posix()
    print(summary)
    print(index)
    print(f"BENCH_RUN_DIR={rel}")
    if problems:
        print("BENCH_GATE=fail", file=sys.stderr)
        for problem in problems:
            print(problem, file=sys.stderr)
        raise SystemExit(1)
    print("BENCH_GATE=pass")


if __name__ == "__main__":
    main()
