"""Collect per-task results from the Airflow metadata DB and write results/.

Two timings per task:
  * task_s  - Airflow's task-instance duration: includes interpreter/JVM/Node
              start-up, coordinator handshake and SDK overhead.
  * work_s  - measured inside the task around the actual transfer.

Outputs results/results.csv (every task of every run) and results/summary.md
(median per implementation over successful runs). Run inside a container:
    make report
"""

from __future__ import annotations

import csv
import json
import os
import statistics
from pathlib import Path

from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.xcom import XComModel
from airflow.utils.session import create_session
from sqlalchemy import and_, select

DAG_ID = "transfer_bench"
OUT = Path(os.environ.get("BENCH_RESULTS_DIR", "/opt/airflow/results"))
TASKS = ["python_sql_to_s3", "transfer_python", "transfer_go", "transfer_java", "transfer_typescript"]

def as_dict(value) -> dict:
    while isinstance(value, (str, bytes)):
        value = json.loads(value)
    return value or {}


def main() -> None:
    # Airflow's ORM rather than raw SQL: the metadata schema changes between releases.
    query = (
        select(TI.run_id, TI.task_id, TI.state, TI.duration, TI.start_date, XComModel.value)
        .outerjoin(
            XComModel,
            and_(
                XComModel.dag_id == TI.dag_id, XComModel.run_id == TI.run_id,
                XComModel.task_id == TI.task_id, XComModel.key == "return_value",
            ),
        )
        .where(TI.dag_id == DAG_ID, TI.task_id.in_(TASKS))
        .order_by(TI.start_date)
    )
    with create_session() as session:
        rows = session.execute(query).all()

    records = []
    for run_id, task_id, state, duration, start_date, value in rows:
        m = as_dict(value)
        work = m.get("duration_s")
        records.append(
            {
                "run_id": run_id,
                "task_id": task_id,
                "state": state,
                "language": m.get("language", ""),
                "impl": m.get("impl", ""),
                "runtime": m.get("runtime", ""),
                "rows": m.get("rows"),
                "bytes": m.get("bytes"),
                "task_s": round(duration, 3) if duration is not None else None,
                "work_s": work,
                "overhead_s": round(duration - work, 3) if duration is not None and work else None,
                "mib_per_s": round(m["bytes"] / 1_048_576 / work, 1) if work and m.get("bytes") else None,
                "first_row_s": m.get("first_row_s"),
                "peak_rss_mb": m.get("peak_rss_mb"),
                "started": start_date.isoformat() if start_date else "",
            }
        )

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(records[0]) if records else ["run_id"])
        w.writeheader()
        w.writerows(records)

    ok = [r for r in records if r["state"] == "success" and r["work_s"]]
    lines = [
        "| task | runtime | runs | rows | task_s (median) | work_s (median) | overhead_s | MiB/s | peak RSS MiB |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    summary = []
    for task_id in TASKS:
        rs = [r for r in ok if r["task_id"] == task_id]
        if not rs:
            continue
        med = lambda k: statistics.median(r[k] for r in rs if r[k] is not None)  # noqa: E731
        summary.append((med("task_s"), task_id, rs, med))
    for _, task_id, rs, med in sorted(summary):
        lines.append(
            f"| {task_id} | {rs[-1]['runtime']} | {len(rs)} | {rs[-1]['rows']:,} | {med('task_s'):.2f} | "
            f"{med('work_s'):.2f} | {med('overhead_s'):.2f} | {med('mib_per_s'):.1f} | {med('peak_rss_mb'):.0f} |"
        )
    failed = [r for r in records if r["state"] != "success"]
    if failed:
        lines.append(f"\n{len(failed)} task instance(s) did not succeed; see results.csv.")
    (OUT / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
