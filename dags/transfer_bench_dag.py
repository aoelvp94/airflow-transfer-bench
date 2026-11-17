"""Postgres -> S3 transfer benchmark across Airflow's language Task SDKs.

    prepare ─> warm_cache ─> python_sql_to_s3 ─> transfer_python ─> transfer_go
                                                                       │
                     summarize <─ transfer_typescript <─ transfer_java <┘

Every transfer task copies the same query result (``bench.events``) to its own
S3 key as CSV, using a server-side cursor and a fixed-size multipart upload.
The Go, Java and TypeScript tasks are ``@task.stub`` declarations; their code
lives in ``impls/<language>`` and is routed by queue to the matching
coordinator (see ``docker-compose.yml``).

Tasks run strictly one after another so they never compete for CPU, the
database or MinIO. ``trigger_rule="all_done"`` keeps one failing language
from hiding the others' numbers.

Schedule: manual only (``make bench``). Nothing runs downstream.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, chain, dag, task

from bench import config
from bench.operators import TimedSqlToS3Operator
from bench.python_transfer import transfer as python_transfer

# --- Non-Python tasks. Only an upstream task's return value can cross the
# --- language boundary, so every implementation receives the whole `plan` dict.


@task.stub(queue="golang", trigger_rule="all_done")
def transfer_go(plan: dict): ...


@task.stub(queue="java", trigger_rule="all_done")
def transfer_java(plan: dict): ...


@task.stub(queue="typescript", trigger_rule="all_done")
def transfer_typescript(plan: dict): ...


@dag(
    dag_id=config.DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["benchmark", "multi-language", "transfer"],
    params={
        "source_table": Param(config.SOURCE_TABLE, type="string"),
        "fetch_size": Param(config.DEFAULT_FETCH_SIZE, type="integer", minimum=100),
        "part_size_mb": Param(config.DEFAULT_PART_SIZE_MB, type="integer", minimum=5),
    },
    doc_md=__doc__,
)
def transfer_bench():
    @task
    def prepare(params: dict, run_id: str) -> dict:
        hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
        row_count = hook.get_first(f"SELECT count(*) FROM {params['source_table']}")[0]
        safe_run = run_id.replace(":", "-").replace("+", "-")
        return {
            "query": config.QUERY_TEMPLATE.format(table=params["source_table"]),
            "bucket": config.BUCKET,
            "key_prefix": f"runs/{safe_run}",
            "fetch_size": params["fetch_size"],
            "part_size_mb": params["part_size_mb"],
            "postgres_conn_id": config.POSTGRES_CONN_ID,
            "s3_conn_id": config.S3_CONN_ID,
            "row_count": row_count,
        }

    @task
    def warm_cache(plan: dict) -> None:
        """Read the table once so the first measured task doesn't pay for a cold cache."""
        hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
        hook.get_first(f"SELECT count(*), sum(length(t::text)) FROM ({plan['query']}) t")

    @task(trigger_rule="all_done")
    def transfer_python(plan: dict) -> dict:
        return python_transfer(**{k: plan[k] for k in _ARG_NAMES})

    @task(trigger_rule="all_done")
    def summarize(ti=None) -> list[dict]:
        results = []
        for task_id in config.TRANSFER_TASK_IDS:
            metrics = ti.xcom_pull(task_ids=task_id)
            if metrics:
                results.append({"task_id": task_id, **metrics})
        for r in sorted(results, key=lambda r: r["duration_s"]):
            mb_s = r["bytes"] / 1_048_576 / r["duration_s"] if r["duration_s"] else 0
            print(
                f"{r['task_id']:<22} {r['duration_s']:>9.2f}s {mb_s:>8.1f} MiB/s "
                f"{r['peak_rss_mb']:>8.1f} MiB rss  {r['rows']} rows  {r['runtime']}"
            )
        # Custom implementations share CSV rules, so their outputs must be identical.
        sizes = {r["task_id"]: r["bytes"] for r in results if r["task_id"] != "python_sql_to_s3"}
        if len(set(sizes.values())) > 1:
            raise ValueError(f"Implementations produced different CSV sizes: {sizes}")
        missing = set(config.TRANSFER_TASK_IDS) - {r["task_id"] for r in results}
        if missing:
            raise ValueError(f"No metrics from: {sorted(missing)}")
        return results

    plan = prepare()

    sql_to_s3 = TimedSqlToS3Operator(
        task_id="python_sql_to_s3",
        trigger_rule="all_done",
        query="{{ ti.xcom_pull(task_ids='prepare')['query'] }}",
        sql_conn_id=config.POSTGRES_CONN_ID,
        aws_conn_id=config.S3_CONN_ID,
        s3_bucket=config.BUCKET,
        s3_key="{{ ti.xcom_pull(task_ids='prepare')['key_prefix'] }}/python_sql_to_s3.csv",
        file_format="csv",
        replace=True,
        pd_kwargs={"index": False},
    )

    chain(
        plan,
        warm_cache(plan),
        sql_to_s3,
        transfer_python(plan),
        transfer_go(plan=plan),
        transfer_java(plan=plan),
        transfer_typescript(plan=plan),
        summarize(),
    )


_ARG_NAMES = ("query", "bucket", "key_prefix", "fetch_size", "part_size_mb", "postgres_conn_id", "s3_conn_id")

transfer_bench()
