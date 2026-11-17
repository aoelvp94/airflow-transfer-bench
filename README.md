# airflow-transfer-bench

The same **Postgres → S3 transfer** written once per Apache Airflow Task SDK
language, run inside one Airflow DAG, measured the same way.

| Task | Language | How it runs in Airflow |
|---|---|---|
| `python_sql_to_s3` | Python | Stock `SqlToS3Operator` from the Amazon provider (pandas) |
| `transfer_python` | Python | `@task`, psycopg server-side cursor + boto3 multipart |
| `transfer_go` | Go | `@task.stub(queue="golang")` → [Go Task SDK](https://pkg.go.dev/github.com/apache/airflow/go-sdk) (`ExecutableCoordinator`) |
| `transfer_java` | Java | `@task.stub(queue="java")` → [Java Task SDK](https://airflow.apache.org/docs/java-sdk/stable/) (`JavaCoordinator`) |
| `transfer_typescript` | TypeScript | `@task.stub(queue="typescript")` → [TypeScript Task SDK](https://airflow.apache.org/docs/ts-sdk/stable/) (`NodeCoordinator`) |

These are the languages Airflow ships an official Task SDK for as of
**Airflow 3.4.0** (SDKs at 1.0.0). Upstream still labels the non-Python SDKs
*experimental*: APIs and the wire protocol may change between releases.

## What is measured

Each task streams `bench.events` (default 1M rows, ~175 MB in Postgres) to its
own S3 key as CSV:

- **Same query.** Every column is cast to text in SQL, so every language gets
  identical strings and has to produce **byte-identical CSV**. The `summarize`
  task fails the run if the custom implementations' outputs differ in size.
- **Same algorithm.** Server-side cursor (`fetch_size` rows per round trip),
  RFC 4180 quoting, 8 MiB multipart parts, no temp files.
- **Run one at a time.** Tasks are chained, so they never compete for CPU,
  Postgres or S3. `warm_cache` reads the table first so the first task doesn't
  pay for a cold cache.

Each task reports:

| Metric | Meaning |
|---|---|
| `task_s` | Airflow task-instance duration: runtime start-up (JVM, Node, Python), coordinator handshake, SDK overhead, plus the work |
| `work_s` | Time measured inside the task around the transfer itself |
| `overhead_s` | `task_s − work_s` |
| `MiB/s` | CSV bytes / `work_s` |
| `peak RSS MiB` | Peak resident memory of the process running the task |

## Sample result

Apple M4, Docker Desktop. The scheduler container (where LocalExecutor runs
the tasks) is capped at 2 CPUs / 6 GB. 1,000,000 rows → 146.6 MB CSV. Median
of 3 runs:

| task | runtime | task_s | work_s | overhead_s | MiB/s | peak RSS MiB |
|---|---|---:|---:|---:|---:|---:|
| transfer_go | go1.25.14 | 2.51 | 2.27 | 0.24 | 61.6 | 161 |
| transfer_java | Java 17.0.21 | 3.92 | 3.37 | 0.45 | 41.5 | 286 |
| transfer_typescript | Node.js v22.23.5 | 3.98 | 3.78 | 0.17 | 37.0 | 203 |
| transfer_python | CPython 3.12.16 | 5.26 | 4.47 | 0.51 | 31.3 | 234 |
| python_sql_to_s3 | CPython 3.12.16 | 14.03 | 13.05 | 0.82 | 10.7 | 1349 |

All five files had the same MD5. Treat these numbers as one machine's numbers
and run it yourself. The very first run after `make up` was several times
slower for every task (cold containers), so use `RUNS ≥ 3` and medians.

## Quick start

Requirements: Docker with Compose v2. Nothing else; all bundles are built in
the image.

```bash
make up                 # build image (~8 min first time), start Airflow, Postgres, S3
make seed ROWS=1000000  # create bench.events
make bench RUNS=3       # trigger 3 runs and wait
make report             # results/results.csv + results/summary.md
```

Airflow UI: <http://localhost:8080> (local-only setup, every user is admin).

Tune a run with DAG params (`fetch_size`, `part_size_mb`, `source_table`) from
the UI, or try other sizes with `make seed ROWS=10000000`. `BENCH_CPUS` and
`BENCH_MEM` change the scheduler container limits.

## Layout

```
dags/
  transfer_bench_dag.py      DAG: prepare → warm_cache → 5 transfers → summarize
  bench/config.py            query, bucket, conn ids (single source of truth)
  bench/python_transfer.py   hand-written Python implementation
  bench/operators.py         SqlToS3Operator wrapped to emit metrics
impls/
  go/main.go                 Go handler   (go-sdk v1.0.0, pgx, aws-sdk-go-v2)
  java/…/TransferJava.java   Java handler (airflow-sdk 1.0.0, pgjdbc, AWS SDK v2)
  typescript/src/main.ts     TS handler   (@apache-airflow/ts-sdk 1.0.0, postgres.js, AWS SDK v3)
docker/airflow.Dockerfile    multi-stage: builds the 3 bundles, adds JRE 17 + Node 22 to apache/airflow:3.4.0
docker-compose.yml           Airflow (LocalExecutor) + Postgres 17 + Versity S3 Gateway
scripts/                     bucket setup, run loop, report
sql/                         source DB init + seed
```

## How the language SDK wiring works

1. The Python DAG declares each non-Python task as a stub. Only an upstream
   task's **return value** can cross the language boundary, so `prepare`
   returns one `plan` dict and every stub is called as `transfer_x(plan=plan)`:

   ```python
   @task.stub(queue="golang", trigger_rule="all_done")
   def transfer_go(plan: dict): ...
   ```

2. Each queue maps to a coordinator, and each coordinator to the Dag bundle
   holding its compiled handler (`docker-compose.yml`):

   ```
   AIRFLOW__SDK__QUEUE_TO_COORDINATOR={"golang": "go", "java": "java", "typescript": "ts"}
   AIRFLOW__SDK__COORDINATORS={"go": {"classpath": "...ExecutableCoordinator",
                                      "kwargs": {"task_handler_bundle_name": "go-handlers"}}, ...}
   ```

3. Each handler registers against `(dag_id, task_id)`, e.g. in Go:

   ```go
   bundle.Register(airflow.TaskHandler("transfer_bench", "transfer_go", transfer))
   ```

   Connections (`bench_postgres`, `bench_s3`) come from the SDK client
   (`GetConnection` / `getConnection` / `getConnectionOrThrow`), so no
   credentials live in the handler code.

Problems found while building this:

- `@task.stub` arguments cannot reference XCom keys other than `return_value`;
  the DAG fails to serialize.
- The Go SDK decodes a dict into a struct strictly: every key needs a field,
  or the task fails with `json: unknown field`.
- Java's `airflow-sdk` has Jackson as a runtime dependency only; declare it if
  you use it.
- MinIO no longer publishes container images, so this uses
  [Versity S3 Gateway](https://github.com/versity/versitygw).

## Adding a language

Airflow has no other official Task SDK yet. To add one (for example Rust or
C#), implement the
[executable bundle spec](https://airflow.apache.org/docs/task-sdk/stable/executable-bundle-spec.html),
add a stub plus a queue → coordinator mapping, and keep the CSV rules in
`bench/config.py` so the byte-equality check still applies.

## License

MIT
