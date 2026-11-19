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
  task downloads each object and fails the run if the hand-written
  implementations differ by MD5 or by size.
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
| `cpu_s` | Process CPU time (user + system) over the same window as `work_s` |
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
and run it yourself. The very first run after `just up` was several times
slower for every task (cold containers), so use `RUNS ≥ 3` and medians.

That table is Aldo's local run. It is not an entry in [`RESULTS.md`](RESULTS.md),
and it has no `cpu_s` column because that metric was added after it was
recorded. Do not treat it as a published data point or extend it.

## Published results

The committed record is [`RESULTS.md`](RESULTS.md): a changelog, newest entry
first, one section per run. `just report` prepends that section (the same
Airflow dag run ids replace it) and writes the raw files locally:

```
results/<airflow-version>/<YYYY-MM-DD>-<env-slug>/   gitignored
  environment.json     Airflow, each SDK, Go/JDK/Node/CPython, CPU, limits, rows, fetch_size, part_size, git SHA, n
  summary.md           median, min, max, bootstrap 95% CI
  stats.json
  results.csv          every task of every run, warm-up included
  runs/<run_id>.json   raw per-run records
  runs/<run_id>.csv
  benchmark.json       customSmallerIsBetter (task_s, work_s, cpu_s); CI copies this to gh-pages only
```

`RESULTS.md` groups the trend by environment. A number from `github-actions`
and a number from a laptop are not one series.

The protocol for a run that shows up in the trend table:

- trigger at least 5 runs (`RUNS=5`, the default)
- discard the earliest run as a warm-up (it stays in the raw files)
- report the median, the min and the max, and a bootstrap 95% CI of the median
- pass the MD5 gate. The four hand-written implementations must upload
  byte-identical objects. `SqlToS3Operator` is timed either way; its MD5 is
  recorded and, when it matches, the summary says so

`n` in the metadata is the measured count, after the warm-up is dropped.

## Tracking across releases

Airflow and the three SDK versions are image build args. The defaults are
**3.4.0** and **1.0.0**:

```bash
AIRFLOW_VERSION=3.4.0 GO_SDK_VERSION=1.0.0 JAVA_SDK_VERSION=1.0.0 TS_SDK_VERSION=1.0.0 just up
```

[`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml) does two jobs:

- **`workflow_dispatch`** takes `airflow_version`, `go_sdk_version`,
  `java_sdk_version`, `ts_sdk_version`, `supervisor_schema_version`, `rows`
  (default 200000), and `runs` (default 5). It builds that combination, seeds,
  benches, and opens a pull request that changes only `RESULTS.md`.
- **Weekly schedule** (Monday 06:17 UTC) reads PyPI and runs only when the
  newest stable `apache-airflow` at or above 3.4.0 has no `github-actions`
  run yet. SDK versions stay at 1.0.0 unless someone dispatches other values.
  A laptop run of the same Airflow version does not suppress this.

The same workflow generates `benchmark.json` and publishes it through
[benchmark-action/github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark)
(`tool: customSmallerIsBetter`) onto the `gh-pages` branch, under `dev/bench`.
That file is not committed to `main`. The chart is fed only by this workflow.
Pointing it at a laptop `benchmark.json` would mix machines into one line.

GitHub-hosted runners are noisy. CI numbers track trends and regressions across
releases. They are not absolute speed, and they are not comparable with the
Apple M4 sample above. The CI scheduler cap is 2 CPU / 3 GB, not the sample's
2 CPU / 6 GB, and the row count defaults to 200,000 rather than 1,000,000.
The `RESULTS.md` entry for those runs repeats this.

This workflow does not change repository settings. A maintainer still has to:

1. Set Actions workflow permissions to read and write, and allow GitHub Actions
   to create pull requests, or the results PR and the `gh-pages` push fail.
2. Let the first successful chart step create `gh-pages` (or push an empty
   orphan `gh-pages` branch if the action reports the branch is missing).
3. Enable GitHub Pages from the `gh-pages` branch, folder `/`, if the chart
   should be on the web. The page is `dev/bench` on that branch.

The Java bundle's `Airflow-Supervisor-Schema-Version` is the build arg
`SUPERVISOR_SCHEMA_VERSION` (default `2026-10-30`, the value this repo uses
with Airflow 3.4.0). A newer Airflow may need a newer date; pass it on
`workflow_dispatch` if the Java task fails the supervisor schema check.

## Quick start

Requirements: Docker with Compose v2, and [`just`](https://github.com/casey/just)
(`cargo install just`, or the packages and GitHub action listed in the
[just docs](https://just.systems/man/en/packages.html)). Nothing else; all
bundles are built in the image.

```bash
just up                 # build image (~8 min first time), start Airflow, Postgres, S3
just seed 1000000       # create bench.events (default ROWS=1000000)
just bench              # 5 runs; the first is a discarded warm-up (default RUNS=5)
just report             # gitignored results/ plus one RESULTS.md entry
```

`just seed 200000` and `ROWS=200000 just seed` are the same. `just bench 5`
and `RUNS=5 just bench` are the same. `just` with no arguments lists the recipes.

Airflow UI: <http://localhost:8080> (local-only setup, every user is admin).

Tune a run with DAG params (`fetch_size`, `part_size_mb`, `source_table`) from
the UI, or try other sizes with `just seed 10000000`. `BENCH_CPUS` and
`BENCH_MEM` change the scheduler container limits (`BENCH_CPUS=2 BENCH_MEM=6g just up`).

## How to contribute a run

A published run is a pull request that changes [`RESULTS.md`](RESULTS.md) and
nothing else. Do not edit the Apple M4 sample above, and do not add that
sample as an entry. Do not commit `results/`; those files are local.

```bash
just up
just seed 1000000               # or another size; the metadata records the size you actually loaded
just bench                      # RUNS=5
just report                     # captures the environment, checks MD5, prepends RESULTS.md
git add RESULTS.md
git status                      # only RESULTS.md
```

Open a pull request. `just report` reads the environment from the running
container and from the git checkout: Airflow version, each SDK version, Go,
JDK, Node, and CPython versions, CPU model, the scheduler CPU and memory
limits, rows, `fetch_size`, `part_size`, git SHA, and `n`. That metadata is
the compact line in the new `RESULTS.md` section. The raw CSV and JSON stay
under `results/` on your machine.

`BENCH_ENV_SLUG=apple-m4 just report` overrides the slug derived from the CPU
model. The slug is part of the section heading. Re-running `just report` on
the same dag run ids replaces that section. A different set of run ids is
prepended. Raw files for a repeated date and slug still land in
`results/<version>/<date>-<slug>/`, with a numeric suffix when the run ids differ.

`AIRFLOW_VERSION`, `GO_SDK_VERSION`, `JAVA_SDK_VERSION`, and `TS_SDK_VERSION`
select the image. Change them before `just up`, not after, or the metadata
will describe a build you did not run.

`just report` summarizes the latest `RUNS` dag runs. Use a fresh volume
(`just clean` deletes the Postgres and S3 data) when an earlier failed attempt
is still in the metadata DB and you do not want it in the window.

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
justfile                     up, seed, bench, report, and the other recipes
scripts/                     bucket setup, run loop, report, CI entrypoint
RESULTS.md                   committed changelog (one section per run)
results/                     local raw output, gitignored except .gitkeep
.github/workflows/benchmark.yml
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

## Caveats

- Each published run is one machine, one cap, and a small `n` (5 triggered, 1
  discarded, 4 left in the median). The bootstrap interval is wide. A single
  run moving by a few tenths of a second is not a regression by itself.
- The object store is Versity S3 Gateway with a posix backend on the same
  Docker network, not Amazon S3. There is no regional round trip. The numbers
  are about client CPU, the database driver, CSV encoding, and Task SDK
  overhead.
- The Go, Java, and TypeScript Task SDKs are experimental. A later SDK release
  can change the wire protocol and make two Airflow versions not the same
  comparison even when the transfer code did not change.
- Tasks run one at a time on LocalExecutor, inside the scheduler container.
  That is deliberate, so they do not compete, and it is not how a busy
  production executor behaves.
- GitHub-hosted runners are shared and noisy. Use those rows to watch a
  release-to-release trend. Do not quote them as the speed of a language, and
  do not set them next to the Apple M4 sample as if they measured the same thing.
- The MD5 gate covers the four hand-written implementations. `SqlToS3Operator`
  goes through pandas and is not required to match, though on the M4 sample it
  did.

## License

MIT
