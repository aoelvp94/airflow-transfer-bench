# transfer_bench 3.4.0 — 2026-11-19-intel-xeon-processor

Warm-up runs discarded: 1. Statistics use n=4 successful measured run(s) out of 5 triggered.
The median is the middle value, or the mean of the two middle values when n is even. The 95% CI is a percentile bootstrap of that median (5000 resamples, fixed seed). With n this small the interval is wide.

Byte-identical MD5 gate: pass (`1dd073f34ef3b14829b4df2fbf4baa80`).
SqlToS3Operator MD5 matches the hand-written implementations.

| task | runtime | n | rows | task_s median (min–max) | task_s 95% CI | work_s median (min–max) | cpu_s median (min–max) | overhead_s median | MiB/s median | peak RSS MiB median |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| transfer_go | go1.25.14 | 4 | 200,000 | 0.42 (0.41–0.56) | 0.41–0.56 | 0.38 (0.36–0.50) | 0.14 (0.14–0.17) | 0.05 (0.05–0.05) | 73.9 (55.5–76.4) | 165 (160–168) |
| transfer_typescript | Node.js v22.23.5 | 4 | 200,000 | 0.81 (0.80–0.85) | 0.80–0.85 | 0.75 (0.74–0.80) | 0.81 (0.79–0.85) | 0.06 (0.05–0.06) | 37.0 (35.0–37.6) | 163 (159–166) |
| transfer_java | Java 17.0.21 | 4 | 200,000 | 1.23 (1.20–1.26) | 1.20–1.26 | 0.98 (0.96–1.01) | 1.57 (1.53–1.63) | 0.25 (0.25–0.25) | 28.3 (27.5–29.2) | 216 (207–224) |
| transfer_python | CPython 3.12.16 | 4 | 200,000 | 1.32 (1.31–1.37) | 1.31–1.37 | 0.99 (0.98–1.02) | 0.66 (0.66–0.68) | 0.33 (0.32–0.35) | 28.1 (27.3–28.4) | 237 (237–238) |
| python_sql_to_s3 | CPython 3.12.16 | 4 | 200,000 | 2.10 (2.03–2.18) | 2.03–2.18 | 1.77 (1.72–1.85) | 1.66 (1.62–1.71) | 0.32 (0.31–0.34) | 15.7 (15.1–16.3) | 490 (487–492) |

## Environment

- Airflow: 3.4.0
- SDK go: 1.0.0
- SDK java: 1.0.0
- SDK typescript: 1.0.0
- Go: go1.25.14
- JDK: 17.0.21
- Node: v22.23.5
- CPython: 3.12.16
- CPU model: Intel(R) Xeon(R) Processor
- CPU limit: 2.0
- Memory limit: 3 GiB
- rows: 200000
- fetch_size: 10000
- part_size: 8 MiB
- git SHA: 092631ccf6927ca2ad08f77393c5e5d547c38c16
- n: 4 measured, 5 triggered, 1 warm-up discarded
- env: intel-xeon-processor
- ci: False
