"""Shared constants for the transfer benchmark.

Every implementation (Python, Go, Java, TypeScript) receives the same values as
TaskFlow keyword arguments, so this file is the single source of truth.
"""

from __future__ import annotations

DAG_ID = "transfer_bench"

POSTGRES_CONN_ID = "bench_postgres"
S3_CONN_ID = "bench_s3"
BUCKET = "bench"
SOURCE_TABLE = "bench.events"

DEFAULT_FETCH_SIZE = 10_000
DEFAULT_PART_SIZE_MB = 8

# Every column is cast to text in SQL so that all languages receive identical
# strings and must produce byte-identical CSV. This keeps the comparison about
# the transfer path (driver + encoding + upload), not type formatting.
QUERY_TEMPLATE = """
SELECT t.id::text                                                         AS id,
       to_char(t.event_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS event_ts,
       t.user_id::text                                                    AS user_id,
       t.event_type,
       t.amount::text                                                     AS amount,
       t.payload::text                                                    AS payload
FROM {table} AS t
ORDER BY t.id  -- the integer column (primary key), not the text alias
""".strip()

# Task id -> queue. Python tasks run on the default queue.
LANGUAGE_QUEUES = {
    "transfer_go": "golang",
    "transfer_java": "java",
    "transfer_typescript": "typescript",
}

TRANSFER_TASK_IDS = [
    "python_sql_to_s3",
    "transfer_python",
    *LANGUAGE_QUEUES,
]
