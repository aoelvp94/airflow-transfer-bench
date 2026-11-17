"""The stock Amazon-provider SqlToS3Operator, wrapped only to emit the same metrics."""

from __future__ import annotations

import platform
import time

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

from bench.python_transfer import peak_rss_mb


class TimedSqlToS3Operator(SqlToS3Operator):
    """Runs SqlToS3Operator unchanged and returns a metrics dict as its XCom.

    SqlToS3Operator materialises the full result set as a DataFrame before
    uploading, so its memory grows with the table. That is the point of
    including it: it is the operator most teams reach for first.
    """

    def execute(self, context):
        start = time.perf_counter()
        super().execute(context)
        duration = time.perf_counter() - start
        head = S3Hook(aws_conn_id=self.aws_conn_id).head_object(key=self.s3_key, bucket_name=self.s3_bucket)
        rows = context["ti"].xcom_pull(task_ids="prepare")["row_count"]
        return {
            "language": "python",
            "impl": "SqlToS3Operator (pandas)",
            "runtime": f"CPython {platform.python_version()}",
            "rows": rows,
            "bytes": head["ContentLength"],
            "parts": None,
            "key": self.s3_key,
            "first_row_s": None,
            "duration_s": round(duration, 4),
            "peak_rss_mb": peak_rss_mb(),
        }
