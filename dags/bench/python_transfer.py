"""Hand-written Python streaming transfer: Postgres server-side cursor -> S3 multipart.

This mirrors the Go, Java and TypeScript implementations line for line:
same query, same CSV rules, same part size, same metrics.
"""

from __future__ import annotations

import csv
import io
import platform
import resource
import time

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def peak_rss_mb() -> float:
    # ru_maxrss is KiB on Linux (the containers this runs in).
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def cpu_time_s() -> float:
    """Process CPU seconds so far (user + system), including child processes."""
    total = 0.0
    for who in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN):
        usage = resource.getrusage(who)
        total += usage.ru_utime + usage.ru_stime
    return total


class MultipartWriter:
    """Buffers bytes and uploads fixed-size parts; mirrors the other languages."""

    def __init__(self, s3, bucket: str, key: str, part_size: int) -> None:
        self.s3, self.bucket, self.key, self.part_size = s3, bucket, key, part_size
        self.upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        self.buf = bytearray()
        self.parts: list[dict] = []
        self.bytes = 0

    def write(self, data: bytes) -> None:
        self.buf += data
        self.bytes += len(data)
        if len(self.buf) >= self.part_size:
            self._flush()

    def _flush(self) -> None:
        number = len(self.parts) + 1
        resp = self.s3.upload_part(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id,
            PartNumber=number, Body=bytes(self.buf),
        )
        self.parts.append({"ETag": resp["ETag"], "PartNumber": number})
        self.buf.clear()

    def close(self) -> None:
        if self.buf or not self.parts:
            self._flush()
        self.s3.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id,
            MultipartUpload={"Parts": self.parts},
        )

    def abort(self) -> None:
        self.s3.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)


def transfer(
    *,
    query: str,
    bucket: str,
    key_prefix: str,
    fetch_size: int,
    part_size_mb: int,
    postgres_conn_id: str,
    s3_conn_id: str,
) -> dict:
    start = time.perf_counter()
    cpu0 = cpu_time_s()
    key = f"{key_prefix}/python.csv"
    s3 = S3Hook(aws_conn_id=s3_conn_id).get_conn()
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    writer = MultipartWriter(s3, bucket, key, part_size_mb * 1024 * 1024)
    rows = 0
    first_row_s = None
    try:
        with conn.cursor(name="transfer_bench") as cur:  # server-side cursor
            cur.itersize = fetch_size
            cur.execute(query)
            header_written = False
            while batch := cur.fetchmany(fetch_size):
                if first_row_s is None:
                    first_row_s = time.perf_counter() - start
                out = io.StringIO()
                w = csv.writer(out, lineterminator="\n")
                if not header_written:
                    w.writerow([d[0] for d in cur.description])
                    header_written = True
                w.writerows(["" if v is None else v for v in row] for row in batch)
                writer.write(out.getvalue().encode())
                rows += len(batch)
        writer.close()
    except BaseException:
        writer.abort()
        raise
    finally:
        conn.close()

    return {
        "language": "python",
        "impl": "psycopg named cursor + boto3 multipart",
        "runtime": f"CPython {platform.python_version()}",
        "rows": rows,
        "bytes": writer.bytes,
        "parts": len(writer.parts),
        "key": key,
        "first_row_s": round(first_row_s or 0.0, 4),
        "duration_s": round(time.perf_counter() - start, 4),
        # Same window as duration_s: the transfer, not the Airflow task startup.
        "cpu_s": round(cpu_time_s() - cpu0, 4),
        "peak_rss_mb": peak_rss_mb(),
    }
