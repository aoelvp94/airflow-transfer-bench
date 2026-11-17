"""Create the benchmark bucket on the local S3 gateway (idempotent, retries until it is up)."""

import time

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = "bench"

hook = S3Hook(aws_conn_id="bench_s3")
for attempt in range(30):
    try:
        if not hook.check_for_bucket(BUCKET):
            hook.create_bucket(bucket_name=BUCKET)
        print(f"bucket {BUCKET!r} ready")
        break
    except Exception as exc:  # gateway still starting
        print(f"waiting for S3 ({exc.__class__.__name__})")
        time.sleep(2)
else:
    raise SystemExit("S3 gateway never became ready")
