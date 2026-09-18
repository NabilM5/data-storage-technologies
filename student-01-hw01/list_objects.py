"""List homework objects in MinIO with sizes (for evidence/object-listing.txt).

Run from infra/:
    docker compose exec -T spark python3 /scripts/hw01/list_objects.py
"""

import boto3

ENDPOINT = "http://minio:9000"
PREFIX = "student_01/weather/"

s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                  aws_access_key_id="admin", aws_secret_access_key="hse2026minio")

for bucket in ("raw", "datalake"):
    print(f"=== s3://{bucket}/{PREFIX} ===")
    total = count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            print(f"  {obj['Size']:>12,} B  s3://{bucket}/{obj['Key']}")
            total += obj["Size"]
            count += 1
    print(f"  -- objects: {count}; total: {total:,} B "
          f"({total / 1024 / 1024:.2f} MiB)\n")
