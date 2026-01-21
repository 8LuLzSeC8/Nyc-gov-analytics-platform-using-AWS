import boto3
import os
from datetime import datetime, timezone

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["bucket"]
    prefix = event["snapshot_prefix"]
    max_age_hours = float(event.get("max_age_hours") or os.getenv("DEFAULT_MAX_AGE_HOURS", "24"))

    latest_modified = None

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            lm = obj["LastModified"]
            if latest_modified is None or lm > latest_modified:
                latest_modified = lm

    if latest_modified is None:
        return {
            "freshnessOk": False,
            "reason": "No objects found under snapshot_prefix",
            "bucket": bucket,
            "snapshot_prefix": prefix
        }

    now = datetime.now(timezone.utc)
    age_hours = (now - latest_modified).total_seconds() / 3600.0

    return {
        "freshnessOk": age_hours <= max_age_hours,
        "bucket": bucket,
        "snapshot_prefix": prefix,
        "lastModified": latest_modified.isoformat(),
        "ageHours": round(age_hours, 2),
        "maxAgeHours": max_age_hours
    }
