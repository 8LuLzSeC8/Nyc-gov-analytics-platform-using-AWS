import json
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["bucket"]
    metrics_prefix = event["metrics_prefix"]
    threshold = float(event.get("quality_threshold", 0.98))

    if metrics_prefix and not metrics_prefix.endswith("/"):
        metrics_prefix += "/"

    metrics_key = f"{metrics_prefix}_METRICS.json"

    try:
        obj = s3.get_object(Bucket=bucket, Key=metrics_key)
        metrics = json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        return {
            "qualityPassed": False,
            "reason": "METRICS_READ_ERROR",
            "metrics_key": metrics_key,
            "error": str(e)
        }

    total_rows = metrics.get("total_rows", 0)
    pu_rate = metrics.get("pu_zone_nonnull_rate", 0)
    do_rate = metrics.get("do_zone_nonnull_rate", 0)

    quality_passed = (
        total_rows > 0 and
        pu_rate >= threshold and
        do_rate >= threshold
    )

    return {
        "qualityPassed": quality_passed,
        "metrics_key": metrics_key,
        "total_rows": total_rows,
        "pu_zone_nonnull_rate": pu_rate,
        "do_zone_nonnull_rate": do_rate,
        "threshold": threshold,
        "governance": metrics.get("governance", {})
    }
