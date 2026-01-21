import sys
import json
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


# ----------------------------
# Helpers
# ----------------------------
s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")

def _latest_prefix_by_last_modified(bucket: str, base_prefix: str) -> str:
    """
    Finds the most recently modified object under base_prefix and returns the 'directory'
    prefix to read from (base_prefix + run_id=.../ or snapshot_id=.../ etc).
    """
    if base_prefix and not base_prefix.endswith("/"):
        base_prefix += "/"

    paginator = s3.get_paginator("list_objects_v2")
    latest = None  # (LastModified, Key)

    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            lm = obj["LastModified"]
            if latest is None or lm > latest[0]:
                latest = (lm, key)

    if not latest:
        raise Exception(f"No objects found under s3://{bucket}/{base_prefix}")

    latest_key = latest[1]
    latest_dir = latest_key.rsplit("/", 1)[0] + "/"
    return latest_dir


def _write_metrics_json(bucket: str, metrics_prefix: str, metrics: dict):
    if metrics_prefix and not metrics_prefix.endswith("/"):
        metrics_prefix += "/"
    key = f"{metrics_prefix}_METRICS.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(metrics, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def _put_governance_metrics(namespace: str, metrics: dict):
    if not namespace:
        return

    metric_data = []
    for name, value in metrics.items():
        if isinstance(value, (int, float)):
            metric_data.append({
                "MetricName": name,
                "Value": float(value),
                "Unit": "None"
            })

    if metric_data:
        cw.put_metric_data(Namespace=namespace, MetricData=metric_data)


def _normalize_master_snapshot(zones_df):
    """
    Make master snapshot compatible with trip join.
    Accepts location_id OR locationid (and common variants).
    Produces: locationid, borough, zone, service_zone
    """
    # lowercase all columns first
    for c in zones_df.columns:
        zones_df = zones_df.withColumnRenamed(c, c.lower())

    # Map variants -> canonical
    # Your data has: location_id
    variants = {
        "locationid": ["locationid", "location_id", "locationid ", "location_id "],
        "borough": ["borough"],
        "zone": ["zone"],
        "service_zone": ["service_zone", "servicezone", "service zone"]
    }

    # helper to find the first matching column that exists
    def pick_col(candidates):
        for col in candidates:
            if col in zones_df.columns:
                return col
        return None

    loc_col = pick_col(variants["locationid"])
    bor_col = pick_col(variants["borough"])
    zon_col = pick_col(variants["zone"])
    svc_col = pick_col(variants["service_zone"])

    missing = []
    if not loc_col: missing.append("locationid/location_id")
    if not bor_col: missing.append("borough")
    if not zon_col: missing.append("zone")
    if not svc_col: missing.append("service_zone")

    if missing:
        raise Exception(f"Master snapshot missing columns {missing}. Found: {zones_df.columns}")

    # rename picked columns into canonical output names
    if loc_col != "locationid":
        zones_df = zones_df.withColumnRenamed(loc_col, "locationid")
    if bor_col != "borough":
        zones_df = zones_df.withColumnRenamed(bor_col, "borough")
    if zon_col != "zone":
        zones_df = zones_df.withColumnRenamed(zon_col, "zone")
    if svc_col != "service_zone":
        zones_df = zones_df.withColumnRenamed(svc_col, "service_zone")

    # keep only what we need (drops your extra governance cols like golden_sk, effective_from, etc.)
    zones_df = zones_df.select("locationid", "borough", "zone", "service_zone")

    # enforce type (join keys must match trip types)
    zones_df = zones_df.withColumn("locationid", F.col("locationid").cast("int"))

    return zones_df


# ----------------------------
# Main
# ----------------------------
# NOTE: include GOV_METRICS_NAMESPACE safely (optional)
argv = sys.argv
base_args = [
    "JOB_NAME",
    "bucket",
    "validated_trips_prefix",
    "snapshot_prefix",
    "curated_trips_prefix",
    "metrics_prefix",
    "run_id"
]
if "--GOV_METRICS_NAMESPACE" in argv:
    base_args.append("GOV_METRICS_NAMESPACE")

args = getResolvedOptions(argv, base_args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]

validated_base = args["validated_trips_prefix"].strip("/") + "/"
snapshot_base  = args["snapshot_prefix"].strip("/") + "/"
curated_base   = args["curated_trips_prefix"].strip("/") + "/"
metrics_prefix = args["metrics_prefix"].strip("/") + "/"
run_id         = args["run_id"]

# 1) Find latest validated run folder + latest snapshot folder
latest_validated_prefix = _latest_prefix_by_last_modified(bucket, validated_base)
latest_snapshot_prefix  = _latest_prefix_by_last_modified(bucket, snapshot_base)

validated_path = f"s3://{bucket}/{latest_validated_prefix}"
snapshot_path  = f"s3://{bucket}/{latest_snapshot_prefix}"

print("Latest validated:", validated_path)
print("Latest snapshot :", snapshot_path)

# 2) Read validated trips
trips = spark.read.parquet(validated_path)

# Ensure join keys are int (yellow taxi usually int)
trips = trips.withColumn("pulocationid", F.col("pulocationid").cast("int")) \
             .withColumn("dolocationid", F.col("dolocationid").cast("int"))

# 3) Read master snapshot (parquet expected)
zones_raw = spark.read.parquet(snapshot_path)

# 4) Normalize master snapshot schema
zones = _normalize_master_snapshot(zones_raw)

# 5) Join for PU
pu = zones.select(
    F.col("locationid").alias("pulocationid"),
    F.col("borough").alias("pu_borough"),
    F.col("zone").alias("pu_zone"),
    F.col("service_zone").alias("pu_servicezone"),
)

# 6) Join for DO
do = zones.select(
    F.col("locationid").alias("dolocationid"),
    F.col("borough").alias("do_borough"),
    F.col("zone").alias("do_zone"),
    F.col("service_zone").alias("do_servicezone"),
)

enriched = (trips
    .join(broadcast(pu), on="pulocationid", how="left")
    .join(broadcast(do), on="dolocationid", how="left")
)

# 7) Output curated
curated_out = f"s3://{bucket}/{curated_base}run_id={run_id}/"
(enriched.write.mode("overwrite").parquet(curated_out))

# 8) Metrics (single fixed file, no run_id in metrics path)
total_rows = enriched.count()
pu_rate = enriched.filter(F.col("pu_zone").isNotNull()).count() / total_rows if total_rows else 0.0
do_rate = enriched.filter(F.col("do_zone").isNotNull()).count() / total_rows if total_rows else 0.0

metrics = {
    "total_rows": total_rows,
    "pu_zone_nonnull_rate": round(pu_rate, 4),
    "do_zone_nonnull_rate": round(do_rate, 4),
    "validated_read_path": validated_path,
    "snapshot_read_path": snapshot_path,
    "curated_write_path": curated_out,
    "generated_utc": datetime.now(timezone.utc).isoformat(),
}

metrics_s3 = _write_metrics_json(bucket, metrics_prefix, metrics)
print("Wrote metrics:", metrics_s3)

# 9) Governance CloudWatch metrics (optional)
namespace = args.get("GOV_METRICS_NAMESPACE", "")
_put_governance_metrics(namespace, {
    "TotalRows": total_rows,
    "PUZoneNonNullRate": pu_rate,
    "DOZoneNonNullRate": do_rate
})

print("SUCCESS - curated:", curated_out)
job.commit()
