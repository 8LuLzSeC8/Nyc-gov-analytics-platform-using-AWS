import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bucket",
    "raw_trips_prefix",
    "validated_trips_prefix",
    "run_id",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["bucket"]
raw_prefix = args["raw_trips_prefix"].rstrip("/") + "/"
validated_prefix = args["validated_trips_prefix"].rstrip("/") + "/"
run_id = args["run_id"]

raw_path = f"s3://{bucket}/{raw_prefix}"
validated_out = f"s3://{bucket}/{validated_prefix}run_id={run_id}/"
quarantine_out = f"s3://{bucket}/validated/quarantine/run_id={run_id}/"

# ----------------------------
# 1) Read raw parquet
# ----------------------------
df = spark.read.parquet(raw_path)

# ----------------------------
# 2) Standardize / cast columns (keep IDs!)
# ----------------------------
# NYC TLC schema fields can vary slightly by month/version.
# We'll only cast the ones that exist.
casts = {
    "VendorID": T.IntegerType(),
    "RatecodeID": T.IntegerType(),
    "PULocationID": T.IntegerType(),
    "DOLocationID": T.IntegerType(),
    "passenger_count": T.IntegerType(),
    "trip_distance": T.DoubleType(),
    "payment_type": T.IntegerType(),
    "fare_amount": T.DoubleType(),
    "extra": T.DoubleType(),
    "mta_tax": T.DoubleType(),
    "tip_amount": T.DoubleType(),
    "tolls_amount": T.DoubleType(),
    "improvement_surcharge": T.DoubleType(),
    "total_amount": T.DoubleType(),
    "congestion_surcharge": T.DoubleType(),
    "airport_fee": T.DoubleType(),
    "Airport_fee": T.DoubleType(),
    "cbd_congestion_fee": T.DoubleType(),
}

for col, typ in casts.items():
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(typ))

# timestamps (these usually exist)
if "tpep_pickup_datetime" in df.columns:
    df = df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
if "tpep_dropoff_datetime" in df.columns:
    df = df.withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))

# ----------------------------
# 3) Null/validity checks (minimal, practical)
# ----------------------------
required_cols = []
for c in ["PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"]:
    if c in df.columns:
        required_cols.append(c)

# Build "bad_reason" string (collect reasons)
bad_reason = F.lit("")

def add_reason(cond, msg):
    return F.when(cond, F.concat_ws("|", bad_reason, F.lit(msg))).otherwise(bad_reason)

# start with empty string
bad_reason = F.lit("")

# Required null checks
if "PULocationID" in df.columns:
    bad_reason = F.when(F.col("PULocationID").isNull(), F.concat_ws("|", bad_reason, F.lit("PULocationID_NULL"))).otherwise(bad_reason)
if "DOLocationID" in df.columns:
    bad_reason = F.when(F.col("DOLocationID").isNull(), F.concat_ws("|", bad_reason, F.lit("DOLocationID_NULL"))).otherwise(bad_reason)

if "tpep_pickup_datetime" in df.columns:
    bad_reason = F.when(F.col("tpep_pickup_datetime").isNull(), F.concat_ws("|", bad_reason, F.lit("PICKUP_TS_NULL"))).otherwise(bad_reason)
if "tpep_dropoff_datetime" in df.columns:
    bad_reason = F.when(F.col("tpep_dropoff_datetime").isNull(), F.concat_ws("|", bad_reason, F.lit("DROPOFF_TS_NULL"))).otherwise(bad_reason)

# Basic sanity checks
if "trip_distance" in df.columns:
    bad_reason = F.when(F.col("trip_distance").isNull(), F.concat_ws("|", bad_reason, F.lit("TRIP_DISTANCE_NULL"))) \
                  .when(F.col("trip_distance") < 0, F.concat_ws("|", bad_reason, F.lit("TRIP_DISTANCE_NEG"))) \
                  .otherwise(bad_reason)

if "total_amount" in df.columns:
    bad_reason = F.when(F.col("total_amount").isNull(), F.concat_ws("|", bad_reason, F.lit("TOTAL_AMOUNT_NULL"))) \
                  .otherwise(bad_reason)

# pickup <= dropoff
if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
    bad_reason = F.when(
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull() &
        (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")),
        F.concat_ws("|", bad_reason, F.lit("DROPOFF_BEFORE_PICKUP"))
    ).otherwise(bad_reason)

df2 = df.withColumn("bad_reason", F.regexp_replace(bad_reason, r"^\|+", ""))

good_df = df2.filter(F.col("bad_reason") == "")
bad_df  = df2.filter(F.col("bad_reason") != "")

# Add governance-ish columns (handy later)
ingested_at = datetime.now(timezone.utc).isoformat()
good_df = good_df.withColumn("run_id", F.lit(run_id)).withColumn("ingested_at_utc", F.lit(ingested_at))
bad_df  = bad_df.withColumn("run_id", F.lit(run_id)).withColumn("ingested_at_utc", F.lit(ingested_at))

# ----------------------------
# 4) Write outputs
# ----------------------------
# Good rows → validated
(good_df.drop("bad_reason")
 .write.mode("overwrite")
 .parquet(validated_out)
)

# Bad rows → quarantine (keep bad_reason)
(bad_df
 .write.mode("overwrite")
 .parquet(quarantine_out)
)

print(f"RAW PATH:        {raw_path}")
print(f"VALIDATED OUT:   {validated_out}")
print(f"QUARANTINE OUT:  {quarantine_out}")
print(f"GOOD ROWS: {good_df.count()}")
print(f"BAD ROWS:  {bad_df.count()}")

job.commit()
