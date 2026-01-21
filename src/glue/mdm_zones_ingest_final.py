import sys
import boto3,json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "s3_input_csv",
    "pg_jdbc_url",
    "pg_secret_id",
    "db_name",
    "batch_id",
    "auto_merge_threshold",
    "steward_min_threshold",
])

secret_id = args["pg_secret_id"]

sm = boto3.client("secretsmanager", region_name="us-east-2")
secret_val = sm.get_secret_value(SecretId=secret_id)
creds = json.loads(secret_val["SecretString"])

PG_USER = creds.get("username") or creds.get("user")
PG_PW   = creds.get("password")
if not PG_USER or not PG_PW:
    raise Exception(f"Secret {secret_id} missing username/user or password")

S3_INPUT = args["s3_input_csv"]
PG_URL = args["pg_jdbc_url"]
DB_NAME = args["db_name"]
BATCH_ID = args["batch_id"]
AUTO_T = int(args["auto_merge_threshold"])
STEWARD_T = int(args["steward_min_threshold"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def norm_col(c):
    return F.upper(F.trim(F.regexp_replace(F.col(c), r"[^A-Za-z0-9 ]", " ")))

# ---------- Read CSV and normalize headers ----------
raw = spark.read.option("header", "true").option("inferSchema", "false").csv(S3_INPUT)

# normalize headers to lowercase for safety
for c in raw.columns:
    raw = raw.withColumnRenamed(c, c.strip().lower())

# Expected columns after lowercasing: locationid, borough, zone, service_zone
required = ["locationid", "borough", "zone", "service_zone"]
missing_cols = [c for c in required if c not in raw.columns]
if missing_cols:
    raise Exception(f"Missing columns in CSV: {missing_cols}. Found: {raw.columns}")

df = (
    raw.select(
        F.col("locationid").cast("int").alias("location_id"),
        F.col("borough").cast("string").alias("borough"),
        F.col("zone").cast("string").alias("zone"),
        F.col("service_zone").cast("string").alias("service_zone"),
    )
    .na.drop(subset=["location_id", "borough", "zone"])
)

# ---------- Quality gate: unique location_id ----------
dup_cnt = (
    df.groupBy("location_id").count()
      .filter(F.col("count") > 1)
      .count()
)
if dup_cnt > 0:
    raise Exception(f"Data quality failed: duplicate LocationID found: {dup_cnt}")

# ---------- Normalize for matching ----------
dfn = (
    df.withColumn("borough_norm", norm_col("borough"))
      .withColumn("zone_norm", norm_col("zone"))
      .withColumn("zone_norm", F.regexp_replace(F.col("zone_norm"), r"\s+", " "))
)

a = dfn.alias("a")
b = dfn.alias("b")

pairs = (
    a.join(
        b,
        (F.col("a.borough_norm") == F.col("b.borough_norm")) &
        (F.col("a.location_id") < F.col("b.location_id")),
        "inner"
    )
    .select(
        F.col("a.location_id").alias("location_id_1"),
        F.col("b.location_id").alias("location_id_2"),
        F.col("a.borough").alias("borough"),
        F.col("a.zone").alias("zone_1"),
        F.col("b.zone").alias("zone_2"),
        F.col("a.zone_norm").alias("zone_1_norm"),
        F.col("b.zone_norm").alias("zone_2_norm"),
    )
)

pairs = (
    pairs.withColumn("max_len", F.greatest(F.length("zone_1_norm"), F.length("zone_2_norm")))
         .withColumn("dist", F.levenshtein("zone_1_norm", "zone_2_norm"))
         .withColumn(
             "score",
             F.when(F.col("max_len") == 0, F.lit(0))
              .otherwise(F.round((F.lit(1) - (F.col("dist") / F.col("max_len"))) * 100).cast("int"))
         )
         .drop("max_len", "dist", "zone_1_norm", "zone_2_norm")
)

pairs = pairs.filter(F.col("score") >= F.lit(STEWARD_T))

classified = (
    pairs.withColumn(
            "action",
            F.when(F.col("score") >= F.lit(AUTO_T), F.lit("AUTO_MERGE"))
             .otherwise(F.lit("STEWARD_REVIEW"))
    )
    .withColumn(
            "confidence_tier",
            F.when(F.col("score") >= F.lit(AUTO_T), F.lit("HIGH"))
             .otherwise(F.lit("MEDIUM"))
    )
    .withColumn(
            "recommended_golden_id",
            F.when(F.length("zone_1") > F.length("zone_2"), F.col("location_id_1"))
             .when(F.length("zone_2") > F.length("zone_1"), F.col("location_id_2"))
             .otherwise(F.least(F.col("location_id_1"), F.col("location_id_2")))
    )
    .withColumn("batch_id", F.lit(BATCH_ID))
)

pending_ids = (
    classified.filter(F.col("action") == "STEWARD_REVIEW")
    .select(F.col("location_id_1").alias("location_id"))
    .union(classified.filter(F.col("action") == "STEWARD_REVIEW")
           .select(F.col("location_id_2").alias("location_id")))
    .distinct()
    .withColumn("pending", F.lit(True))
)

records = (
    df.select("location_id", "borough", "zone", "service_zone")
      .withColumn("batch_id", F.lit(BATCH_ID))
      .withColumn("status", F.lit("APPROVED"))
      .withColumn("source_file", F.lit(S3_INPUT))
)

records = (
    records.join(pending_ids, on="location_id", how="left")
           .withColumn("status", F.when(F.col("pending") == True, F.lit("PENDING")).otherwise(F.col("status")))
           .drop("pending")
)

matches_out = classified.select(
    "batch_id",
    "location_id_1",
    "location_id_2",
    "borough",
    "zone_1",
    "zone_2",
    "score",
    "confidence_tier",
    "action",
    "recommended_golden_id",
)

# ---------- Write to RDS (idempotent per batch_id) ----------
pre_rec = f"DELETE FROM mdm_zone_record_v2 WHERE batch_id = '{BATCH_ID}';"
pre_match = f"DELETE FROM mdm_zone_match_v2 WHERE batch_id = '{BATCH_ID}';"

rec_dyf = DynamicFrame.fromDF(records, glueContext, "rec_dyf")
match_dyf = DynamicFrame.fromDF(matches_out, glueContext, "match_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=rec_dyf,
    connection_type="postgresql",
    connection_options={
        "url": PG_URL,
        "user": PG_USER,
        "password": PG_PW,
        "dbtable": "mdm_zone_record_v2",
        "database": DB_NAME,
        "preactions": pre_rec,
    }
)

glueContext.write_dynamic_frame.from_options(
    frame=match_dyf,
    connection_type="postgresql",
    connection_options={
        "url": PG_URL,
        "user": PG_USER,
        "password": PG_PW,
        "dbtable": "mdm_zone_match_v2",
        "database": DB_NAME,
        "preactions": pre_match,
    }
)

job.commit()