import sys
import boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "pg_jdbc_url",
    "pg_secret_id",
    "db_name",
    "snapshot_id",
    "s3_validated_base",
    "s3_validated_base2",
    "s3_validated_base3",
])


secret_id = args["pg_secret_id"]

sm = boto3.client("secretsmanager", region_name="us-east-2")
secret_val = sm.get_secret_value(SecretId=secret_id)
creds = json.loads(secret_val["SecretString"])

PG_USER = creds.get("username") or creds.get("user")
PG_PW   = creds.get("password")
if not PG_USER or not PG_PW:
    raise Exception(f"Secret {secret_id} missing username/user or password")



PG_URL = args["pg_jdbc_url"]
DB_NAME = args["db_name"]
SNAPSHOT_ID = args["snapshot_id"]
S3_BASE  = args["s3_validated_base"].rstrip("/")
S3_BASE2 = args["s3_validated_base2"].rstrip("/")
S3_BASE3 = args["s3_validated_base3"].rstrip("/")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

out_path_zone   = f"{S3_BASE}/{SNAPSHOT_ID}/parquet"
out_path_vendor = f"{S3_BASE2}/{SNAPSHOT_ID}/vendor"
out_path_rate   = f"{S3_BASE3}/{SNAPSHOT_ID}/ratecode"

zone_df = (
    spark.read.format("jdbc")
      .option("url", PG_URL)
      .option("dbtable", "v_mdm_zone_golden_all_v2")
      .option("user", PG_USER)
      .option("password", PG_PW)
      .option("driver", "org.postgresql.Driver")
      .load()
      .withColumn("snapshot_id", F.lit(SNAPSHOT_ID))
      .withColumn("snapshot_ts", F.current_timestamp())
)

if zone_df.count() == 0:
    raise Exception("Zone snapshot is empty")

zone_df.write.mode("overwrite").parquet(out_path_zone)


vendor_df = (
    spark.read.format("jdbc")
      .option("url", PG_URL)
      .option("dbtable", "v_mdm_vendor_golden_all_v2")
      .option("user", PG_USER)
      .option("password", PG_PW)
      .option("driver", "org.postgresql.Driver")
      .load()
      .withColumn("snapshot_id", F.lit(SNAPSHOT_ID))
      .withColumn("snapshot_ts", F.current_timestamp())
)

if vendor_df.count() == 0:
    raise Exception("Vendor snapshot is empty")

vendor_df.write.mode("overwrite").parquet(out_path_vendor)

rate_df = (
    spark.read.format("jdbc")
      .option("url", PG_URL)
      .option("dbtable", "v_mdm_rate_code_golden_all_v2")
      .option("user", PG_USER)
      .option("password", PG_PW)
      .option("driver", "org.postgresql.Driver")
      .load()
      .withColumn("snapshot_id", F.lit(SNAPSHOT_ID))
      .withColumn("snapshot_ts", F.current_timestamp())
)

if rate_df.count() == 0:
    raise Exception("Rate code snapshot is empty")

rate_df.write.mode("overwrite").parquet(out_path_rate)

job.commit()
