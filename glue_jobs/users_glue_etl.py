import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_PATH", "CURATED_PATH", "PROCESS_DATE"]
)

RAW_PATH = args["RAW_PATH"]
CURATED_PATH = args["CURATED_PATH"]
PROCESS_DATE = args["PROCESS_DATE"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.adaptive.enabled", "true")

# ---------------- BRONZE ----------------
bronze_df = (
    spark.read
         .option("mergeSchema", "true")
         .parquet(f"{RAW_PATH}/users/")
         .withColumn("ingestion_ts", current_timestamp())
)

# ---------------- SILVER ----------------
window_spec = Window.partitionBy("user_id").orderBy(col("ingestion_ts").desc())

silver_df = (
    bronze_df
    .filter(col("user_id").isNotNull())
    .withColumn("email", lower(col("email")))
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn(
        "user_status",
        when(col("is_active") == True, "ACTIVE").otherwise("INACTIVE")
    )
    .withColumn("processed_ts", current_timestamp())
)

# ---------------- GOLD ----------------
gold_df = (
    silver_df
    .groupBy("user_status")
    .agg(
        count("*").alias("total_users"),
        max("processed_ts").alias("last_updated_ts")
    )
    .withColumn("process_date", lit(PROCESS_DATE))
)

gold_df.write \
    .mode("overwrite") \
    .partitionBy("process_date") \
    .parquet(f"{CURATED_PATH}/users/")
