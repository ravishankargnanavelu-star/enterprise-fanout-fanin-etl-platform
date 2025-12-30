import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -------------------------------------------------
# Job Arguments (Passed from Step Functions)
# -------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_PATH",
        "CURATED_PATH",
        "PROCESS_DATE"
    ]
)

RAW_PATH = args["RAW_PATH"]
CURATED_PATH = args["CURATED_PATH"]
PROCESS_DATE = args["PROCESS_DATE"]

# -------------------------------------------------
# Spark & Glue Context
# -------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------------------------------
# Spark Optimizations (Enterprise Standard)
# -------------------------------------------------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.parquet.mergeSchema", "true")

# =================================================
# BRONZE LAYER – Raw Ingestion
# =================================================
bronze_df = (
    spark.read
         .option("mergeSchema", "true")
         .parquet(f"{RAW_PATH}/sales/")
         .withColumn("ingestion_ts", current_timestamp())
)

# =================================================
# SILVER LAYER – Cleansing & Business Logic
# =================================================
window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())

silver_df = (
    bronze_df
    # Validation
    .filter(col("order_id").isNotNull())
    .filter(col("amount").isNotNull())

    # Deduplication (latest record wins)
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")

    # Business rules
    .withColumn(
        "order_status",
        when(col("amount") > 0, "VALID").otherwise("INVALID")
    )

    # Enrichment
    .withColumn("processed_ts", current_timestamp())
)

# =================================================
# GOLD LAYER – Analytics / Cold Layer
# =================================================
gold_df = (
    silver_df
    .groupBy("order_status")
    .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        max("processed_ts").alias("last_updated_ts")
    )
    .withColumn("process_date", lit(PROCESS_DATE))
)

# -------------------------------------------------
# Write GOLD Data (Partitioned, Analytics Ready)
# -------------------------------------------------
(
    gold_df.write
        .mode("overwrite")
        .partitionBy("process_date")
        .format("parquet")
        .save(f"{CURATED_PATH}/sales/")
)
