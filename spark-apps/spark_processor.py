from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

# --- ❄️ SNOWFLAKE CONFIGURATION (FILL THESE IN) ---
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
SNOWFLAKE_OPTIONS = {
    "sfURL": "VFNFKPJ-BAC71951.snowflakecomputing.com",        # e.g. bac71951.us-east-1.snowflakecomputing.com
    "sfUser": "TARUN",          # Your Snowflake Login Name
    "sfPassword": "TarunSnowflake123",      # Your Snowflake Password
    "sfDatabase": "SHOPMETRIC_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "SHOP_WH"
}

# --- 📦 AWS S3 CONFIGURATION (PAUSED) ---
# aws_access_key = "PASTE_ACCESS_KEY_HERE"
# aws_secret_key = "PASTE_SECRET_KEY_HERE"
# aws_bucket = "shopmetric-datalake-tarun"

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("ShopMetric-Hybrid-Master") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
    # .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \  <-- UNCOMMENT LATER
    # .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \  <-- UNCOMMENT LATER

spark.sparkContext.setLogLevel("WARN")

# 2. Schema
schema = StructType() \
    .add("order_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("quantity", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("country", StringType()) \
    .add("status", StringType()) \
    .add("timestamp", TimestampType())

# 3. Read Kafka (The Waiter)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "shop_orders") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse Data
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# --- PATH 1: AWS S3 (The Basement) - PAUSED ⏸️ ---
# s3_query = parsed_df.writeStream \
#     .format("json") \
#     .option("path", f"s3a://{aws_bucket}/shopmetric_raw/") \
#     .option("checkpointLocation", f"s3a://{aws_bucket}/checkpoints_s3/") \
#     .outputMode("append") \
#     .start()

# --- PATH 2: SNOWFLAKE (The Display Case) - ACTIVE ▶️ ---
filtered_df = parsed_df.filter(col("status") == "COMPLETED") # Only show the good pizza

def write_to_snowflake(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"❄️ Writing Batch {batch_id} to Snowflake...")
        batch_df.write \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", "ORDERS") \
            .mode("append") \
            .save()
    else:
        print(f"Skipping empty batch {batch_id}")

snowflake_query = filtered_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .start()

print("🚀 ShopMetric Hybrid Stream Started... (S3 Paused, Snowflake Active)")
spark.streams.awaitAnyTermination()