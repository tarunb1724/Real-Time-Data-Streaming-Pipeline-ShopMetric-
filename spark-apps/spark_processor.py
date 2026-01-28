from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

# 1. Initialize Spark (with the JARs you downloaded)
# 1. Initialize Spark
# We removed the .config("spark.jars", ...) line because we pass them in the command now!
spark = SparkSession.builder \
    .appName("ShopMetric-Debug") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# 2. Define Schema (Must match your Generator!)
schema = StructType() \
    .add("order_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("quantity", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("country", StringType()) \
    .add("status", StringType()) \
    .add("timestamp", TimestampType())

# 3. Read Stream from Kafka
print("⏳ Connecting to Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092")  \
    .option("subscribe", "shop_orders") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. TRANSFORMATION: Filter out CANCELLED orders
clean_df = parsed_df.filter(col("status") == "COMPLETED")

# 6. Output to Console (Debug)
query = clean_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()