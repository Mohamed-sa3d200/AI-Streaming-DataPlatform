import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Setup Hadoop Environment Path for Windows (Ensure this directory contains bin/winutils.exe and bin/hadoop.dll)
os.environ["HADOOP_HOME"] = r"F:\hadoop"
sys.path.append(r"F:\Hadoop\bin")

def main():
    print("🚀 Starting Local Spark Engine and resolving packages...")
    
    # 2. Initialize Spark Session with Kafka and Delta Lake packages
    spark = SparkSession.builder \
        .appName("LocalCustomerChatsStreaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.5.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Set log level to WARN to reduce unnecessary console noise
    spark.sparkContext.setLogLevel("WARN") 

    # 3. [Phase 4] Connect to Local Kafka and Read Stream
    print("📥 Connecting to local Kafka bootstrap server...")
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "customer_chats") \
        .option("startingOffsets", "latest") \
        .load()

    # Convert binary Kafka value payload to a readable string
    json_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data")

    # Define the expected schema for incoming JSON payloads
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("event_time", StringType(), True)
    ])

    parsed_df = json_stream_df \
        .withColumn("parsed_data", from_json(col("json_data"), schema)) \
        .select("parsed_data.*")

    # 4. [Phase 5] Apply Data Quality Rules (Validation)
    valid_condition = (
        col("user_id").isNotNull() & 
        (col("message") != "") & 
        (col("rating") >= 1) & 
        (col("rating") <= 5)
    )

    valid_df = parsed_df.filter(valid_condition)
    invalid_df = parsed_df.filter(~valid_condition)

    # 5. [Phase 6] Format Dead Letter Queue (DLQ) and tag errors
    dlq_df = invalid_df.withColumn(
        "error_reason",
        when(col("user_id").isNull(), "Missing User ID")
        .when(col("message") == "", "Empty Message Text")
        .when((col("rating") < 1) | (col("rating") > 5), "Rating Out of Range")
        .otherwise("Unknown Validation Failure")
    )

    # 6. [Phase 7] Initialize Local Streaming Sinks and Write Data
    print("💾 Initializing local sinks. Writing streaming data to storage...")

    # Write clean data to Delta Lake table (Silver Layer)
    silver_query = valid_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "D:/project_data/checkpoints/silver") \
        .start("D:/project_data/lakehouse/silver_customer_chats")

    # Write corrupted data to Parquet files (Dead Letter Queue)
    dlq_query = dlq_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", "D:/project_data/checkpoints/dlq") \
        .start("D:/project_data/lakehouse/bad_records_dlq")

    # Keep the script active and monitor streaming pipelines
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()