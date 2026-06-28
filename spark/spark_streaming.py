import os
import sys


os.environ["HADOOP_HOME"] = r"F:\hadoop"
sys.path.append(r"F:\hadoop\bin")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 1. Initialize Spark Session with Kafka Integration Package
spark = SparkSession.builder \
    .appName("KafkaSparkLocalStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read Stream from Local Kafka Topic
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer_chats") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Convert Kafka Value (Bytes) to Readable String
clean_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as message_payload")

# 4. Write Stream Result Live to your Console/Terminal
query = clean_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("⚡ Local Spark Streaming is running and waiting for Kafka events...")
query.awaitTermination()