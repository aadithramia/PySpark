import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Reads json source and writes to json sink

spark = SparkSession.builder.appName("kafka_to_console")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Topic1") \
    .option("startingOffsets", "earliest") \
    .load()

data.printSchema()

values = data.select(col("value").cast("string"))

query = values.writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint-dir") \
        .outputMode("update") \
        .start() \
        .awaitTermination()
