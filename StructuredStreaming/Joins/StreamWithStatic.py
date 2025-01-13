import os
os.environ['SPARK_HOME'] = "C:/install/spark-3.1.2-bin-hadoop3.2"

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("stream_join_with_staticdata")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

#reading tsv
#this gets reloaded for every microbatch
UserDataSnapshot = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .csv("C:/Users/shravanr/learning/spark/pyspark/StructuredStreaming/Joins/data/input/StaticStream.csv") \
    

schema = StructType([
    StructField("UserId", StringType()),
    StructField("LoginTime", TimestampType())
])

LoginStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Topic1") \
    .option("startingOffsets", "earliest") \
    .load() \

LoginStream = LoginStream.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# query = LoginStream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", "C:/Users/shravanr/learning/spark/pyspark/StructuredStreaming/Joins/data/checkpoint-dir") \
#     .start() \
#     .awaitTermination()

join_expr = LoginStream["UserId"] == UserDataSnapshot["UserId"]

join_type = "inner"

# streaming side has to be non-nullable
join_type = "left_outer"

#since this is inner join, if record from kafka doesnt have a match from sttic stream, it will be dropped
#howver, consider a scenario wherein a new user just got created and we got login info for the new user. 
# this will be okay because the static stream will get reloaded for every microbatch. in real world, this will ideally be a db table
joined_df = LoginStream.join(UserDataSnapshot, join_expr, join_type) \
    .drop(UserDataSnapshot.UserId)

joined_df = joined_df.select("UserId", "UserName", "LoginTime") \
    .withColumnRenamed("LoginTime", "LastLoginTime")

query = joined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "C:/Users/shravanr/learning/spark/pyspark/StructuredStreaming/Joins/data/checkpoint-dir") \
    .start() \
    .awaitTermination()