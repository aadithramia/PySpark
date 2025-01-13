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


StreamA_schema = StructType([    
    StructField("EventTime", TimestampType()),
	StructField("Key", StringType()),
	StructField("ValueA", StringType())
])

StreamA = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "StreamA") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1) \
    .load() 
    

StreamA = StreamA.select(from_json(col("value").cast("string"), StreamA_schema).alias("data")) \
    .select("data.*") \
	.withWatermark("EventTime", "1 hour")

#StreamA.printSchema()

StreamB_schema = StructType([    
    StructField("EventTime", TimestampType()),
	StructField("Key", StringType()),
	StructField("ValueB", StringType())
])

StreamB = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "StreamB") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1) \
    .load() 

StreamB = StreamB.select(from_json(col("value").cast("string"), StreamB_schema).alias("data")) \
    .select("data.*") \
	.withWatermark("EventTime", "1 hour")

join_expr = StreamA["Key"] == StreamB["Key"]
join_type = "inner"
joined_df = StreamA.join(StreamB, join_expr, join_type) \
	.drop(StreamB.Key)

query = joined_df.writeStream \
	.outputMode("append") \
	.format("console") \
	.option("truncate", "false") \
	.option("checkpointLocation", "C:/Users/shravanr/learning/spark/pyspark/StructuredStreaming/Joins/data/checkpoint-dir") \
	.start() \
	.awaitTermination()


# +-------------------+---+------+------+
# Sample input for StreamA:
# {"EventTime":"2025-01-01T09:00:00","Key":"AAPL","ValueA":"50"}
# {"EventTime":"2025-01-01T09:00:00","Key":"MSFT","ValueA":"100"}

# Sample input for StreamB:
# {"EventTime":"2025-01-01T09:40:00","Key":"AAPL","ValueB":"100"}
# {"EventTime":"2025-01-01T10:50:00","Key":"MSFT","ValueB":"200"}

# Sample output:
# +-------------------+---+------+------+
# |EventTime          |Key|ValueA|ValueB|
# +-------------------+---+------+------+
# |2025-01-01 09:40:00|AAPL|50|100|
# |2025-01-01 19:30:00|MSFT|100|200|
# +-------------------+---+------+------+

# +-------------------+---+------+------+
# Sample input for StreamA:
# {"EventTime":"2025-01-01T09:00:00","Key":"AAPL","ValueA":"50"}
# {"EventTime":"2025-01-01T09:00:00","Key":"MSFT","ValueA":"100"}
# {"EventTime":"2025-01-01T10:10:00","Key":"MSFT","ValueA":"100"} -> this will trigger cleanup of 9 AM records from state store


# Sample input for StreamB:
# {"EventTime":"2025-01-01T09:40:00","Key":"AAPL","ValueB":"100"}
# {"EventTime":"2025-01-01T19:30:00","Key":"MSFT","ValueB":"200"}

#output depends on order of arrival of records in StreamA and StreamB
#if both of StreamB events appear after all the StreamA events, then the output will be:

