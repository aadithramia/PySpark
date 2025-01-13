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
	.config("spark.sql.shuffle.partitions", "2") \
	.getOrCreate()


StreamX_schema = StructType([    
	StructField("ImpTime", TimestampType()),
	StructField("ImpId", StringType()),
	StructField("ValueA", StringType())
])

StreamX = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9092") \
	.option("subscribe", "StreamX") \
	.option("startingOffsets", "earliest") \
	.option("maxOffsetsPerTrigger", 1) \
	.load() 
	

StreamX = StreamX.select(from_json(col("value").cast("string"), StreamX_schema).alias("data")) \
	.select("data.*") \
	.withWatermark("ImpTime", "30 minutes")

#StreamX.printSchema()

StreamY_schema = StructType([    
	StructField("ClickTime", TimestampType()),
	StructField("ClickId", StringType()),
	StructField("ValueB", StringType())
])

StreamY = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9092") \
	.option("subscribe", "StreamY") \
	.option("startingOffsets", "earliest") \
	.option("maxOffsetsPerTrigger", 1) \
	.load() 

StreamY = StreamY.select(from_json(col("value").cast("string"), StreamY_schema).alias("data")) \
	.select("data.*") \
	.withWatermark("ClickTime", "30 minutes")

#StreamX = StreamX.withColumnRenamed("EventTime", "EventTimeX")
#StreamY = StreamY.withColumnRenamed("EventTime", "EventTimeY")

# Define join expression and join type
#join_expr = (StreamX["Key"] == StreamY["Key"]) #& (StreamX["EventTimeX"] <= StreamY["EventTimeY"] + expr("INTERVAL 1 HOUR"))
join_expr = (StreamX["ImpId"] == StreamY["ClickId"]) & (StreamX["ImpTime"].between(StreamY["ClickTime"], StreamY["ClickTime"] + expr("INTERVAL 30 MINUTES")))
join_type = "leftOuter"

# Perform join
joined_df = StreamX.join(StreamY, join_expr, join_type)
	

query = joined_df.writeStream \
	.outputMode("append") \
	.format("console") \
	.option("truncate", "false") \
	.option("checkpointLocation", "C:/Users/shravanr/learning/spark/pyspark/StructuredStreaming/Joins/data/checkpoint-dir") \
	.trigger(processingTime='10 seconds') \
	.start() \
	.awaitTermination()


