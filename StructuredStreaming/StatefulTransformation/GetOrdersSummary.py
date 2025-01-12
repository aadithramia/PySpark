import os
os.environ['SPARK_HOME'] = "C:/install/spark-3.1.2-bin-hadoop3.2"

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Reads json source and writes to json sink



spark = SparkSession.builder.appName("GetOrdersSummary")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

schema = StructType([
    StructField("TransactionDateTime", TimestampType()),
    StructField("Symbol", StringType()),
    StructField("OrderType", StringType()),
    StructField("Qty", IntegerType())
])

trades = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Orders") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1) \
    .load()

trades = trades.select(from_json(col("value").cast("string"), schema).alias("json")) \
    .select("json.*")

trades = trades.withColumn("BUY", expr("case when OrderType = 'BUY' then Qty else 0 end")) \
    .withColumn("SELL", expr("case when OrderType = 'SELL' then Qty else 0 end")) \
    .drop("OrderType", "Qty")

# +-------------------+------+---+----+
# |TransactionDateTime|Symbol|BUY|SELL|
# +-------------------+------+---+----+
# |2025-01-01 09:00:00|  AAPL| 50|   0|
# +-------------------+------+---+----+

#defining tumbling window of 15 minutes on TransactionDateTime and grouping by Symbol
tradesSummary = trades \
    .withWatermark("TransactionDateTime", "1 hour") \
    .groupBy(window(col("TransactionDateTime"), "15 minutes"), "Symbol") \
    .agg(sum("BUY").alias("Buys"), sum("SELL").alias("Sells")) \
    .select("window.start", "window.end", "Symbol", "Buys", "Sells")

tradesSummary = tradesSummary.withColumn("NetQty", expr("Buys - Sells"))
#data.printSchema()
tradesSummary.writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint-dir") \
        .outputMode("append") \
        .start() \
        .awaitTermination()

