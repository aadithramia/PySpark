import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("network_read")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

data = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

#outputMode cannot be complete here. complete works only with aggregations. otherwise it throws AnalysisException
query = data.writeStream \
    .format("console") \
    .option("checkpointLocation", "network_read_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()