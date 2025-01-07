import findspark
findspark.init()
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("network_read")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3)\
        .getOrCreate()

lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

words = lines.select(expr("explode(split(value,' ')) as word"))
word_count = words.groupBy("word").count()

#outputMode cannot be complete here. complete works only with aggregations. otherwise it throws AnalysisException
query = word_count.writeStream \
    .format("console") \
    .option("checkpointLocation", "network_wordcount_checkpoint") \
    .outputMode("complete") \
    .start()

query.awaitTermination()