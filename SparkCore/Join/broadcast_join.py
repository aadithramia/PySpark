import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("static_static_join")\
	.master("local[3]") \
	.config("spark.streaming.stopGracefullyOnShutdown", "true") \
	.config("spark.sql.shuffle.partitions", 3) \
	.getOrCreate()

A = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .csv("C:/Users/shravanr/learning/spark/pyspark/SparkCore/Join/data/a.csv") 

A = A.repartition(3)


B = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .csv("C:/Users/shravanr/learning/spark/pyspark/SparkCore/Join/data/b.csv") 


joined = A.join(broadcast(B), A.ID == B.ID, "inner")
joined.foreach(lambda x: None)

r = input("press any key to exit")