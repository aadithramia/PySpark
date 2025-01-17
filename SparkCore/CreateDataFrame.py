import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CreateDF") \
	.config("spark.sql.shuffle.partitions", 1) \
	.getOrCreate()


rows = [Row(1, "Alice", 12), Row(2, "Bob", 23), Row(3, "Cathy",67), Row(4, "Bob",45), Row(5, "Elena", None)]
rdd = spark.sparkContext.parallelize(rows, 2) # creating rdd with 2 partitions
df = spark.createDataFrame(rdd, ["ID","Name", "Age"])
df.show()
