import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CreateDF") \
	.config("spark.sql.shuffle.partitions", 1) \
	.getOrCreate()

print("A")
A = [(1, "Alice", 12), (2, "Bob", 23), (3, "Cathy",67), (4, "Bob",45), (5, "Elena", None)]
A = spark.createDataFrame(A, ["ID","Name", "Age"])
A.show()

#count("col") gives count of non-null values in the column
#count("*") gives count of rows
#countDistinct("col") gives count of distinct values in the column
A.select(min("Age").alias("min_age"), max("Age"), avg("Age"), count("*"),count("Age"), countDistinct("Name")).show()