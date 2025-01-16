import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreateDF") \
	.config("spark.sql.shuffle.partitions", 1) \
	.getOrCreate()


A = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
A = spark.createDataFrame(A, ["ID", "Name"])


B = [("X", 100), ("Y", 200)]
B = spark.createDataFrame(B, ["Code","Value"])


#cross join - gives the cartesian product of A and B; no join condition
A.crossJoin(B).show()
