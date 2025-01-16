import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreateDF") \
	.config("spark.sql.shuffle.partitions", 1) \
	.getOrCreate()

print("A")
A = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
A = spark.createDataFrame(A, ["ID", "Name"])
A.show()

print("B")
B = [(1, "Alice"), (4, "Dave"), (5, "Elena")]
B = spark.createDataFrame(B, ["ID","Name"])
B.show()


#semi join - gives the rows that are in A and in B
A.join(B, A.ID == B.ID, "semi").show()

#anti join - gives the rows that are in A but not in B
A.join(B, A.ID == B.ID, "anti").show()
