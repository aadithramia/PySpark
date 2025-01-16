import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreateDF") \
	.config("spark.sql.shuffle.partitions", 1) \
	.getOrCreate()

print("A")
A = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
A = spark.createDataFrame(A, ["Name", "Age"])
A.show()

print("B")
B = [(1, "Alice"), (4, "Dave"), (5, "Elena")]
B = spark.createDataFrame(B, ["Name", "Age"])
B.show()

#union
print("union")
U = A.union(B)
U.show()
#spark union does nt remove duplicates
U.distinct().show()
U.dropDuplicates().show()

#union all
print("union all")
U = A.unionAll(B)
U.show()

#intersect
print("intersect")
I = A.intersect(B)
I.show()

#except
print("except")
E = A.exceptAll(B)
E.show()

#subtract
print("subtract")
S = A.subtract(B)
S.show()

