import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("static_static_join")\
	.master("local[3]") \
	.config("spark.streaming.stopGracefullyOnShutdown", "true") \
	.config("spark.sql.shuffle.partitions", 1) \
	.getOrCreate()

DataSetA = [(1, "A","v"), (2, "B","v"), (3, "C","v"), (4, "D", "v")]
DataSetA = spark.createDataFrame(data=DataSetA, schema=["Key", "ValueA", "CommonColumn"])

DataSetB = [ (2, 12, "w"), (3, 13,"w"), (4, 14,"w"), (5, 15,"w")]
DataSetB = spark.createDataFrame(data=DataSetB, schema=["Key", "ValueB","CommonColumn"])

#join_expr = DataSetA["Key"] == DataSetB["Key"] #this also works
join_expr = DataSetA.Key == DataSetB.Key


#join_type = "left_outer" # this works
join_type = "left" # this works too
#join_expr = "leftOuter" # this doesnt work (not supported)

#left outer
print("Left outer join\n")
joined_data = DataSetA.join(DataSetB, join_expr, join_type)

joined_data.show()

#drop duplicate column
#replace null with default value
joined_data \
.drop(DataSetB.Key) \
.withColumn("ValueB", coalesce(DataSetB["ValueB"], lit(0))).show()

print("Right outer join\n")

DataSetA.join(DataSetB, join_expr, "right").show()

print("Full outer join\n")
DataSetA.join(DataSetB, join_expr, "full").show()
