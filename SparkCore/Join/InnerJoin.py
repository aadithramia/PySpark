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

join_type = "inner"

joined_data = DataSetA.join(DataSetB, join_expr, join_type)
joined_data.select("*").show()

#joined_data.select("Key", "ValueA", "ValueB").show() -> runs into AnalysisException due to ambiguous column name (Key)


#fix : drop the common columns
joined_data.drop(DataSetB.CommonColumn, DataSetB.Key).show()

#alternative fix : disambiguate the column name
joined_data.select(DataSetB["Key"], "ValueA", "ValueB").show()
#this works too
#joined_data.select(DataSetB.Key, "ValueA", "ValueB").show()

# selecting specific columns from join results and aliasing. the ambiguous column needs to be dealt with separately as shown
joined_data.select(DataSetB.Key, col("ValueA").alias("Name"), col("ValueB").alias("Age")) \
	.withColumnRenamed("Key", "ID") \
	.show()
