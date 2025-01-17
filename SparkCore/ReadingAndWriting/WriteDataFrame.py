import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("WriteData")\
	.master("local[3]") \
	.config("spark.streaming.stopGracefullyOnShutdown", "true") \
	.config("spark.sql.shuffle.partitions", 3) \
	.getOrCreate()


#spark.sparkContext.setLogLevel("INFO")
# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(r"C:\Users\shravanr\learning\spark\pyspark\input_dir\data.tsv")
 
print(df.rdd.getNumPartitions())

df.show()
# fails if path exists. need to set mode to overwrite to address this
#df.write.format("csv") \
#	.save(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\ReadingAndWriting\data\output\NoOptions")o overwrite 

df = df.repartition(3)

df.write.format("csv") \
	.option("maxRecordsPerFile", 2) \
	.mode("overwrite") \
	.save(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\ReadingAndWriting\data\output\Overwrite") 

print("completed overwrite")

df.write.format("csv") \
	.mode("append") \
	.save(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\ReadingAndWriting\data\output\Append") 

print("completed append")

df.write.format("csv") \
	.mode("ignore") \
	.save(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\ReadingAndWriting\data\output\Ignore") 

print("completed ignore")

# helps with partition elimination
df.write.format("csv") \
	.partitionBy("Department") \
	.mode("overwrite") \
	.save(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\ReadingAndWriting\data\output\PartitionBy") 

print("completed PartitionBy")

# bucketBy only available to spark managed tables
# df.write.format("csv") \
# 	.bucketBy(4, "EmployeeId") \
# 	.mode("overwrite") \
# 	.save(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\ReadingAndWriting\data\output\BucketBy") 

# print("completed PartitionBy")
