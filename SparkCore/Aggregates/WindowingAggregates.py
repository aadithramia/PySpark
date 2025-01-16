import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("static_static_join")\
	.master("local[3]") \
	.config("spark.streaming.stopGracefullyOnShutdown", "true") \
	.config("spark.sql.shuffle.partitions", 3) \
	.getOrCreate()

A = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .csv(r"C:\Users\shravanr\learning\spark\pyspark\SparkCore\Aggregates\WindowingAggregatesData.tsv") 

A.show()

running_total_window = Window.partitionBy("Country") \
						.orderBy("WeekNumber") \
						.rowsBetween(Window.unboundedPreceding, Window.currentRow)

A.withColumn("RunningTotal", sum("Sales").over(running_total_window)).show()

print("running total for last 3 weeks")
running_total_window = Window.partitionBy("Country") \
						.orderBy("WeekNumber") \
						.rowsBetween(-2, Window.currentRow)

A.withColumn("RunningTotal", sum("Sales").over(running_total_window)).show()