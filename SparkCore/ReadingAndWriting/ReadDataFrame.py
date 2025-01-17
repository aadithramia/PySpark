import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf
from pyspark.sql import functions as F

conf = SparkConf()

conf = conf.set("spark.sql.shuffle.partitions", "2")

spark = SparkSession.builder.appName("ReadDF").config(conf=conf).getOrCreate()

#spark.sparkContext.setLogLevel("INFO")
# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(r"C:\Users\shravanr\learning\spark\pyspark\input_dir\data.tsv")
 


partitioned = df.repartition(2)


avg_salary_by_age = partitioned\
                    .where("Age > 25")\
                    .groupBy("Department").count()
avg_salary_by_age.collect()
input('a')
spark.stop()

