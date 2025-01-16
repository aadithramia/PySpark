import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("InvoiceDataFrame").getOrCreate()

data_list = [
    ("INV001", "ProductA", 2, 10.50),
    ("INV001", "ProductB", 1, 20.00),
    ("INV001", "ProductC", 5, 7.25),
    ("INV002", "ProductD", 3, 15.75),
    ("INV002", "ProductE", 2, 50.00),
    ("INV002", "ProductF", 4, 8.99),
]
# Define column names
columns = ["InvoiceNumber", "ProductName", "Qty", "UnitPrice"]

# Create Spark DataFrame
df = spark.createDataFrame(data_list, schema=columns)

# Show the DataFrame
df.show()

df.groupBy("InvoiceNumber").agg(expr("round(sum(Qty*UnitPrice),0) as InvoiceValue"), count("ProductName").alias("NumProducts")).show()