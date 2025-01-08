import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("file_read")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

data = spark.read \
    .format("json") \
    .load("CommonTransformations_input.json")

#data = data.selectExpr("StoreId", "InvoiceNumber", "InvoiceDate", "explode(LineItems) as LineItem")

#data = data.select("StoreId", "InvoiceNumber", "InvoiceDate", expr("explode(LineItems) as LineItem"))

data = data.select("StoreId", "InvoiceNumber", "InvoiceDate","DeliveryAddress", "LineItems")

data = data.withColumn("DeliveryAddress", expr("concat(DeliveryAddress.StreetName, ',',  DeliveryAddress.City, ', ',  DeliveryAddress.State,' ', DeliveryAddress.ZipCode)"))

data = data.withColumn("InvoiceDate", expr("to_date(InvoiceDate)") )
data = data.select("StoreId", "InvoiceNumber", "InvoiceDate","DeliveryAddress", expr("explode(LineItems) as LineItem"))
data = data.withColumn("LineItemId", expr("LineItem.LineItemId"))
data = data.withColumn("Qty", expr("LineItem.Quantity"))
data = data.withColumn("UnitPrice", expr("LineItem.UnitPrice"))

data = data.drop("LineItem")

data = data.withColumn("Price", expr("Qty * UnitPrice"))

data.show(20, truncate=False)

invoiceAmt = data.groupBy("StoreId", "InvoiceNumber").sum("Price").withColumnRenamed("sum(Price)", "InvoiceAmt")

invoiceAmt.show(20)

invoiceAmt.printSchema()

invoiceAmt = invoiceAmt.withColumn("InvoiceAmt", expr("cast(InvoiceAmt as int)"))

invoiceAmt.show(20)

invoiceAmt.printSchema()
