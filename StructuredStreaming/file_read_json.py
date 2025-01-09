import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Reads json source and writes to json sink

spark = SparkSession.builder.appName("file_read")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

data = spark.readStream \
    .format("json") \
    .option("path", "data/invoice_data_input") \
    .load()

data = data.select("StoreId", "InvoiceNumber", "InvoiceDate","DeliveryAddress", "LineItems")

data = data.withColumn("DeliveryAddress", expr("concat(DeliveryAddress.StreetName, ',',  DeliveryAddress.City, ', ',  DeliveryAddress.State,' ', DeliveryAddress.ZipCode)"))

data = data.withColumn("InvoiceDate", expr("to_date(InvoiceDate)") )
data = data.select("StoreId", "InvoiceNumber", "InvoiceDate","DeliveryAddress", expr("explode(LineItems) as LineItem"))
data = data.withColumn("LineItemId", expr("LineItem.LineItemId"))
data = data.withColumn("Qty", expr("LineItem.Quantity"))
data = data.withColumn("UnitPrice", expr("LineItem.UnitPrice"))

data = data.drop("LineItem")

data = data.withColumn("Price", expr("Qty * UnitPrice"))



invoiceAmt = data.groupBy("StoreId", "InvoiceNumber").sum("Price").withColumnRenamed("sum(Price)", "InvoiceAmt")


invoiceAmt = invoiceAmt.withColumn("InvoiceAmt", expr("cast(InvoiceAmt as int)"))


#query = invoiceAmt.writeStream \ => doesnt work; json source supports append mode with streaming aggregations only when watermark is defined
query = data.writeStream \
                        .format("json") \
                        .option("path", "data/invoice_data_output") \
                        .option("checkpointLocation", "checkpoint-dir") \
                        .outputMode("append") \
                        .queryName("a") \
                        .start()

query.awaitTermination()