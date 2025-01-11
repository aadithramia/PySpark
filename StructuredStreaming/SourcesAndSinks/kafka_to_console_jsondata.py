import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Reads json source and writes to json sink

spark = SparkSession.builder.appName("kafka_to_console")\
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Topic2") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1) \
    .load()

schema = StructType([
    StructField("InvoiceNumber", StringType()),
    StructField("InvoiceDate", StringType()),
    StructField("StoreId", StringType()),
    StructField("DeliveryAddress", StructType([
        StructField("StreetName", StringType()),
        StructField("City", StringType()),
        StructField("State", StringType()),
        StructField("ZipCode", StringType())
    ])),
    StructField("LineItems", ArrayType(StructType([
        StructField("LineItemId", StringType()),
        StructField("UnitPrice", DoubleType()),
        StructField("Quantity", IntegerType())
    ])))
])

json_data = data.select(from_json(col("value").cast("string"), schema).alias("json"))


# select implicitly renames fields to the field name as defined in schema when you use it to extract fields from the nested JSON structure. When you use from_json with alias("json"), it creates a column named json that contains a struct with the fields defined in your schema. When you then select fields from this struct using json.fieldName, Spark automatically flattens the structure and uses the field names directly.
#
data = json_data.select("json.StoreId", "json.InvoiceNumber", "json.InvoiceDate","json.DeliveryAddress", "json.LineItems") \

data = data.withColumn("DeliveryAddress", expr("concat(DeliveryAddress.StreetName, ',',  DeliveryAddress.City, ', ',  DeliveryAddress.State,' ', DeliveryAddress.ZipCode)"))

# data = data.withColumn("InvoiceDate", expr("to_date(InvoiceDate)") )
# data = data.select("StoreId", "InvoiceNumber", "InvoiceDate","DeliveryAddress", expr("explode(LineItems) as LineItem"))
# data = data.withColumn("LineItemId", expr("LineItem.LineItemId"))
# data = data.withColumn("Qty", expr("LineItem.Quantity"))
# data = data.withColumn("UnitPrice", expr("LineItem.UnitPrice"))

# data = data.drop("LineItem")

query = data.writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint-dir") \
        .option("truncate", False) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start() \
        .awaitTermination()
