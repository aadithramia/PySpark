import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *



def build_address(house_number, street_name, city, state, zip_code):
    return f"{house_number} {street_name}, {city}, {state} {zip_code}"

# Create SparkSession
spark = SparkSession.builder.appName("InvoiceDataFrame").getOrCreate()

data_list = [
    ("INV001", "123", "Main St", "Los Angeles", "CA", "90001"),
    ("INV002", "456", "Broadway Ave", "New York", "NY", "10001"),
    ("INV003", "789", "Market St", "San Francisco", "CA", "94105"),
    ("INV004", "101", "Elm St", "Chicago", "IL", "60601"),
    ("INV005", "202", "Sunset Blvd", "Miami", "FL", "33101"),
]

# Define column names
columns =  ["InvoiceNumber", "HouseNumber", "StreetName", "City", "State", "ZipCode"]

# Create Spark DataFrame
df = spark.createDataFrame(data_list, schema=columns)

# Show the DataFrame
df.show()

build_address_udf = udf(build_address, StringType())

df = df.withColumn("Address", build_address_udf("HouseNumber", "StreetName", "City", "State", "ZipCode")) \
		.drop("HouseNumber", "StreetName", "City", "State", "ZipCode")

df.show(truncate=False)

# option 2: registering as sql function
build_address_udf2 = spark.udf.register("get_address", build_address, StringType())

df = spark.createDataFrame(data_list, schema=columns)

df = df.withColumn("Address", expr("get_address(HouseNumber, StreetName, City, State, ZipCode)")) \
		.drop("HouseNumber", "StreetName", "City", "State", "ZipCode")

df.show(truncate=False)


