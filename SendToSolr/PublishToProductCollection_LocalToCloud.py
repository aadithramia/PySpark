#Runs on local machine, sends to Solr on cloud
#conda env : Trial

import findspark
findspark.init()
findspark.find()

import requests
from requests.auth import HTTPBasicAuth
import pysolr
import time


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


ProductSchema = "SolrDocId string, BrandId long, BrandName string, ProductId long, ProductName string, ProductNameLanguage string, ProductCategory string, Gtin string, Mpn string, LanguageCodes string, CountryCodes string, NumRetailers int, TopRetailers string, NumOffers int, SampleOffers string, BPGVersionId string, Timestamp string"
input_stream = r"C:\Users\shravanr\Downloads\ProductData_20241226081718.tsv.sample10k"
batch_size = 5000 # batch size for Spark partitioning

# Solr connection details
SOLR_URL = 'http://51.8.239.64/solr/Products'
USERNAME = 'igs'
PASSWORD = 'igs345'

print(f"Input Stream: {input_stream}")
print(f"Batch Size: {batch_size}")
print(f"Solr URL: {SOLR_URL}")

from SolrBatchSendUtils import index_batch



spark = SparkSession.builder.appName("SendToSolr").getOrCreate()
spark.sparkContext.addPyFile("SolrBatchSendUtils.py")

# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .schema(ProductSchema)\
    .option("header", "false") \
    .load(input_stream)


df = df.withColumn("LanguageCodes", F.split(df["LanguageCodes"], ","))
df = df.withColumn("CountryCodes", F.split(df["CountryCodes"], ","))
df = df.withColumn("TopRetailers", F.split(df["TopRetailers"], ",").cast(ArrayType(IntegerType())))
df = df.withColumn("SampleOffers", F.split(df["SampleOffers"], ",").cast(ArrayType(IntegerType())))


#df.show()
# Process DataFrame in batches and index
df.foreachPartition(lambda partition: index_batch(partition, batch_size, SOLR_URL, USERNAME, PASSWORD))

# Commit changes to Solr after all batches are processed
session = requests.Session()
session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
solr = pysolr.Solr(SOLR_URL, always_commit=False, session=session)
solr.commit()

print("Data ingestion completed.")
