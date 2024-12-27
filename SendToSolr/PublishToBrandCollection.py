#Runs on MT cluster, sends to Solr on cloud
# import findspark
# findspark.init()
# findspark.find()

import requests
from requests.auth import HTTPBasicAuth
import pysolr
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_stream", required=True, help="Input stream path")
parser.add_argument("--batch_size", type=int, required=True, help="Batch size for Spark partitioning")
parser.add_argument("--solr_url", required=True, help="Solr URL")

args = parser.parse_args()

input_stream = args.input_stream
batch_size = int(args.batch_size)
SOLR_URL = args.solr_url

print(f"Input Stream: {input_stream}")
print(f"Batch Size: {batch_size}")
print(f"Solr URL: {SOLR_URL}")

# Solr connection details
USERNAME = 'igs'
PASSWORD = 'igs345'

BrandSchema = "SolrDocId string, BrandId long, BrandName string, BrandAliases string, Domains string, NumProducts int, SampleProductNames string, NumRetailers int, TopRetailers string, BPGVersionId string, Timestamp string"

spark = SparkSession.builder.appName("SendToSolr").getOrCreate()

from SolrBatchSendUtils import index_batch

# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .schema(BrandSchema)\
    .option("header", "false") \
    .load(input_stream)


df = df.withColumn("BrandAliases", F.split(df["BrandAliases"], ","))
df = df.withColumn("SampleProductNames", F.split(df["SampleProductNames"], ","))
df = df.withColumn("TopRetailers", F.split(df["TopRetailers"], ","))

# df.show()

# Process DataFrame in batches and index
df.foreachPartition(lambda partition: index_batch(partition, batch_size, SOLR_URL, USERNAME, PASSWORD))

# Commit changes to Solr after all batches are processed
session = requests.Session()
session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
solr = pysolr.Solr(SOLR_URL, always_commit=False, session=session)
solr.commit()

print("Data ingestion completed.")
