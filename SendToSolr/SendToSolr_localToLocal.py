#Runs on local machine, sends to Solr on local machine
#conda env : Trial

import requests
from requests.auth import HTTPBasicAuth
import pysolr
import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


# Solr connection details
SOLR_URL = 'http://localhost:8983/solr/Brands'
USERNAME = 'igs'
PASSWORD = 'igs345'

# Define batch size for Spark partitioning
batch_size = 350

def index_batch(batch):
    """Index a batch of documents to Solr with authentication."""
    
    # Create an HTTP session for Basic Authentication
    #session = requests.Session()
    #session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    #session.headers.update({'Content-Type': 'application/json; charset=utf-8'})
    
    # Create Solr connection using the authenticated session
    solr = pysolr.Solr(SOLR_URL, always_commit=False)
    
    documents = [row.asDict() for row in batch]
    
    if documents:
        solr.add(documents)
        print(f"Indexed batch of {len(documents)} documents.")




spark = SparkSession.builder.appName("SendToSolr").getOrCreate()

# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .option("inferSchema", "true") \
    .option("header", "false") \
    .load(r"C:\Users\shravanr\learning\spark\input_dir\BrandData_Raw_Sample1000.csv")

df = df.toDF("BrandId", "BrandName", "BrandAliases", "Domains", "NumProducts", "SampleProductNames", "NumRetailers", "TopRetailers")


df = df.withColumn("BrandAliases", F.split(df["BrandAliases"], ","))
df = df.withColumn("SampleProductNames", F.split(df["SampleProductNames"], ","))
df = df.withColumn("TopRetailers", F.split(df["TopRetailers"], ","))

df.show()

# Process DataFrame in batches and index
df.foreachPartition(index_batch)

# Commit changes to Solr after all batches are processed
solr = pysolr.Solr(SOLR_URL, always_commit=False)
solr.commit()

print("Data ingestion completed.")
spark.stop()

