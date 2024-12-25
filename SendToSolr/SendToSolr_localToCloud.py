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



#input_stream = "adl://bingads-platform-prod-vc1-c08.azuredatalakestore.net/local/users/shravanr/BPG/Brand/BrandData_Sample1000.csv"
input_stream = r"C:\Users\shravanr\learning\spark\input_dir\BrandData_Raw_Sample1000.csv"
batch_size = 500 # batch size for Spark partitioning

# Solr connection details
SOLR_URL = 'http://51.8.239.64/solr/PCBrands'
USERNAME = 'igs'
PASSWORD = 'igs345'

def get_chunks(iterator, size):
        """Yield successive chunks from the iterator."""
        chunk = []
        for row in iterator:
            chunk.append(row.asDict())
            if len(chunk) == size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def index_batch(batch):
    """Index a batch of documents to Solr with authentication."""
    
    # Create an HTTP session for Basic Authentication
    session = requests.Session()
    session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    session.headers.update({'Content-Type': 'application/json; charset=utf-8'})
    
    # Create Solr connection using the authenticated session
    solr = pysolr.Solr(SOLR_URL, always_commit=False, session=session)
    
    for chunk in get_chunks(batch, batch_size):
        try:
            start_time = time.time()
            solr.add(chunk)
            end_time = time.time()
            elapsed_time = end_time - start_time
            print("Indexed {} documents in {:.2f} seconds.".format(len(chunk), elapsed_time))
        except Exception as e:
            print(f"Error indexing batch: {e}")




spark = SparkSession.builder.appName("SendToSolr").getOrCreate()

# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .schema("BrandId long, BrandName string, BrandAliases string, Domains string, NumProducts int, SampleProductNames string, NumRetailers int, TopRetailers string")\
    .option("header", "false") \
    .load(input_stream)

#df = df.toDF("BrandId", "BrandName", "BrandAliases", "Domains", "NumProducts", "SampleProductNames", "NumRetailers", "TopRetailers")


df = df.withColumn("BrandAliases", F.split(df["BrandAliases"], ","))
df = df.withColumn("SampleProductNames", F.split(df["SampleProductNames"], ","))
df = df.withColumn("TopRetailers", F.split(df["TopRetailers"], ","))

# df.show()
# Process DataFrame in batches and index
df.foreachPartition(index_batch)

# Commit changes to Solr after all batches are processed
session = requests.Session()
session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
solr = pysolr.Solr(SOLR_URL, always_commit=False, session=session)
solr.commit()

print("Data ingestion completed.")
