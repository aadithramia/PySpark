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

def send_chunk(chunk, solr):
    isLargeBatch = False
    try:
        start_time = time.time()
        solr.add(chunk)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Indexed {} documents in {:.2f} seconds.".format(len(chunk), elapsed_time))
    except pysolr.SolrError as e:
        if str(e).__contains__("HTTP 413"):
            chunk_size = len(chunk)
            if chunk_size == 1:                
                print(f"Error indexing document: Request Entity Too Large (HTTP 413) : {chunk[0]}.")
            else:
                print(f"Error indexing batch of size {len(chunk)}: Request Entity Too Large (HTTP 413). Splitting chunk")
                isLargeBatch = True
    except Exception as e:
        print(f"Error indexing batch: {e}")
    if isLargeBatch:        
        sub_chunk_size = int(len(chunk)/2)        
        sub_chunks = [chunk[:sub_chunk_size] , chunk[sub_chunk_size:]]        
        for sub_chunk in sub_chunks:
            send_chunk(sub_chunk, solr)  


def index_batch(batch):
    """Index a batch of documents to Solr with authentication."""
    
    # Create an HTTP session for Basic Authentication
    session = requests.Session()
    session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    session.headers.update({'Content-Type': 'application/json; charset=utf-8'})
    
    # Create Solr connection using the authenticated session
    solr = pysolr.Solr(SOLR_URL, always_commit=False, session=session)
    
    for chunk in get_chunks(batch, batch_size):
        send_chunk(chunk, solr)           




spark = SparkSession.builder.appName("SendToSolr").getOrCreate()

# Read the TSV file (ensure correct path)
df = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .option("encoding", "UTF-8") \
    .schema("BrandId long, BrandName string, BrandAliases string, Domains string, NumProducts int, SampleProductNames string, NumRetailers int, TopRetailers string")\
    .option("header", "false") \
    .load(input_stream)


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
