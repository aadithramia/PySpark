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


BrandSchema = "SolrDocId string, BrandId long, BrandName string, BrandAliases string, Domains string, NumProducts int, SampleProductNames string, NumRetailers int, TopRetailers string, BPGVersionId string, Timestamp string"
input_stream = r"C:\Users\shravanr\Downloads\BrandData_20241226125735.tsv.sample10k"
batch_size = 1000 # batch size for Spark partitioning

# Solr connection details
SOLR_URL = 'http://51.8.239.64/solr/Brands'
USERNAME = 'igs'
PASSWORD = 'igs345'

print(f"Input Stream: {input_stream}")
print(f"Batch Size: {batch_size}")
print(f"Solr URL: {SOLR_URL}")


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
    .schema(BrandSchema)\
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
