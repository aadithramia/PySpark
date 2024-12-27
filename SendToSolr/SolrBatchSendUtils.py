import pysolr
import time

import requests
from requests.auth import HTTPBasicAuth


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


def index_batch(batch, batch_size, SOLR_URL, USERNAME, PASSWORD):
    """Index a batch of documents to Solr with authentication."""
    
    # Create an HTTP session for Basic Authentication
    session = requests.Session()
    session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
    session.headers.update({'Content-Type': 'application/json; charset=utf-8'})
    
    # Create Solr connection using the authenticated session
    solr = pysolr.Solr(SOLR_URL, always_commit=False, session=session)
    
    for chunk in get_chunks(batch, batch_size):
        send_chunk(chunk, solr)           
