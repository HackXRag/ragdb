from queue import Queue
from threading import Thread

import ragdb
import sys

from openai import OpenAI

import mysql.connector
from mysql.connector import Error

db = ragdb.open_ragdb()
ins_db = ragdb.open_ragdb()

from langchain_community.embeddings import HuggingFaceInferenceAPIEmbeddings
from langchain_experimental.text_splitter import SemanticChunker
from langchain_core.documents import Document

if False:

    # HuggingFace embeddings setup
    inference_api_key = "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"
    embeddings = HuggingFaceInferenceAPIEmbeddings(
        api_key=inference_api_key, model_name="avsolatorio/GIST-Embedding-v0"
        )

else:


    # Modify OpenAI's API key and API base to use vLLM's API server.
    openai_api_key = "EMPTY"
    #openai_api_base = "http://rbdgx1.cels.anl.gov:8000/v1"
    openai_api_base = "http://localhost:8000/v1"
    #openai_api_base = "http://lambda7:8000/v1"
    
    client = OpenAI(
        # defaults to os.environ.get("OPENAI_API_KEY")
        api_key=openai_api_key,
        base_url=openai_api_base,
        )
    
    models = client.models.list()
    model = models.data[0].id
    
    from langchain_openai import OpenAIEmbeddings
    
    embeddings = OpenAIEmbeddings(model=model,
                                  api_key=openai_api_key,
                                  base_url=openai_api_base,
                                  # encoding_format="float" 
                                  )

print(embeddings)

text_splitter = SemanticChunker(embeddings)

#qry =  ("SELECT id, parsed_text_location FROM ParsedDocument limit 200")
qry = ("SELECT id, parsed_text_location FROM ParsedDocument WHERE NOT EXISTS (SELECT chunk_id FROM DocumentChunk WHERE parsed_document_id = id)");

print(qry)
cursor = db.cursor(buffered=True)
cursor.execute(qry)

ins_cursor = ins_db.cursor()

insert_chunk = ("INSERT INTO DocumentChunk (parsed_document_id, chunking_scheme_id, chunk_text) VALUES (%s, 0, %s)")

def db_worker(tid, out_q):

    while True:
        item = out_q.get()
        if (item is None):
            out_q.task_done()
            break
        
        for doc in item:
            doc_id = doc.metadata['doc_id']
            chunk = doc.page_content
            
            print(f"{doc_id} {chunk[0:100]}")
            
            ins_cursor.execute(insert_chunk, [doc_id, chunk])
            
        ins_db.commit()
        out_q.task_done()


def worker(tid, in_q, out_q):
    print(f"start worker {tid}")
    while True:
        item = in_q.get()
        if item == []:
            in_q.task_done()
            break
        print(f'{tid} Working on {item}')

        size = 0

        doclist = []
        idlist = []
        for ditem in item:
            doc_id, loc = ditem

            print(f"chunk {doc_id} {loc}")
            with open(loc, "r") as file:
                text = file.read()
                size += len(text)

                doclist.append(Document(page_content=text, metadata={'doc_id': doc_id}))

                
        try:
            print(f"{tid} request size {size}")
            docs = text_splitter.split_documents(doclist)
            print(f"{tid} split done")

            out_q.put(docs)
        except Exception as e:
            print(f"Failed to chunk {doc_id} {loc}: {e}")

        print(f'{tid} Finished {item}')
        in_q.task_done()
    print(f"finish worker {tid}")
    out_q.put(None)

nthreads = 16

ndocs = 4

in_q = Queue()
out_q = Queue()

threads = []
for tid in range(1, nthreads+1):
    print(f"Creating {tid}")
    t = Thread(target=worker, args=[tid, in_q, out_q])
    t.start()
    threads.append(t)

db_thread = Thread(target=db_worker, args=['db', out_q])
db_thread.start()
threads.append(db_thread)

batch = []
for (doc_id, loc) in cursor:

    batch.append([doc_id, loc])
    if len(batch) == ndocs:
        in_q.put(batch)
        batch = []
    continue

for t in threads:
    in_q.put([])


print("join in_q")
in_q.join()
print("join out_q")
out_q.join()
                                        
for t in threads:
    print(f"join {t}")
    t.join()
