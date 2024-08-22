import ragdb
import sys

import mysql.connector
from mysql.connector import Error

db = ragdb.open_ragdb()
ins_db = ragdb.open_ragdb()

from langchain_community.embeddings import HuggingFaceInferenceAPIEmbeddings
from langchain_experimental.text_splitter import SemanticChunker

# HuggingFace embeddings setup
inference_api_key = "XXX"
embeddings = HuggingFaceInferenceAPIEmbeddings(
    api_key=inference_api_key, model_name="avsolatorio/GIST-Embedding-v0"
    )

# Using HuggingFace embeddings with SemanticChunker
text_splitter = SemanticChunker(embeddings)

qry =  ("SELECT id, parsed_text_location FROM ParsedDocument ")

cursor = db.cursor(buffered=True)
cursor.execute(qry)

ins_cursor = ins_db.cursor()

insert_chunk = ("INSERT INTO DocumentChunk (parsed_document_id, chunking_scheme_id, chunk_text) VALUES (%s, 0, %s)")

for (doc_id, loc) in cursor:
    print(f"chunk {doc_id} {loc}")
    with open(loc, "r") as file:
        text = file.read()

        try:
            docs = text_splitter.create_documents([text])
            
            for doc in docs:
                ins_cursor.execute(insert_chunk, [doc_id, doc.page_content])
        except Exception as e:
            print(f"Failed to chunk {doc_id} {loc}: {e}")
    ins_db.commit()

