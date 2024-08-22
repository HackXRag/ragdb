import ragdb
import sys

from openai import OpenAI

import mysql.connector
from mysql.connector import Error

db = ragdb.open_ragdb()
ins_db = ragdb.open_ragdb()

from langchain_community.embeddings import HuggingFaceInferenceAPIEmbeddings
from langchain_experimental.text_splitter import SemanticChunker

# Modify OpenAI's API key and API base to use vLLM's API server.
openai_api_key = "EMPTY"
openai_api_base = "http://lambda7:8000/v1"

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
        encoding_format="float" 
                              )

print(embeddings)

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

