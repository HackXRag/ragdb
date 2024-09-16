import os
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
from openai import OpenAI
import ragdb
from _util import _print


# Parameters
model_name="Salesforce/SFR-Embedding-Mistral"
framework_name="VLLM"
#model_name="all-MiniLM-L6-v2"
#framework_name="SentenceTransformer"

model_base = os.path.basename(model_name)

# Load the FAISS index, note: the naming convention.
_t = _print(f"loading faiss index {model_base}_faiss_index.index")
if os.path.isfile(f"{model_base}_faiss_index.index"):
    index = faiss.read_index(f"{model_base}_faiss_index.index", faiss.IO_FLAG_MMAP)
else:
    # call load_faiss_index.py
    raise NotImplementedError("This method has not been implemented yet.")
_t = _print(f"done loading faiss index {model_base}_faiss_index.index", _t)

def get_model(model_name, framework_name):
    if framework_name == "SentenceTransformer":
        return SentenceTransformer(model_name)
    elif framework_name == "VLLM":
        host=""
        port=""
        open_api_key="EMPTY"
        openai_api_base = "http://localhost:8000/v1"
        client = OpenAI(
                api_key=open_api_key,
                base_url=openai_api_base,
        )
        models = client.models.list()
        if not models.data[0].id == model_name:
            raise ValueError(f"Invalid model_name {model_name}")

        return client
    else:
        # throw an exception
        raise ValueError(f"Invalid framework_name: {framework_name}.")

# pass in Transformer
def model_encode(model, query_text):
    print(f'model type {type(model)}')

    if isinstance(model, SentenceTransformer):
        query_embedding = model.encode(query_text)
        query_embedding = np.array([query_embedding]).astype('float32')
        return query_embedding[0][0]
    
    elif isinstance(model, OpenAI):
        client = model
        models = client.models.list()
        model_name = models.data[0].id
        print(f"using model name {model_name} with OpenAI interface")
        responses = client.embeddings.create(
               input=query_text,
               model=model_name,
        )
        #for data in responses.data:
            #print(data.embedding)  # list of float of len 4096
        x = [ data.embedding for data in responses.data  ]
        return x[0]
    
    else:
        raise TypeError(f'model is type {type(model)}')

# Function to query the FAISS index
def query_faiss(model, query_text, k=5):
    # Generate embedding for the query text
    query_embedding = model_encode(model, [query_text])
    
    # Reshape the query embedding to match FAISS input requirements
    query_embedding = np.array([query_embedding]).astype('float32')
    
    # Perform the search
    print(query_embedding)
    distances, indices = index.search(query_embedding, k)
    
    return distances[0], indices[0]


# Example usage
model = get_model(model_name, framework_name)
query = input("Enter your query: ")
distances, indices = query_faiss(model, query)

print("Top 5 similar chunks:")
for i, (dist, idx) in enumerate(zip(distances, indices)):
    print(f"{i+1}. Index: {idx}, Distance: {dist}")

# Note: You'll need to map these indices back to your original chunks
# This mapping should be stored separately when you created the FAISS index

chunk_ids = np.load("chunk_ids.npy")
db = ragdb.open_ragdb()
cursor = db.cursor()
for i, (dist, idx) in enumerate(zip(distances, indices)):
    
    chunk_id = chunk_ids[idx]
    chunk_id = int(chunk_id)

    # Query the DocumentChunk table to get the chunk_text
    cursor.execute("SELECT chunk_text FROM DocumentChunk WHERE chunk_id = %s", (chunk_id,))
    result = cursor.fetchone()
    if result:
        chunk_text = result[0]
        print(f"{i+1}. Chunk ID: {chunk_id}")
        print(f"   Distance: {dist}")
        #print(f"   Text: {chunk_text[:100]}...")  # Print first 100 characters
        print(f"   Text: {chunk_text}")
        print()
    else:
        print(f"{i+1}. Chunk ID: {chunk_id} not found in the database")
        print()

# Close database connection
cursor.close()
db.close()

