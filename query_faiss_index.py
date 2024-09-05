import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import ragdb
from _util import _print
# Load the FAISS index
# Assuming the index is saved as 'faiss_index.index'
_t = _print("loading faiss index")
index = faiss.read_index("faiss_index.index")
_t = _print("done loading faiss index", _t)

# Load the embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Function to query the FAISS index
def query_faiss(query_text, k=5):
    # Generate embedding for the query text
    query_embedding = model.encode([query_text])[0]
    
    # Reshape the query embedding to match FAISS input requirements
    query_embedding = np.array([query_embedding]).astype('float32')
    
    # Perform the search
    distances, indices = index.search(query_embedding, k)
    
    return distances[0], indices[0]

# Example usage
query = input("Enter your query: ")
distances, indices = query_faiss(query)

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

    print(f'chunk_id: {chunk_id} is type {type(chunk_id)}')
    # Query the DocumentChunk table to get the chunk_text
    cursor.execute("SELECT chunk_text FROM DocumentChunk WHERE chunk_id = %s", (chunk_id,))
    result = cursor.fetchone()
    if result:
        chunk_text = result[0]
        print(f"{i+1}. Chunk ID: {chunk_id}")
        print(f"   Distance: {dist}")
        print(f"   Text: {chunk_text[:100]}...")  # Print first 100 characters
        print()
    else:
        print(f"{i+1}. Chunk ID: {chunk_id} not found in the database")
        print()

# Close database connection
cursor.close()
db.close()

