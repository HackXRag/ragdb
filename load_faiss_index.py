import mysql.connector
import numpy as np
import faiss
import ragdb
import os

model = "Salesforce/SFR-Embedding-Mistral"

# Initialize database connection
db = ragdb.open_ragdb()
cursor = db.cursor()

# Fetch embeddings from the database
cursor.execute(f"select chunk_id, vector FROM ChunkVector cv, EmbeddingType et WHERE cv.embedding_id=et.id AND et.embedding_name=\"{model}\"")
embeddings = cursor.fetchall()

# Initialize lists to store chunk IDs and vectors
chunk_ids = []
vectors = []

# Process each embedding
for chunk_id, embedding_binary in embeddings:
    # Decode the binary data back into a numpy array
    embedding_array = np.frombuffer(embedding_binary, dtype=np.float32)
    
    chunk_ids.append(chunk_id)
    vectors.append(embedding_array)

# Convert list of vectors to a 2D numpy array
print (f'len(vectors): {len(vectors)} len(vectors[0]): {len(vectors[0])}')

vectors_array = np.array(vectors)

# Get the dimension of the vectors
vector_dim = vectors_array.shape[1]

# Initialize FAISS index
index = faiss.IndexFlatL2(vector_dim)

# Add vectors to the index
index.add(vectors_array)

print(f"Added {len(chunk_ids)} vectors to the FAISS index")

# Save the FAISS index to a file
model_part = os.path.basename(model)
faiss.write_index(index, model_part + "_faiss_index.index")
print("FAISS index saved to faiss_index.index")

# Save the chunk_ids to a separate file
np.save("chunk_ids.npy", np.array(chunk_ids))
print("Chunk IDs saved to chunk_ids.npy")

# Close database connection
cursor.close()
db.close()

# The FAISS index is now ready for similarity search
# You can save the index and chunk_ids for later use if needed

