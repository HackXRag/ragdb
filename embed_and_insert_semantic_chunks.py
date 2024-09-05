# for each document in the database, embed the document and insert the chunks into the database
# the documents are stored in a mysql database table named DocumentChunk.
'''
mysql> desc DocumentChunk;
+--------------------+-----------------+------+-----+-------------------+-------------------+
| Field              | Type            | Null | Key | Default           | Extra             |
+--------------------+-----------------+------+-----+-------------------+-------------------+
| chunk_id           | bigint unsigned | NO   | PRI | NULL              | auto_increment    |
| parsed_document_id | varchar(32)     | YES  | MUL | NULL              |                   |
| chunking_scheme_id | int             | YES  |     | NULL              |                   |
| chunk_text         | longtext        | YES  |     | NULL              |                   |
| ts                 | timestamp       | YES  |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+--------------------+-----------------+------+-----+-------------------+-------------------+
'''
# The embedding is stored in a mysql database table named ChunkVector.
'''mysql> desc ChunkVector;
+--------------+-----------------+------+-----+---------+----------------+
| Field        | Type            | Null | Key | Default | Extra          |
+--------------+-----------------+------+-----+---------+----------------+
| chunk_id     | bigint unsigned | NO   | MUL | NULL    | auto_increment |
| embedding_id | int             | YES  | MUL | NULL    |                |
| vector       | blob            | YES  |     | NULL    |                |
+--------------+-----------------+------+-----+---------+----------------+
'''

import mysql.connector
from sentence_transformers import SentenceTransformer
import numpy as np
import ragdb
from _util import _print

db = ragdb.open_ragdb()
cursor = db.cursor()

# Initialize the embedding model
embedding_name = 'all-MiniLM-L6-v2'
embedding_dim = '384'
model = SentenceTransformer(embedding_name)

# Insert the embedding model if its not in the db
embedding_type_id = ragdb.get_or_create_embedding_type(cursor, db, embedding_name, embedding_dim)

# Fetch all document chunks
cursor.execute("SELECT chunk_id, chunk_text FROM DocumentChunk")
chunks = cursor.fetchall()

print(f'len chunks: {len(chunks)}')

chunk_count = 1 
_t = _print("starting chunk embedding")

for chunk_id, chunk_text in chunks:
    # Generate embedding
    embedding = model.encode(chunk_text)
 
    # Convert embedding to binary
    embedding_binary = embedding.tobytes()
    
    # Insert embedding into ChunkVector table
    insert_query = "INSERT INTO ChunkVector (chunk_id, embedding_id, vector) VALUES (%s, %s, %s)"
    retval = cursor.execute(insert_query, (chunk_id, embedding_type_id, embedding_binary))

    if chunk_count % 1000 == 0:
        _t = _print(f'chunk_count: {chunk_count}', _t)

    chunk_count = chunk_count + 1

try:
    db.commit()
    print("Transaction committed successfully")
except mysql.connector.Error as error:
    print(f"Error committing transaction: {error}")
    db.rollback()  # Roll back changes if commit fails

cursor.close()
# db.close()

_t = _print(f"Processed and inserted embeddings for {len(chunks)} chunks.", _t)



# Assuming you have fetched the binary data from the database
# select the first blob data from the ChunkVector table
cursor = db.cursor()
cursor.execute("SELECT vector FROM ChunkVector LIMIT 1")
embedding_binary = cursor.fetchone()[0]

# Decode the binary data back into a numpy array
def decode_embedding(embedding_binary):
    embedding_array = np.frombuffer(embedding_binary, dtype=np.float32)
    print(f'embedding_array len: {len(embedding_array)}\nembedding: {embedding_array}')
    
    # If you know the original shape of the embedding, you can reshape it
    # For example, if it's a 384-dimensional embedding:
    embedding_reshaped = embedding_array.reshape((384,))
    print(f'embedding_array len: {len(embedding_array)}\nembedding: {embedding_array}')

    return embedding_reshaped
    
embedding = decode_embedding(embedding_binary)


cursor.close()
db.close()
