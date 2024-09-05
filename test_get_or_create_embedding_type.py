import ragdb

db = ragdb.open_ragdb()

cursor=db.cursor()

embedding_name = 'PubMedBERT'
embedding_dim = 768

id = ragdb.get_or_create_embedding_type(cursor, db, embedding_name, embedding_dim)

print(f'{embedding_name} {id}')

embedding_name = 'tomas'
embedding_dim = 1026
new_id = ragdb.get_or_create_embedding_type(cursor, db, embedding_name, embedding_dim)

print(f'{embedding_name} {new_id}')

