import sys
import mysql.connector
from mysql.connector import Error

def open_ragdb(host = "localhost"):
    try:
        # Establish the connection
        connection = mysql.connector.connect(
            host=host,
            database='RagTest',
            user='rag'
            )
        
        if connection.is_connected():
            return connection
        
    except Error as e:
        print("Error while connecting to MySQL", e, file=sys.stderr)

    return None



def get_or_create_embedding_type(cursor, db, embedding_name, embedding_dim):
    """
    Get or create an embedding type in the EmbeddingType table.

    This function checks if an embedding type with the given name exists in the
    EmbeddingType table. If it does, it returns the existing id. If it doesn't,
    it creates a new record and returns the new id.

    Args:
        cursor (mysql.connector.cursor.MySQLCursor): A cursor object to execute SQL queries.
        db (mysql.connector.connection.MySQLConnection): A database connection object.
        embedding_name (str): The name of the embedding type.
        embedding_dim (int): The dimension of the embedding vector.

    Returns:
        int or None: The id of the embedding type if successful, None if an error occurred.

    Raises:
        mysql.connector.Error: If a database-related error occurs.
        Exception: If an unexpected error occurs.

    Note:
        This function manages its own transaction. It will commit changes if successful
        and rollback if an error occurs.
    """
    try:
        # Check if the embedding type already exists
        cursor.execute(f"SELECT id FROM EmbeddingType WHERE embedding_name = %s and vector_length = %s", (embedding_name,embedding_dim))
        result = cursor.fetchone()

        if result:
            # If it exists, return the id
            print('found')
            return result[0]
        else:
            # If it doesn't exist, insert a new record
            print("not found")
            cursor.execute(f'SELECT max(id) FROM EmbeddingType')
            result = cursor.fetchone()
            max_id  = result[0]
            insert_query = "INSERT INTO EmbeddingType (id, embedding_name, vector_length) VALUES (%s, %s, %s)"

            cursor.execute(insert_query, (max_id+1, embedding_name, embedding_dim))
            db.commit()

            # Get the id of the newly inserted record
            cursor.execute(f'SELECT max(id) FROM EmbeddingType')
            result = cursor.fetchone()
            new_id  = result[0]

            return new_id
    except Error as e:
        print(f"Database error occurred: {e}")
        db.rollback()
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        db.rollback()
        return None

# Example usage:
# embedding_id = get_or_create_embedding_type(cursor, "all-MiniLM-L6-v2", 384)

