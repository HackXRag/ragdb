import mysql.connector
from mysql.connector import Error
import nanoid
from typing import List, Tuple

def create_document_set(conn, description: str, documents: List[Tuple[str, str, str]]):
    """
    Create a document set and insert documents into ParsedDocument table.
    
    Args:
        conn: Database connection
        description: Description of the document set
        documents: List of tuples containing (source_doc_id, file_location, parser_name)
        
    Returns:
        str: The ID of the newly created document set
    """
    cursor = conn.cursor()
    
    try:
        # Start transaction
        conn.start_transaction()
        
        # 1. Create document set
        docset_id = nanoid.generate()
        cursor.execute("""
            INSERT INTO DocumentSet (id, description)
            VALUES (%s, %s)
        """, (docset_id, description))
        
        # 2. Get or create parser
        def get_or_create_document_parser(parser_name):
            cursor.execute("""
                SELECT id FROM DocumentParser 
                WHERE parser_name = %s
            """, (parser_name,))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
            cursor.execute("""
                INSERT INTO DocumentParser (parser_name)
                VALUES (%s)
            """, (parser_name,))
            return cursor.lastrowid
        
        # 3. Insert documents
        for source_doc_id, file_location, parser_name in documents:
            parser_id = get_or_create_document_parser(parser_name)
            
            cursor.execute("""
                INSERT INTO ParsedDocument 
                (id, document_id, parsed_text_location, parser_id, docset_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (nanoid.generate(), source_doc_id, file_location, parser_id, docset_id))
        
        # Complete transaction
        conn.commit()
        return docset_id
        
    except Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

# Example usage:
if __name__ == "__main__":
    from ragdb import open_ragdb
    
    # List of (source_doc_id, file_location, parser_name)
    documents = [
        ("doc1", "/path/to/doc1.txt", "Nougat Tom"),
        ("doc2", "/path/to/doc2.txt", "Nougat Tom"),
    ]
    
    conn = open_ragdb()
    docset_id = create_document_set(
        conn=conn,
        description="Example document set",
        documents=documents
    )
    
    if docset_id:
        print(f"Successfully created document set with ID: {docset_id}")
    else:
        print("Failed to create document set") 