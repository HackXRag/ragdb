import mysql.connector
from mysql.connector import Error
import nanoid
from typing import List, Tuple

parser_name="PyPDF2"

parser_name="PyPDF2"

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
    cursor = conn.cursor(buffered=True)
    
    try:
        # Start transaction
        conn.start_transaction()
        
        # 1. Create document set
        docset_id = nanoid.generate()
        print(f'inserting into DocumentSet {docset_id}, {description}')
        cursor.execute("""
            INSERT INTO DocumentSet (id, description)
            VALUES (%s, %s)
        """, (docset_id, description))
        
        
        # 2. Get or create parser
        def get_or_create_document_parser(parser_name):
            print(f'selecting from DocumentParser {parser_name}')
            cursor.execute("""
                SELECT id FROM DocumentParser 
                WHERE parser_name = %s
            """, (parser_name,))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
            print(f'inserting into DocumentParser {parser_name}')
            cursor.execute("""
                INSERT INTO DocumentParser (parser_name)
                VALUES (%s)
            """, (parser_name,))
            return cursor.lastrowid
        
        parser_id = get_or_create_document_parser(parser_name)
        
        # 2.5 Insert source papers into source document assumes 
        # that (source_id, docset_id) is a unique constraint, which it is not.
        documents_copy = []
        for source_id, pdf_location, txt_location, title in documents:
            
            # First see if SourceDocument exists. We need this to check
            # on the unique constraint (source_id, docset_id)
            print(f'selecting {source_id} from SourceDocument')
            cursor.execute("""
                SELECT id, source_id FROM SourceDocument 
                WHERE source_id = %s
            """, (source_id,))
            result = cursor.fetchone()
            if result:
                print(f'found source document {result[0]}')
                

            _id = nanoid.generate()
            print(f'inserting {source_id} into SourceDocument')
            print(f'{_id}, {title}, {source_id}, {pdf_location}, {docset_id}' )
            cursor.execute("""
                INSERT INTO SourceDocument (id, title_line, source_id, pdf_text_location, docset_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (_id, title, source_id, pdf_location, docset_id ))
            
            documents_copy.append((source_id, pdf_location, txt_location, title, _id))
            
        
        # 3. Insert parsed documents
        for source_id, pdf_location, text_location, title, _id in documents_copy:
            
            parser_id = get_or_create_document_parser(parser_name)
            print(f'inserting into ParsedDocument id: {_id}) document_id: {source_id} docset_id: {docset_id}')            
            cursor.execute("""
                INSERT INTO ParsedDocument 
                (id, document_id, parsed_text_location, parser_id, docset_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (nanoid.generate(), _id, text_location, parser_id, docset_id))
            
        
        conn.commit()
        return docset_id

    except Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None
        
    
    

if __name__ == "__main__":
    from ragdb import open_ragdb
    
    # List of (source_doc_id, pdf_file_location, text_file_location, title)

    documents = [
            ("doc1", "/path/to/doc1.pdf", "/path/to/doc1.txt", "Document Title 1"),
            ("doc2", "/path/to/doc1.pdf", "/path/to/doc2.txt", "Document Title 2"),
    ]
    
    conn = open_ragdb(host="chestnut.cels.anl.gov")
    docset_id = create_document_set(
        conn=conn,
        description="DDR and Aptosis",
        documents=documents
    )
    
    if docset_id:
        print(f'Successfully created document set with ID: {docset_id}')
        print(f'DELETE from ParsedDocument where docset_id="{docset_id}";')
        print(f'DELETE from SourceDocument where docset_id="{docset_id}";')
        print(f'DELETE from DocumentSet where id="{docset_id}";')
        
    else:
        print("Failed to create document set") 