import sys
import mysql.connector
from mysql.connector import Error

def open_ragdb():

    try:
        # Establish the connection
        connection = mysql.connector.connect(
            host='arborvitae.cels.anl.gov',
            database='RagTest',
            user='rag'
            )
        
        if connection.is_connected():
            return connection
        
    except Error as e:
        print("Error while connecting to MySQL", e, file=sys.stderr)

    return None
