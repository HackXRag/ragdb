"""
Docset commands.

A docset is a set of documents that are collected for the purpose of
chunking, embedding, and serving up for a RAG-based application.

"""

from ragdb import *

import argparse
import json
from pprint import pprint
from mysql.connector import Error
import sys
import os.path
import glob
import nanoid

def do_create(db, name, description):
    print(f"create {name}")

    cursor = db.cursor()

    try:
        cursor.execute("INSERT INTO DocumentSet (id, description) VALUES (%s, %s)",
                       [name, description])
    except Error as e:
        print(f"Error creating docset: {e}")
        sys.exit(1)
    db.commit()

def do_list(db, sets, show_contents):

    cond = ""
    params = []
    if len(sets) > 0:
        cond = "id IN (" + ",".join(map(lambda x: "%s", sets)) +")"
        params.extend(sets)

    cursor = db.cursor()

    if show_contents:
        pass
    else:
         qry = "SELECT id, description FROM DocumentSet"
         if cond:
             qry += " WHERE " + cond
         cursor.execute(qry, params)

         for (id, desc) in cursor:
             print(f"Doc {id}: {desc}")

def acceptable_file(f):
    (base, ext) = os.path.splitext(f)
    return ext in [".md", ".pdf", ".rst"]

def do_add(db, docset, files, recursive):

    #
    # Verify we have a set with this id
    #
    cursor = db.cursor()
    cursor.execute("SELECT description FROM DocumentSet WHERE id = %s", [docset])
    res = cursor.fetchone()
    
    if res is None:
        print(f"No set found for docset id {docset}", file=sys.stderr)
        sys.exit(1)

    #
    # Query current contents so we don't duplicate
    #
    seen = {}
    cursor.execute("SELECT pdf_text_location FROM SourceDocument WHERE docset_id=%s", [docset])
    for (path,) in cursor:
        seen[path] = 1

    all_files = []

    for file in files:
        if os.path.isfile(file):
            all_files.append(os.path.abspath(file))
        else:
            all_files.extend(map(os.path.abspath, sorted(filter(acceptable_file, glob.glob(f"{file}/**", recursive=recursive)))))

    for file in all_files:
        if file in seen:
            print(f"Skipping {file}, already present in {docset}")
            continue
        st = os.stat(file)
        cursor.execute("INSERT INTO SourceDocument (id, docset_id, size, pdf_text_location) "
                       "VALUES (%s, %s, %s, %s)",
                       [nanoid.generate(), docset, st.st_size, file])
    db.commit()

def do_chunk_markdown(db, docset):

    #
    # Verify we have a set with this id
    #
    ins_cursor = db.cursor(buffered=True)
    cursor = db.cursor(buffered=True)
    cursor.execute("SELECT description FROM DocumentSet WHERE id = %s", [docset])
    res = cursor.fetchone()
    
    if res is None:
        print(f"No set found for docset id {docset}", file=sys.stderr)
        sys.exit(1)

    #
    # For the markdown chunking, we add ParsedDocument records to mirror SourceDocument since
    # we don't have any parsing to do. Then chunk and create DocumentChunk records.
    #

    paths = []
    cursor.execute("SELECT id, pdf_text_location FROM SourceDocument WHERE docset_id=%s", [docset])
    for (docid, path,) in cursor:
        print("del")
        ins_cursor.execute("DELETE FROM ParsedDocument WHERE docset_id=%s AND document_id=%s", [docset, docid])
        pid = nanoid.generate()
        ins_cursor.execute("INSERT INTO ParsedDocument(id, document_id, parsed_text_location, docset_id) "
                           "VALUES (%s, %s, %s, %s)", [pid, docid, path, docset]);
        paths.append((docid, pid, path))
    db.commit()

    headers_to_split_on = [
        ("#", "Header 1"),
        ("##", "Header 2"),
        ("###", "Header 3"),
        ]

    from langchain_text_splitters import MarkdownHeaderTextSplitter
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)

    chunk_size = 250
    chunk_overlap = 30
    text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, chunk_overlap=chunk_overlap
            )

    for (docid, pid, path) in paths:
        ins_cursor.execute("DELETE FROM DocumentChunk WHERE parsed_document_id = %s", [pid])
        with open(path, 'r') as fh:
            txt = fh.read()
            hdr_splits = markdown_splitter.split_text(txt)
            splits = text_splitter.split_documents(hdr_splits)

            for doc in splits:
                ins_cursor.execute("INSERT INTO DocumentChunk (parsed_document_id, chunk_text) VALUES (%s, %s)",
                                   [pid, doc.page_content]);
        db.commit()

def do_compute_embeddings(db, docset, model, vllm):

    #
    # Verify we have a set with this id
    #
    ins_cursor = db.cursor(buffered=True)
    cursor = db.cursor(buffered=True)
    cursor.execute("SELECT description FROM DocumentSet WHERE id = %s", [docset])
    res = cursor.fetchone()
    
    if res is None:
        print(f"No set found for docset id {docset}", file=sys.stderr)
        sys.exit(1)

    # 
    # Validate model parameter
    #
    cursor.execute("SELECT id, vector_length FROM EmbeddingType WHERE embedding_name = %s", [model])
    res = cursor.fetchone()
    if res is None:
        print(f"Model not found for {model}", file=sys.stderr)
        sys.exit(1)
    (model_id, vector_length) = res

    from models import EmbeddingModel
    model = EmbeddingModel(model, 'VLLM', endpoint=vllm)

    cursor.execute("""SELECT chunk_id, chunk_text
		      FROM DocumentChunk c JOIN ParsedDocument p ON c.parsed_document_id = p.id
                      WHERE p.docset_id = %s""", [docset])
    for (chunk_id, chunk_text,) in cursor:
        ins_cursor.execute("DELETE FROM ChunkVector WHERE chunk_id = %s", [chunk_id])
        vector = model.encode(chunk_text)
        ins_cursor.execute("INSERT INTO ChunkVector (chunk_id, embedding_id, vector) VALUES (%s, %s, %s)",
                           [chunk_id, model_id, vector.tobytes()]);
    db.commit()

def do_load_faiss(db, docset, output, vllm):

    import numpy as np
    import faiss
    
    #
    # Verify we have a set with this id
    #
    ins_cursor = db.cursor(buffered=True)
    cursor = db.cursor(buffered=True)
    cursor.execute("SELECT description FROM DocumentSet WHERE id = %s", [docset])
    res = cursor.fetchone()
    
    if res is None:
        print(f"No set found for docset id {docset}", file=sys.stderr)
        sys.exit(1)

    #
    # Determine model type for this docset
    #
    cursor.execute("""SELECT distinct (embedding_id)
    		      FROM ParsedDocument d JOIN DocumentChunk c on d.id = c.parsed_document_id JOIN ChunkVector v ON v.chunk_id = c.chunk_id
                      WHERE d.docset_id = %s""", [docset])
    res = cursor.fetchall()
    if len(res) == 0:
        print(f"No chunks found for {docset}", file=sys.stderr)
        sys.exit(1);
    elif len(res) > 1:
        print(f"Multiple embeddings found in saved vectors for {docset}", file=sys.stderr)
        sys.exit(1)
    (embedding_id,) = res[0]

    cursor.execute("SELECT embedding_name, vector_length FROM EmbeddingType WHERE id = %s", [embedding_id])
    (embedding_name, vector_length) = cursor.fetchone()
    
    cursor.execute("""SELECT distinct c.chunk_id, v.vector
    		      FROM ParsedDocument d JOIN DocumentChunk c on d.id = c.parsed_document_id JOIN ChunkVector v ON v.chunk_id = c.chunk_id
                      WHERE d.docset_id = %s""", [docset])

    # Initialize lists to store chunk IDs and vectors
    chunk_ids = []
    vectors = []

    for (chunk_id, vector) in cursor:
        embedding_array = np.frombuffer(vector, dtype=np.float32)
    
        chunk_ids.append(chunk_id)
        vectors.append(embedding_array)

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
    faiss.write_index(index, "{output}.index")
    print(f"FAISS index saved to {output}.index")
    
    # Save the chunk_ids to a separate file
    np.save(f"{output}.chunk_ids.npy", np.array(chunk_ids))
    print("Chunk IDs saved to chunk_ids.npy")

    
def parse_args():
    parser = argparse.ArgumentParser(prog="docset")
    parser.add_argument("--dbhost", help="Database hostname", default="arborvitae.cels.anl.gov")
    parser.add_argument("--vllm", help="VLLM endpoint", default="http://lambda7.cels.anl.gov:8000/v1")

    subparsers = parser.add_subparsers(help="sub-command help", dest="command")
    
    parser_create = subparsers.add_parser("create", help="Create a new docset")
    parser_create.add_argument("name", help="Docset name. Must be unique and 32 characters or fewer")
    parser_create.add_argument("description", help="Short description")
    
    parser_add = subparsers.add_parser("add", help="Add documents to a docset")
    parser_add.add_argument("--recursive", "-r", help="Rescurse through directories to find files", action='store_true')
    parser_add.add_argument("name", help="Docset name")
    parser_add.add_argument("file", help="Filename", action="extend", nargs="+")
    
    parser_list = subparsers.add_parser("list", help="List docsets")
    parser_list.add_argument("--contents", "-c", help="Also list contents of the docsets")
    parser_list.add_argument("docset", help="Docset name", action="extend", nargs="*")
    
    parser_list = subparsers.add_parser("chunk-markdown", help="Chunk the docset. Just markdown for now")
    parser_list.add_argument("docset", help="Docset name")
    
    parser_embed = subparsers.add_parser("compute-embeddings", help="Compute embeddings for the given docset")
    parser_embed.add_argument("docset", help="Docset name")
    parser_embed.add_argument("model", help="Embedding model name")
    
    parser_load_faiss = subparsers.add_parser("load-faiss", help="Load a faiss database for the given docset")
    parser_load_faiss.add_argument("docset", help="Docset name")
    parser_load_faiss.add_argument("output", help="Name of output faiss index")
    
    args = parser.parse_args()
    return args

def main():

    args = parse_args()

    db = open_ragdb(args.dbhost)
    
    if args.command == "create":
        do_create(db, args.name, args.description)
        
    elif args.command == "add":
        do_add(db, args.name, args.file, args.recursive)
        
    elif args.command == "list":
        do_list(db, args.docset, args.contents)

    elif args.command == "chunk-markdown":
        do_chunk_markdown(db, args.docset)

    elif args.command == "compute-embeddings":
        do_compute_embeddings(db, args.docset, args.model, args.vllm)

    elif args.command == "load-faiss":
        do_load_faiss(db, args.docset, args.output, args.vllm)


if __name__ == '__main__':
    main()
