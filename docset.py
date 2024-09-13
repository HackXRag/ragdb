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

    
def parse_args():
    parser = argparse.ArgumentParser(prog="docset")

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
    
    args = parser.parse_args()
    return args

def main():

    args = parse_args()

    db = open_ragdb(host="arborvitae.cels.anl.gov")
    
    if args.command == "create":
        do_create(db, args.name, args.description)
        
    elif args.command == "add":
        do_add(db, args.name, args.file, args.recursive)
        
    elif args.command == "list":
        do_list(db, args.docset, args.contents)

    elif args.command == "chunk-markdown":
        do_chunk_markdown(db, args.docset)


if __name__ == '__main__':
    main()
