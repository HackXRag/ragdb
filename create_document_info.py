"""
Module for creating document information from paired PDF and text files.
"""

from pathlib import Path
from typing import List, Tuple

def create_document_info(txt_folder_path: str, pdf_folder_path: str) -> List[Tuple[str, str, str, str]]:
    """
    Create document information from text and PDF files in specified folders.
    
    Args:
        txt_folder_path (str): Path to the folder containing text files
        pdf_folder_path (str): Path to the folder containing PDF files
    
    Returns:
        List[Tuple[str, str, str, str]]: List of tuples containing:
            - base_name: Name of the document without extension
            - pdf_path: Full path to PDF file
            - txt_path: Full path to text file
            - additional_info: Reserved for additional information (empty for now)
    """
    documents = []
    
    # Iterate through text files in the folder
    for txt_file in Path(txt_folder_path).glob("*.txt"):
        base_name = txt_file.stem
        pdf_path = Path(pdf_folder_path) / f"{base_name}.pdf"
        
        # Check if PDF file exists in PDF folder
        if not pdf_path.exists():
            print(f"Warning: PDF file {pdf_path} does not exist")
            continue
        
        # Create tuple and append to documents list
        doc_tuple = (base_name, str(pdf_path), str(txt_file), "")
        documents.append(doc_tuple)
    
    return documents

def main():
    """Command line interface for the module."""
    txt_folder_path = input("Enter the path to the text documents folder: ")
    pdf_folder_path = input("Enter the path to the PDF documents folder: ")
    
    documents = create_document_info(txt_folder_path, pdf_folder_path)
    for doc in documents:
        print(doc)

if __name__ == "__main__":
    main()