import os
import multiprocessing as multiprocessing
from pathlib import Path
from pypdf import PdfReader
from finsights.db.connection import get_downloaded_documents, get_document_by_transcript_uuid, mark_document_parsed, mark_document_failed
from finsights.config import MAX_CONCURRENT_CONVERSION_WORKERS, PDF_DIR, TEXT_DIR

def convert_pdf_to_text(transcript_uuid: str) -> bool:
    """
    Convert a single PDF to text and update database.
    
    Args:
        transcript_uuid: UUID of the document to process
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get document details from database
        doc = get_document_by_transcript_uuid(transcript_uuid)
        if not doc:
            print(f"Document {transcript_uuid} not found")
            return False
            
        pdf_file_name = doc["pdf_file_name"]
        if not pdf_file_name:
            print(f"No PDF file name for {transcript_uuid}")
            return False
            
        # Construct file paths
        pdf_path = Path(PDF_DIR) / pdf_file_name
        text_file_name = f"{transcript_uuid}.txt"
        temp_text_file_name = f"temp_{transcript_uuid}.txt"
        text_path = Path(TEXT_DIR) / text_file_name
        temp_text_path = Path(TEXT_DIR) / temp_text_file_name
        
        
        # Check if PDF file exists
        if not pdf_path.exists():
            print(f"PDF file not found: {pdf_path}")
            mark_document_failed(transcript_uuid, f"PDF file not found: {pdf_path}")
            return False
            
        # Convert PDF to text using pypdf
        # we write to a temp file to not accumulate the entire txt file.
        with open(pdf_path, 'rb') as pdf_file:
            reader = PdfReader(pdf_file)
            with open(temp_text_path, 'w', encoding='utf-8') as temp_text_file:
                text_content = ""
                for page_num, page in enumerate(reader.pages):
                    try:
                        text_content = f"\n--- Page {page_num + 1} ---\n"
                        page_text = page.extract_text() or "" 
                        text_content += page_text
                        temp_text_file.write(text_content)
                    except Exception as e:
                        print(f"Error extracting text from page {page_num + 1}: {e}")
                        continue
        
        # swap the temp text file with the actual text file
        os.replace(temp_text_path, text_path)
        
            
        # Update database
        mark_document_parsed(transcript_uuid, text_file_name)
        
        # Delete PDF file after successful conversion
        try:
            pdf_path.unlink()  # Delete the PDF file
            print(f"Successfully converted {transcript_uuid} to text and deleted PDF")
        except Exception as e:
            print(f"Warning: Could not delete PDF {pdf_path}: {e}")
        
        return True
        
    except Exception as e:
        print(f"Error converting {transcript_uuid}: {e}")
        # Update database with error status
        mark_document_failed(transcript_uuid, str(e))
        return False


def convert_pdfs():
    """Orchestrate PDF to text conversion using process pool"""
    downloaded_documents = get_downloaded_documents()
    
    if not downloaded_documents:
        print("No downloaded documents to process")
        return
        
    print(f"Found {len(downloaded_documents)} documents to convert")
    
    # Create process pool with 10 workers
    # We use multiprocessing instead of threading because we want to avoid GIL
    # the with ensures that the pool is closed after the context manager exits
    with multiprocessing.Pool(processes=min(MAX_CONCURRENT_CONVERSION_WORKERS, len(downloaded_documents))) as pool:
        # Submit all conversion tasks
        results = pool.map(convert_pdf_to_text, downloaded_documents)
        
        # Count results
        successful = sum(1 for result in results if result)
        failed = len(results) - successful
        
        print(f"Conversion complete: {successful} successful, {failed} failed")


if __name__ == "__main__":
    convert_pdfs()