import os
from datetime import datetime, timedelta
from finsights.config import PDF_DIR, TEXT_DIR
from finsights.db.connection import get_documents_before_date, delete_document
from pathlib import Path

def clean_up_files():
    """Clean up the files from the previous run"""
    today = datetime.now().date()
    one_week_ago = today - timedelta(days=7)
    out_of_date_documents = get_documents_before_date(one_week_ago)
    for document in out_of_date_documents:
        if document["processing_status"] == "downloaded":
            pdf_path = Path(PDF_DIR) / document["pdf_file_name"]
            pdf_path.unlink(missing_ok=True)
        elif document["processing_status"] == "parsed":
            text_path = Path(TEXT_DIR) / document["text_file_name"]
            text_path.unlink(missing_ok=True)
        elif document["processing_status"] == "failed" and document["pdf_file_name"]:
            pdf_path = Path(PDF_DIR) / document["pdf_file_name"]
            pdf_path.unlink(missing_ok=True)
        delete_document(document["transcript_uuid"])
    print(f"Cleaned up {len(out_of_date_documents)} files")