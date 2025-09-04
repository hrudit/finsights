import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from finsights.config import DB_PATH
from zoneinfo import ZoneInfo

def now_ist_str() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%dT%H:%M:%S")

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # so results are dict-like
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def insert_document(doc):
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO documents (
            transcript_uuid, company_name, script_code,
            pdf_url, pdf_url_sha256, json_text,
            created_at, announcement_date, updated_at, processing_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            doc["uuid"], doc["company_name"], doc["script_code"],
            doc["pdf_url"], doc["pdf_url_sha256"], doc["json_text"],
            now_ist_str(), doc["announcement_date"], now_ist_str(), "discovered"
        ))

def get_document_by_transcript_uuid(transcript_uuid: str):
    with get_conn() as conn:
        row = conn.execute("""
        SELECT * FROM documents WHERE transcript_uuid = ?
        """, (transcript_uuid,)).fetchone()
        return dict(row) if row else None

def list_documents_by_status(status, limit=10):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM documents
            WHERE processing_status = ?
            ORDER BY announcement_date DESC
            LIMIT ?
        """, (status, limit)).fetchall()
        return [dict(row) for row in rows]

def debug_print_all_documents():
    with get_conn() as conn:
        for row in conn.execute("SELECT * FROM documents").fetchall():
            print(dict(row))

def mark_document_downloaded(transcript_uuid: str, pdf_file_name: str):
    """discovered -> downloaded"""
    # We update first to downloading to avoid race conditions
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE documents
               SET processing_status='downloaded',
                   updated_at=?,
                   pdf_created_at=?,
                   pdf_file_name=?
             WHERE transcript_uuid=? AND processing_status='discovered'
            """,
            (now_ist_str(), now_ist_str(), pdf_file_name, transcript_uuid)
        )
        if cur.rowcount == 0:
            row = conn.execute(
                "SELECT processing_status FROM documents WHERE transcript_uuid=?",
                (transcript_uuid,)
            ).fetchone()
            if not row:
                raise ValueError(f"Document {transcript_uuid!r} not found")
            raise ValueError(f"Expected 'discovered', found '{row[0]}'")


def mark_document_parsed(transcript_uuid: str, text_file_name: str):
    """downloaded -> parsed"""
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE documents
               SET processing_status='parsed',
                   updated_at=?,
                   text_file_created_at=?,
                   text_file_name=?
             WHERE transcript_uuid=? AND processing_status='downloaded'
            """,
            (now_ist_str(), now_ist_str(), text_file_name, transcript_uuid)
        )
        if cur.rowcount == 0:
            row = conn.execute(
                "SELECT processing_status FROM documents WHERE transcript_uuid=?",
                (transcript_uuid,)
            ).fetchone()
            if not row:
                raise ValueError(f"Document {transcript_uuid!r} not found")
            raise ValueError(f"Expected 'downloaded', found '{row[0]}'")

def mark_document_failed(transcript_uuid: str, error_message: str):
    """status -> failed"""
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE documents
               SET processing_status='failed',
                   updated_at=?,
                   error_message=?
             WHERE transcript_uuid=?
            """,
            (now_ist_str(), error_message, transcript_uuid)
        )