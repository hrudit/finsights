# Tests for finsights.db.connection using in-memory DB fixtures
import pytest
from datetime import datetime
from finsights.db.connection import (
    insert_document,
    get_document_by_transcript_uuid,
    mark_document_downloaded,
    mark_document_parsed,
)

TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S"


def assert_ts(value: str):
    assert isinstance(value, str) and value
    # raises if not in expected format
    datetime.strptime(value, TIMESTAMP_FMT)


def test_insert_document(mock_db_connection, sample_document):
    """Insert a document and verify it exists with discovered status."""
    insert_document(sample_document)

    row = mock_db_connection.execute(
        "SELECT transcript_uuid, company_name, processing_status, created_at, announcement_date, updated_at FROM documents WHERE transcript_uuid = ?",
        (sample_document["uuid"],),
    ).fetchone()

    assert row is not None
    assert row["transcript_uuid"] == sample_document["uuid"]
    assert row["company_name"] == sample_document["company_name"]
    assert row["processing_status"] == "discovered"

    # Timestamp assertions
    assert_ts(row["created_at"])
    assert row["announcement_date"] == sample_document["announcement_date"]
    assert_ts(row["updated_at"])


def test_download_incorrect_then_process_correct(mock_db_connection, sample_document):
    """
    - Insert a document (discovered)
    - Force wrong state (parsed), then calling mark_document_downloaded should error
    - Reset to discovered, mark downloaded, then mark parsed successfully with proper timestamps
    """
    # Insert initial document
    insert_document(sample_document)

    # Force an incorrect state first
    mock_db_connection.execute(
        "UPDATE documents SET processing_status='parsed' WHERE transcript_uuid=?",
        (sample_document["uuid"],),
    )
    mock_db_connection.commit()

    # Attempt to mark as downloaded should fail because status is not discovered
    with pytest.raises(ValueError, match="Expected 'discovered', found 'parsed'"):
        mark_document_downloaded(sample_document["uuid"], "report.pdf")

    # Reset back to discovered to perform valid transitions
    mock_db_connection.execute(
        "UPDATE documents SET processing_status='discovered' WHERE transcript_uuid=?",
        (sample_document["uuid"],),
    )
    mock_db_connection.commit()

    # Now mark as downloaded (valid)
    mark_document_downloaded(sample_document["uuid"], "report.pdf")

    # Check downloaded state + timestamps
    dl_row = mock_db_connection.execute(
        "SELECT processing_status, pdf_file_name, pdf_created_at, updated_at FROM documents WHERE transcript_uuid=?",
        (sample_document["uuid"],),
    ).fetchone()
    assert dl_row["processing_status"] == "downloaded"
    assert dl_row["pdf_file_name"] == "report.pdf"
    assert_ts(dl_row["pdf_created_at"])
    assert_ts(dl_row["updated_at"])

    # Then mark as parsed (valid)
    mark_document_parsed(sample_document["uuid"], "report.txt")

    # Verify final state + timestamps
    final_row = mock_db_connection.execute(
        "SELECT processing_status, pdf_file_name, text_file_name, text_file_created_at, updated_at FROM documents WHERE transcript_uuid=?",
        (sample_document["uuid"],),
    ).fetchone()

    assert final_row["processing_status"] == "parsed"
    assert final_row["pdf_file_name"] == "report.pdf"
    assert final_row["text_file_name"] == "report.txt"
    assert_ts(final_row["text_file_created_at"])
    assert_ts(final_row["updated_at"])
