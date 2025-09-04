import pytest
import sqlite3
from pathlib import Path
from unittest.mock import patch
from finsights.db.connection import get_conn


@pytest.fixture
def in_memory_db():
    """Create an in-memory SQLite database with schema"""
    # Create in-memory database
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    
    # Read and execute schema
    schema_path = Path(__file__).parent.parent / "finsights" / "db" / "sql" / "table_schema.sql"
    schema_sql = schema_path.read_text()
    conn.executescript(schema_sql)
    
    # We use yield in a fixture to perform clean up once the 
    # test is done.
    yield conn
    conn.close()

# This is a pure side effect fixture
# The inner class mock_get_conn is a context manager that returns a mock connection to the in-memory database
# this is so that it stays valid using the with syntax that is used in our connection.py file
@pytest.fixture
def mock_db_connection(in_memory_db):
    """Mock the get_conn context manager to use in-memory database"""
    def mock_get_conn():
        class MockConn:
            def __enter__(self):
                return in_memory_db
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type:
                    in_memory_db.rollback()
                else:
                    in_memory_db.commit()
        
        return MockConn()
    
    with patch('finsights.db.connection.get_conn', side_effect=mock_get_conn):
        yield in_memory_db
        # once we exit the with block, the path will no longer exist as the 
        #  __exit__ method in patch will be called.


@pytest.fixture
def sample_document():
    """Sample document data for testing"""
    return {
        "uuid": "test-uuid-123",
        "company_name": "Test Corp",
        "script_code": "TC",
        "pdf_url": "https://example.com/test.pdf",
        "pdf_url_sha256": "abc123def456",
        "json_text": '{"test": "data", "content": "sample"}',
        "announcement_date": "2025-01-01T00:00:00"
    }


@pytest.fixture
def populated_db(mock_db_connection, sample_document):
    """Database with sample data inserted"""
    from finsights.db.connection import insert_document
    
    # Insert test document
    insert_document(sample_document)
    
    # Default test documents
    test_docs = [
        {
            "uuid": "downloaded-uuid-456",
            "company_name": "Downloaded Corp",
            "script_code": "DC",
            "pdf_url": "https://example.com/downloaded.pdf",
            "pdf_url_sha256": "downloaded123",
            "json_text": '{"status": "downloaded"}',
            "announcement_date": "2025-01-02T00:00:00"
        },
        {
            "uuid": "parsed-uuid-789",
            "company_name": "Parsed Corp", 
            "script_code": "PC",
            "pdf_url": "https://example.com/parsed.pdf",
            "pdf_url_sha256": "parsed123",
            "json_text": '{"status": "parsed"}',
            "announcement_date": "2025-01-03T00:00:00"
        }
    ]
    
    for doc in test_docs:
        insert_document(doc)
    yield mock_db_connection
