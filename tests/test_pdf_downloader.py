import pytest
import asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
import aiohttp

from finsights.services.downloader.pdf_downloader import (
    get_pdf_path,
    fetch_pdf,
    download_pdfs
)


class TestGetPdfPath:
    """Test the get_pdf_path function"""
    
    def test_get_pdf_path(self):
        """Test PDF path generation"""
        transcript_uuid = "test-uuid-123"
        expected_path = Path("finsights/pdfs/test-uuid-123.pdf")
        
        with patch('finsights.services.downloader.pdf_downloader.PDF_DIR', Path("finsights/pdfs")):
            result = get_pdf_path(transcript_uuid)
            
            assert result == expected_path
            assert result.name == "test-uuid-123.pdf"


class TestFetchPdf:
    """Test the fetch_pdf async function"""
    
    @pytest.mark.asyncio
    async def test_fetch_pdf_success(self, tmp_path):
        """Test successful PDF download"""
        transcript_uuid = "test-uuid-123"
        url = "https://example.com/test.pdf"
        pdf_content = b"%PDF-1.4\nTest PDF content"
        
        # Mock session and response
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        
        # Create a proper async iterator for iter_chunked
        # An async generator is built to be consumed by an async for loop
        async def mock_iter_chunked(chunk_size):
            yield pdf_content
        
        # Mock the content and iter_chunked method
        mock_response.content = MagicMock()
        mock_response.content.iter_chunked = mock_iter_chunked
        
        # Mock the session.get context manager
        cm = MagicMock(__aenter__=AsyncMock(return_value=mock_response))
        mock_session.get.return_value = cm
        
        with patch('finsights.services.downloader.pdf_downloader.PDF_DIR', tmp_path), \
             patch('builtins.open', mock_open()) as mock_file_opener:
            
            await fetch_pdf(url, transcript_uuid, mock_session)
            
            # Verify session.get was called with correct URL
            mock_session.get.assert_called_once_with(url)
            mock_response.raise_for_status.assert_called_once()
            
            # Verify file was opened for writing
            expected_path = tmp_path / f"{transcript_uuid}.pdf"
            mock_file_opener.assert_called_once_with(expected_path, "wb")

            # Verify file was written
            mock_file_opener.return_value.write.assert_called_once_with(pdf_content)
    
    
    
    @pytest.mark.asyncio
    async def test_fetch_pdf_http_error(self):
        """Test fetch_pdf with HTTP error"""
        transcript_uuid = "test-uuid-123"
        url = "https://example.com/nonexistent.pdf"
        
        # Mock session and response with HTTP error
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
            request_info=MagicMock(),
            history=(),
            status=404
        )
        
        cm = MagicMock(__aenter__=AsyncMock(return_value=mock_response))
        mock_session.get.return_value = cm
        
        with pytest.raises(aiohttp.ClientResponseError):
            await fetch_pdf(url, transcript_uuid, mock_session)
    
    @pytest.mark.asyncio
    async def test_fetch_pdf_multiple_chunks(self, tmp_path):
        """Test fetch_pdf with multiple content chunks"""
        transcript_uuid = "test-uuid-123"
        url = "https://example.com/large.pdf"
        chunk1 = b"First chunk of PDF content"
        chunk2 = b"Second chunk of PDF content"
        chunk3 = b"Third chunk of PDF content"
        
        # Mock session and response
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        
        # Create a proper async iterator for iter_chunked with multiple chunks
        async def mock_iter_chunked(chunk_size):
            yield chunk1
            yield chunk2
            yield chunk3
        
        # Mock the content and iter_chunked method
        mock_response.content = MagicMock()
        mock_response.content.iter_chunked = mock_iter_chunked
        
        # Mock the session.get context manager
        cm = MagicMock(__aenter__=AsyncMock(return_value=mock_response))
        mock_session.get.return_value = cm
        
        with patch('finsights.services.downloader.pdf_downloader.PDF_DIR', tmp_path), \
             patch('builtins.open', mock_open()) as mock_file:
            
            await fetch_pdf(url, transcript_uuid, mock_session)
            
            # Verify all chunks were written
            mock_file.return_value.write.assert_any_call(chunk1)
            mock_file.return_value.write.assert_any_call(chunk2)
            mock_file.return_value.write.assert_any_call(chunk3)
            
            # Verify write was called 3 times (once per chunk)
            assert mock_file.return_value.write.call_count == 3


class TestDownloadPdfs:
    """Test the download_pdfs orchestrator function"""
    
    @pytest.mark.asyncio
    async def test_download_pdfs_success(self):
        """Test successful PDF downloads"""
        transcript_uuids = ["uuid1", "uuid2", "uuid3"]
        urls = ["https://example.com/doc1.pdf", "https://example.com/doc2.pdf", "https://example.com/doc3.pdf"]
        
        with patch('finsights.services.downloader.pdf_downloader.get_pdf_url') as mock_get_url, \
             patch('finsights.services.downloader.pdf_downloader.fetch_pdf') as mock_fetch, \
             patch('finsights.services.downloader.pdf_downloader.mark_document_downloaded') as mock_mark_downloaded, \
             patch('finsights.services.downloader.pdf_downloader.aiohttp.ClientSession') as mock_session_class:
            
            # Setup mocks
            mock_get_url.side_effect = urls
            mock_fetch.return_value = None  # fetch_pdf is async, returns None
            mock_mark_downloaded.return_value = None
            
            # Mock the session
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            
            await download_pdfs(transcript_uuids)
            
            # Verify all PDFs were processed
            assert mock_get_url.call_count == 3
            assert mock_fetch.call_count == 3
            assert mock_mark_downloaded.call_count == 3
    
    @pytest.mark.asyncio
    async def test_download_pdfs_with_failures(self):
        """Test PDF downloads with some failures"""
        transcript_uuids = ["uuid1", "uuid2", "uuid3"]
        urls = ["https://example.com/doc1.pdf", "https://example.com/doc2.pdf", "https://example.com/doc3.pdf"]
        
        with patch('finsights.services.downloader.pdf_downloader.get_pdf_url') as mock_get_url, \
             patch('finsights.services.downloader.pdf_downloader.fetch_pdf') as mock_fetch, \
             patch('finsights.services.downloader.pdf_downloader.mark_document_downloaded') as mock_mark_downloaded, \
             patch('finsights.services.downloader.pdf_downloader.mark_document_failed') as mock_mark_failed, \
             patch('finsights.services.downloader.pdf_downloader.aiohttp.ClientSession') as mock_session_class:
            
            # Setup mocks - first two succeed, third fails
            mock_get_url.side_effect = urls
            mock_fetch.side_effect = [None, None, Exception("Download failed")]
            mock_mark_downloaded.return_value = None
            mock_mark_failed.return_value = None
            
            # Mock the session
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            
            await download_pdfs(transcript_uuids)
            
            # Verify processing
            assert mock_get_url.call_count == 3
            assert mock_fetch.call_count == 3
            assert mock_mark_downloaded.call_count == 2  # Only 2 succeeded
            assert mock_mark_failed.call_count == 1      # 1 failed
    
    @pytest.mark.asyncio
    async def test_download_pdfs_empty_list(self):
        """Test download_pdfs with empty list"""
        with patch('finsights.services.downloader.pdf_downloader.aiohttp.ClientSession') as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value.__aenter__.return_value = mock_session
            
            # Should not raise an exception
            await download_pdfs([])


class TestIntegration:
    """Integration tests for PDF downloader"""
    
    @pytest.mark.asyncio
    async def test_full_download_workflow(self, mock_db_connection, tmp_path):
        """Test complete download workflow with database integration"""
        transcript_uuid = "integration-test-uuid"
        pdf_url = "https://example.com/integration_test.pdf"
        pdf_content = b"%PDF-1.4\nIntegration test PDF content"
        
        # Setup database
        with mock_db_connection:
            mock_db_connection.execute(
                """
                INSERT INTO documents (
                    transcript_uuid, company_name, script_code, pdf_url, pdf_url_sha256,
                    created_at, announcement_date, updated_at, processing_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (transcript_uuid, "Integration Test Company", "ITC123", pdf_url, 
                 "hash456", "2025-01-01T00:00:00", "2025-01-01T00:00:00", 
                 "2025-01-01T00:00:00", "discovered")
            )
        
        with patch('finsights.services.downloader.pdf_downloader.PDF_DIR', tmp_path), \
             patch('finsights.services.downloader.pdf_downloader.aiohttp.ClientSession') as mock_session_class:
            
            # Mock the HTTP response
            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            
            # Create a proper async iterator for iter_chunked
            async def mock_iter_chunked(chunk_size):
                yield pdf_content
            
            mock_response.content = MagicMock()
            mock_response.content.iter_chunked = mock_iter_chunked
            
            cm = MagicMock(__aenter__=AsyncMock(return_value=mock_response))
            mock_session.get.return_value = cm
            mock_session_class.return_value.__aenter__.return_value = mock_session
            
            # Run the download
            await download_pdfs([transcript_uuid])
            
            # Verify PDF file was created
            pdf_path = tmp_path / f"{transcript_uuid}.pdf"
            assert pdf_path.exists()
            assert pdf_path.read_bytes() == pdf_content
            
            # Verify database was updated
            with mock_db_connection:
                cursor = mock_db_connection.execute(
                    "SELECT processing_status, pdf_file_name FROM documents WHERE transcript_uuid = ?",
                    (transcript_uuid,)
                )
                row = cursor.fetchone()
                assert row["processing_status"] == "downloaded"
                assert row["pdf_file_name"] == f"{transcript_uuid}.pdf"
    
    @pytest.mark.asyncio
    async def test_download_workflow_with_failure(self, mock_db_connection):
        """Test download workflow when download fails"""
        transcript_uuid = "failure-test-uuid"
        pdf_url = "https://example.com/nonexistent.pdf"
        
        # Setup database
        with mock_db_connection:
            mock_db_connection.execute(
                """
                INSERT INTO documents (
                    transcript_uuid, company_name, script_code, pdf_url, pdf_url_sha256,
                    created_at, announcement_date, updated_at, processing_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (transcript_uuid, "Failure Test Company", "FTC123", pdf_url, 
                 "hash789", "2025-01-01T00:00:00", "2025-01-01T00:00:00", 
                 "2025-01-01T00:00:00", "discovered")
            )
        
        with patch('finsights.services.downloader.pdf_downloader.aiohttp.ClientSession') as mock_session_class:
            # Mock the session with HTTP error
            mock_session = MagicMock()
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=404
            )
            
            cm = MagicMock(__aenter__=AsyncMock(return_value=mock_response))
            mock_session.get.return_value = cm
            mock_session_class.return_value.__aenter__.return_value = mock_session
            
            # Run the download
            await download_pdfs([transcript_uuid])
            
            # Verify database was updated with failure status
            with mock_db_connection:
                cursor = mock_db_connection.execute(
                    "SELECT processing_status, error_message FROM documents WHERE transcript_uuid = ?",
                    (transcript_uuid,)
                )
                row = cursor.fetchone()
                assert row["processing_status"] == "failed"
                assert "404" in row["error_message"]
