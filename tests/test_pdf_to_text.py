import pytest
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from pypdf import PdfReader

from finsights.services.converter.pdf_to_text import convert_pdf_to_text, convert_pdfs


class TestConvertPdfToText:
    """Test the convert_pdf_to_text function"""
    
    def test_convert_pdf_to_text_success(self, mock_db_connection, tmp_path):
        """Test successful PDF to text conversion"""
        # Setup test data
        transcript_uuid = "test-uuid-123"
        pdf_file_name = "test_document.pdf"
        text_file_name = f"{transcript_uuid}.txt"
        
        # Create mock document in database
        with mock_db_connection:
            mock_db_connection.execute(
                """
                INSERT INTO documents (
                    transcript_uuid, company_name, script_code, pdf_url, pdf_url_sha256,
                    created_at, announcement_date, updated_at, processing_status, pdf_file_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (transcript_uuid, "Test Company", "TC123", "http://test.com/doc.pdf", 
                 "hash123", "2025-01-01T00:00:00", "2025-01-01T00:00:00", 
                 "2025-01-01T00:00:00", "downloaded", pdf_file_name)
            )
        
        # Create a simple PDF file for testing
        pdf_content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        pdf_path = tmp_path / pdf_file_name
        pdf_path.write_bytes(pdf_content)
        
        # Mock the database functions
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc, \
             patch('finsights.services.converter.pdf_to_text.mark_document_parsed') as mock_mark_parsed, \
             patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path), \
             patch('finsights.services.converter.pdf_to_text.TEXT_DIR', tmp_path):
            
            # Mock document data
            mock_get_doc.return_value = {
                "transcript_uuid": transcript_uuid,
                "pdf_file_name": pdf_file_name
            }
            
            # Mock PdfReader to return test content
            with patch('finsights.services.converter.pdf_to_text.PdfReader') as mock_reader:
                mock_page = MagicMock()
                mock_page.extract_text.return_value = "This is test PDF content"
                mock_reader.return_value.pages = [mock_page]
                
                # Run the conversion
                result = convert_pdf_to_text(transcript_uuid)
                
                # Assertions
                assert result is True
                mock_mark_parsed.assert_called_once_with(transcript_uuid, text_file_name)
                
                # Check that text file was created
                text_path = tmp_path / text_file_name
                assert text_path.exists()
                
                # Check that PDF was deleted
                assert not pdf_path.exists()
    
    def test_convert_pdf_to_text_document_not_found(self):
        """Test conversion when document is not found in database"""
        transcript_uuid = "nonexistent-uuid"
        
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc:
            mock_get_doc.return_value = None
            
            result = convert_pdf_to_text(transcript_uuid)
            
            assert result is False
    
    
    def test_convert_pdf_to_text_pdf_file_not_found(self, tmp_path):
        """Test conversion when PDF file doesn't exist"""
        transcript_uuid = "test-uuid-123"
        pdf_file_name = "nonexistent.pdf"
        
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc, \
             patch('finsights.services.converter.pdf_to_text.mark_document_failed') as mock_mark_failed, \
             patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path):
            
            mock_get_doc.return_value = {
                "transcript_uuid": transcript_uuid,
                "pdf_file_name": pdf_file_name
            }
            
            result = convert_pdf_to_text(transcript_uuid)
            
            assert result is False
            mock_mark_failed.assert_called_once_with(
                transcript_uuid, 
                f"PDF file not found: {tmp_path / pdf_file_name}"
            )
    
    def test_convert_pdf_to_text_pdf_parsing_error(self, tmp_path):
        """Test conversion when PDF parsing fails"""
        transcript_uuid = "test-uuid-123"
        pdf_file_name = "corrupted.pdf"
        
        # Create a corrupted PDF file
        pdf_path = tmp_path / pdf_file_name
        pdf_path.write_text("This is not a valid PDF")
        
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc, \
             patch('finsights.services.converter.pdf_to_text.mark_document_failed') as mock_mark_failed, \
             patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path), \
             patch('finsights.services.converter.pdf_to_text.TEXT_DIR', tmp_path):
            
            mock_get_doc.return_value = {
                "transcript_uuid": transcript_uuid,
                "pdf_file_name": pdf_file_name
            }
            
            result = convert_pdf_to_text(transcript_uuid)
            
            assert result is False
            mock_mark_failed.assert_called_once()
    
    def test_convert_pdf_to_text_page_extraction_error(self, tmp_path):
        """Test conversion when individual page extraction fails"""
        transcript_uuid = "test-uuid-123"
        pdf_file_name = "test_document.pdf"
        
        # Create a simple PDF file
        pdf_content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        pdf_path = tmp_path / pdf_file_name
        pdf_path.write_bytes(pdf_content)
        
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc, \
             patch('finsights.services.converter.pdf_to_text.mark_document_parsed') as mock_mark_parsed, \
             patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path), \
             patch('finsights.services.converter.pdf_to_text.TEXT_DIR', tmp_path):
            
            mock_get_doc.return_value = {
                "transcript_uuid": transcript_uuid,
                "pdf_file_name": pdf_file_name
            }
            
            # Mock PdfReader with pages that throw exceptions
            with patch('finsights.services.converter.pdf_to_text.PdfReader') as mock_reader:
                mock_page = MagicMock()
                mock_page.extract_text.side_effect = Exception("Page extraction failed")
                mock_reader.return_value.pages = [mock_page]
                
                result = convert_pdf_to_text(transcript_uuid)
                
                # Should still succeed (continues on page errors)
                assert result is True
                mock_mark_parsed.assert_called_once()
    
    def test_convert_pdf_to_text_multiple_pages(self, tmp_path):
        """Test conversion with multiple pages"""
        transcript_uuid = "test-uuid-123"
        pdf_file_name = "multi_page_document.pdf"
        text_file_name = f"{transcript_uuid}.txt"
        
        # Create a simple PDF file
        pdf_content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        pdf_path = tmp_path / pdf_file_name
        pdf_path.write_bytes(pdf_content)
        
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc, \
             patch('finsights.services.converter.pdf_to_text.mark_document_parsed') as mock_mark_parsed, \
             patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path), \
             patch('finsights.services.converter.pdf_to_text.TEXT_DIR', tmp_path):
            
            mock_get_doc.return_value = {
                "transcript_uuid": transcript_uuid,
                "pdf_file_name": pdf_file_name
            }
            
            # Mock PdfReader with multiple pages
            with patch('finsights.services.converter.pdf_to_text.PdfReader') as mock_reader:
                mock_page1 = MagicMock()
                mock_page1.extract_text.return_value = "Page 1 content"
                mock_page2 = MagicMock()
                mock_page2.extract_text.return_value = "Page 2 content"
                mock_reader.return_value.pages = [mock_page1, mock_page2]
                
                result = convert_pdf_to_text(transcript_uuid)
                
                assert result is True
                mock_mark_parsed.assert_called_once_with(transcript_uuid, text_file_name)
                
                # Check text file content
                text_path = tmp_path / text_file_name
                assert text_path.exists()
                content = text_path.read_text()
                assert "Page 1 content" in content
                assert "Page 2 content" in content
                assert "--- Page 1 ---" in content
                assert "--- Page 2 ---" in content
    
    def test_convert_pdf_to_text_pdf_deletion_failure(self, tmp_path):
        """Test conversion when PDF deletion fails"""
        transcript_uuid = "test-uuid-123"
        pdf_file_name = "test_document.pdf"
        
        # Create a simple PDF file
        pdf_content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        pdf_path = tmp_path / pdf_file_name
        pdf_path.write_bytes(pdf_content)
        
        with patch('finsights.services.converter.pdf_to_text.get_document_by_transcript_uuid') as mock_get_doc, \
             patch('finsights.services.converter.pdf_to_text.mark_document_parsed') as mock_mark_parsed, \
             patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path), \
             patch('finsights.services.converter.pdf_to_text.TEXT_DIR', tmp_path):
            
            mock_get_doc.return_value = {
                "transcript_uuid": transcript_uuid,
                "pdf_file_name": pdf_file_name
            }
            
            # Mock PdfReader
            with patch('finsights.services.converter.pdf_to_text.PdfReader') as mock_reader:
                mock_page = MagicMock()
                mock_page.extract_text.return_value = "Test content"
                mock_reader.return_value.pages = [mock_page]
                
                # Mock unlink to raise an exception
                with patch('pathlib.Path.unlink', side_effect=OSError("Permission denied")):
                    result = convert_pdf_to_text(transcript_uuid)
                    
                    # Should still succeed (PDF deletion failure is not critical)
                    assert result is True
                    mock_mark_parsed.assert_called_once()


class TestConvertPdfs:
    """Test the convert_pdfs orchestrator function"""
    
    def test_convert_pdfs_no_documents(self):
        """Test when there are no downloaded documents to process"""
        with patch('finsights.services.converter.pdf_to_text.get_downloaded_documents') as mock_get_docs:
            mock_get_docs.return_value = []
            
            # Should not raise an exception
            convert_pdfs()
    
    def test_convert_pdfs_with_documents(self):
        """Test conversion with multiple documents"""
        transcript_uuids = ["uuid1", "uuid2", "uuid3"]
        
        with patch('finsights.services.converter.pdf_to_text.get_downloaded_documents') as mock_get_docs, \
             patch('finsights.services.converter.pdf_to_text.multiprocessing.Pool') as mock_pool_class:
            
            mock_get_docs.return_value = transcript_uuids
            
            # Mock the pool and its methods
            mock_pool = MagicMock()
            mock_pool_class.return_value.__enter__.return_value = mock_pool
            mock_pool.map.return_value = [True, True, False]  # 2 success, 1 failure
            
            convert_pdfs()
            
            # Verify pool was created with correct number of processes
            mock_pool_class.assert_called_once()
            mock_pool.map.assert_called_once()


class TestIntegration:
    """Integration tests for PDF to text conversion"""
    
    def test_full_conversion_workflow(self, mock_db_connection, tmp_path):
        """Test the complete workflow from database to text file"""
        transcript_uuid = "integration-test-uuid"
        pdf_file_name = "integration_test.pdf"
        
        # Setup database
        with mock_db_connection:
            mock_db_connection.execute(
                """
                INSERT INTO documents (
                    transcript_uuid, company_name, script_code, pdf_url, pdf_url_sha256,
                    created_at, announcement_date, updated_at, processing_status, pdf_file_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (transcript_uuid, "Integration Test Company", "ITC123", "http://test.com/doc.pdf", 
                 "hash456", "2025-01-01T00:00:00", "2025-01-01T00:00:00", 
                 "2025-01-01T00:00:00", "downloaded", pdf_file_name)
            )
        
        # Create test PDF
        pdf_content = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n"
        pdf_path = tmp_path / pdf_file_name
        pdf_path.write_bytes(pdf_content)
        
        with patch('finsights.services.converter.pdf_to_text.PDF_DIR', tmp_path), \
             patch('finsights.services.converter.pdf_to_text.TEXT_DIR', tmp_path):
            
            # Mock PdfReader
            with patch('finsights.services.converter.pdf_to_text.PdfReader') as mock_reader:
                mock_page = MagicMock()
                mock_page.extract_text.return_value = "Integration test content"
                mock_reader.return_value.pages = [mock_page]
                
                result = convert_pdf_to_text(transcript_uuid)
                
                assert result is True
                
                # Verify text file was created
                text_path = tmp_path / f"{transcript_uuid}.txt"
                assert text_path.exists()
                
                # Verify PDF was deleted
                assert not pdf_path.exists()
                
                # Verify database was updated
                with mock_db_connection:
                    cursor = mock_db_connection.execute(
                        "SELECT processing_status, text_file_name FROM documents WHERE transcript_uuid = ?",
                        (transcript_uuid,)
                    )
                    row = cursor.fetchone()
                    assert row["processing_status"] == "parsed"
                    assert row["text_file_name"] == f"{transcript_uuid}.txt"
