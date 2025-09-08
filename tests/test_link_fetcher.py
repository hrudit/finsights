import pytest
import pytest_asyncio
import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock
import aiohttp

from finsights.config import BSE_PDF_URL_PAST

from finsights.services.fetcher.link_fetcher import (
    _format_bse_date,
    _fetch_page,
    _is_transcript,
    filter_transcripts_from_json,
    create_transcript_list,
    transcripts_to_dbstate
)


def create_mock_session_with_responses(responses_by_page=None, errors_by_page=None):
    """
    Helper function to create a properly mocked aiohttp.ClientSession with responses or errors.
    
    Args:
        responses_by_page: Dict mapping page numbers to response data
        errors_by_page: Dict mapping page numbers to exceptions to raise
    
    Returns:
        Tuple of (mock_session_class, mock_session) for use with patch
    """
    def mock_get_response(*args, **kwargs):
        page = kwargs.get('params', {}).get('pageno', 1)
        
        # Check if this page should raise an error
        if errors_by_page and page in errors_by_page:
            raise errors_by_page[page]
        
        # Get the response data for this page
        response_data = responses_by_page.get(page, {"Table": [], "Table1": [{"ROWCNT": 0}]})
        
        # Create mock response
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=response_data)
        
        # Return async context manager
        return MagicMock(__aenter__=AsyncMock(return_value=mock_response))
    
    # Create mock session
    mock_session = MagicMock()
    mock_session.get.side_effect = mock_get_response
    
    # The only things that need to be async are the ones that are waited on.
    mock_session_class = MagicMock()
    cm = MagicMock(__aenter__=AsyncMock(return_value=mock_session))
    mock_session_class.return_value = cm
    
    return mock_session_class, mock_session


def create_mock_response(response_data, should_raise_error=None):
    """
    Helper function to create a single mock response for simpler test cases.
    
    Args:
        response_data: The JSON data to return
        should_raise_error: Exception to raise instead of returning data
    
    Returns:
        Mock response object
    """
    if should_raise_error:
        # For error cases, we need to raise the error in the context manager
        cm = MagicMock(__aenter__=AsyncMock(side_effect=should_raise_error))
        return cm
    
    # For success cases
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = AsyncMock(return_value=response_data)
    
    cm = MagicMock(__aenter__=AsyncMock(return_value=mock_response))
    return cm


class TestFormatBseDate:
    """Test the _format_bse_date function"""
    
    def test_format_bse_date(self):
        test_date = date(2025, 1, 29)
        result = _format_bse_date(test_date)
        assert result == "20250129"
    
    def test_format_bse_date_edge_cases(self):
        # Test with single digit month and day
        test_date = date(2025, 1, 5)
        result = _format_bse_date(test_date)
        assert result == "20250105"


class TestIsTranscript:
    """Test the _is_transcript function"""
    
    def test_is_transcript_valid(self):
        ann = {
            "NEWSSUB": "Company Earnings Call Transcript Q4 2024",
            "ATTACHMENTNAME": "earnings_transcript.pdf"
        }
        assert _is_transcript(ann) == True
    
    def test_is_transcript_case_insensitive(self):
        ann = {
            "NEWSSUB": "COMPANY EARNINGS CALL TRANSCRIPT",
            "ATTACHMENTNAME": "TRANSCRIPT.PDF"
        }
        assert _is_transcript(ann) == True
    
    def test_is_transcript_no_transcript_keyword(self):
        ann = {
            "NEWSSUB": "Quarterly Results Announcement",
            "ATTACHMENTNAME": "results.pdf"
        }
        assert _is_transcript(ann) == False
    
    def test_is_transcript_no_pdf_extension(self):
        ann = {
            "NEWSSUB": "earnings call transcript",
            "ATTACHMENTNAME": "transcript.doc"
        }
        assert _is_transcript(ann) == False


    

class TestFilterTranscriptsFromJson:
    """Test the filter_transcripts_from_json function"""
    
    def test_filter_transcripts_valid_data(self):
        json_data = {
            "Table": [
                {
                    "NEWSSUB": "Company A Earnings Call Transcript",
                    "ATTACHMENTNAME": "transcript1.pdf",
                    "SLONGNAME": "Company A Ltd",
                    "SCRIP_CD": "12345",
                    "NEWS_DT": "2025-01-29T10:00:00.000"
                },
                {
                    "NEWSSUB": "Quarterly Results",
                    "ATTACHMENTNAME": "results.pdf",
                    "SLONGNAME": "Company B Ltd",
                    "SCRIP_CD": "67890",
                    "NEWS_DT": "2025-01-29T11:00:00.000"
                },
                {
                    "NEWSSUB": "Company C Earnings Call Transcript",
                    "ATTACHMENTNAME": "transcript2.pdf",
                    "SLONGNAME": "Company C Ltd",
                    "SCRIP_CD": "11111",
                    "NEWS_DT": "2025-01-29T12:00:00.000"
                }
            ]
        }
        
        result = filter_transcripts_from_json(json_data)
        assert len(result) == 2
        assert result[0]["SLONGNAME"] == "Company A Ltd"
        assert result[1]["SLONGNAME"] == "Company C Ltd"
    
    def test_filter_transcripts_empty_table(self):
        json_data = {"Table": []}
        result = filter_transcripts_from_json(json_data)
        assert result == []
    
    def test_filter_transcripts_missing_table(self):
        json_data = {}
        result = filter_transcripts_from_json(json_data)
        assert result == []
    
    def test_filter_transcripts_no_transcripts(self):
        json_data = {
            "Table": [
                {
                    "NEWSSUB": "Quarterly Results",
                    "ATTACHMENTNAME": "results.pdf"
                },
                {
                    "NEWSSUB": "Board Meeting Notice",
                    "ATTACHMENTNAME": "notice.pdf"
                }
            ]
        }
        result = filter_transcripts_from_json(json_data)
        assert result == []


class TestFetchPage:
    """Test the _fetch_page async function"""
    
    @pytest.mark.asyncio
    async def test_fetch_page_success(self):
        # Use helper function to create mock response
        mock_response = create_mock_response({"Table": [], "Table1": [{"ROWCNT": 0}]})
        
        # Create mock session
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        
        # Test the function
        result = await _fetch_page(mock_session, date(2025, 1, 29), date(2025, 1, 29), 1)
        
        assert result == {"Table": [], "Table1": [{"ROWCNT": 0}]}
        mock_session.get.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_fetch_page_timeout_error(self):
        # Use helper function to create error response
        mock_response = create_mock_response(None, should_raise_error=asyncio.TimeoutError())
        
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        
        with pytest.raises(asyncio.TimeoutError):
            await _fetch_page(mock_session, date(2025, 1, 29), date(2025, 1, 29), 1)
    
    @pytest.mark.asyncio
    async def test_fetch_page_http_error(self):
        # Create mock response that raises HTTP error
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
            request_info=MagicMock(), 
            history=(), 
            status=500
        )
        
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_response
        mock_session = MagicMock()
        mock_session.get.return_value = cm
        
        with pytest.raises(aiohttp.ClientResponseError):
            await _fetch_page(mock_session, date(2025, 1, 29), date(2025, 1, 29), 1)


class TestCreateTranscriptList:
    """Test the create_transcript_list async function"""
    
    @pytest.mark.asyncio
    async def test_create_transcript_list_multiple_pages(self):
        # Define responses for each page
        responses_by_page = {
            1: {
                "Table": [
                    {
                        "NEWSSUB": "Company A Earnings Call Transcript",
                        "ATTACHMENTNAME": "transcript1.pdf",
                        "SLONGNAME": "Company A Ltd",
                        "SCRIP_CD": "12345",
                        "NEWS_DT": "2025-01-29T10:00:00.000"
                    }
                ],
                "Table1": [{"ROWCNT": 3}]  # 3 total rows, 1 per page = 3 pages
            },
            2: {
                "Table": [
                    {
                        "NEWSSUB": "Company B Earnings Call Transcript",
                        "ATTACHMENTNAME": "transcript2.pdf",
                        "SLONGNAME": "Company B Ltd",
                        "SCRIP_CD": "67890",
                        "NEWS_DT": "2025-01-29T11:00:00.000"
                    }
                ],
                "Table1": [{"ROWCNT": 3}]
            },
            3: {
                "Table": [
                    {
                        "NEWSSUB": "Company C Earnings Call Transcript",
                        "ATTACHMENTNAME": "transcript3.pdf",
                        "SLONGNAME": "Company C Ltd",
                        "SCRIP_CD": "11111",
                        "NEWS_DT": "2025-01-29T12:00:00.000"
                    }
                ],
                "Table1": [{"ROWCNT": 3}]
            }
        }
        
        # Use helper function to create mock session
        mock_session_class, mock_session = create_mock_session_with_responses(responses_by_page)
        
        with patch('aiohttp.ClientSession', mock_session_class):
            result = await create_transcript_list(date(2025, 1, 29), date(2025, 1, 29))
            
            assert len(result) == 3
            assert result[0]["SLONGNAME"] == "Company A Ltd"
            assert result[1]["SLONGNAME"] == "Company B Ltd"
            assert result[2]["SLONGNAME"] == "Company C Ltd"
    
    @pytest.mark.asyncio
    async def test_create_transcript_list_with_timeout_errors(self):
        # Define responses and errors for each page
        responses_by_page = {
            1: {
                "Table": [
                    {
                        "NEWSSUB": "Company A Earnings Call Transcript",
                        "ATTACHMENTNAME": "transcript1.pdf",
                        "SLONGNAME": "Company A Ltd",
                        "SCRIP_CD": "12345",
                        "NEWS_DT": "2025-01-29T10:00:00.000"
                    }
                ],
                "Table1": [{"ROWCNT": 3}]  # 3 total rows, 1 per page = 3 pages
            }
        }
        
        errors_by_page = {
            2: asyncio.TimeoutError(),
            3: asyncio.TimeoutError()
        }
        
        # Use helper function to create mock session with mixed responses and errors
        mock_session_class, mock_session = create_mock_session_with_responses(
            responses_by_page, errors_by_page
        )
        
        with patch('aiohttp.ClientSession', mock_session_class):
            result = await create_transcript_list(date(2025, 1, 29), date(2025, 1, 29))
            
            # Should still get the transcript from page 1, even though pages 2 and 3 timed out
            assert len(result) == 1
            assert result[0]["SLONGNAME"] == "Company A Ltd"


class TestTranscriptsToDbstate:
    """Test the transcripts_to_dbstate function"""
    
    def test_transcripts_to_dbstate_success(self, mock_db_connection):
        transcript_list = [
            {
                "SLONGNAME": "Test Company Ltd",
                "SCRIP_CD": "12345",
                "ATTACHMENTNAME": "test_transcript.pdf",
                "NEWS_DT": "2025-01-29T10:00:00.000"
            }
        ]
        
        result = transcripts_to_dbstate(transcript_list)
        
        assert len(result) == 1
        assert isinstance(result[0], str)  # UUID string
        
        # Verify the document was actually inserted into the database
        cursor = mock_db_connection.execute("SELECT * FROM documents WHERE transcript_uuid = ?", (result[0],))
        row = cursor.fetchone()
        
        assert row is not None
        assert row["company_name"] == "Test Company Ltd"
        assert row["script_code"] == "12345"
        assert row["pdf_url"] == ( BSE_PDF_URL_PAST + "test_transcript.pdf" )
        assert row["announcement_date"] == "2025-01-29T10:00:00"
        assert row["processing_status"] == "discovered"
    
    def test_transcripts_to_dbstate_duplicate_handling(self, mock_db_connection):
        # First, insert a document to create a duplicate scenario
        transcript_list = [
            {
                "SLONGNAME": "Test Company Ltd",
                "SCRIP_CD": "12345",
                "ATTACHMENTNAME": "test_transcript.pdf",
                "NEWS_DT": "2025-01-29T10:00:00.000"
            }
        ]
        
        # Insert the first transcript
        result1 = transcripts_to_dbstate(transcript_list)
        assert len(result1) == 1
        
        # Try to insert the same transcript again (should be duplicate)
        result2 = transcripts_to_dbstate(transcript_list)
        assert len(result2) == 0  # Should return empty list due to duplicate
        
        # Verify only one document exists in the database
        cursor = mock_db_connection.execute("SELECT COUNT(*) as count FROM documents")
        count = cursor.fetchone()["count"]
        assert count == 1


class TestIntegration:
    """Integration tests combining multiple functions"""
    
    @pytest.mark.asyncio
    async def test_full_workflow_single_transcript(self, mock_db_connection):
        """Test the complete workflow from fetching to database state"""
        # Define single page response
        responses_by_page = {
            1: {
                "Table": [
                    {
                        "NEWSSUB": "Company Earnings Call Transcript Q4 2024",
                        "ATTACHMENTNAME": "earnings_transcript.pdf",
                        "SLONGNAME": "Test Company Ltd",
                        "SCRIP_CD": "12345",
                        "NEWS_DT": "2025-01-29T10:00:00.000"
                    }
                ],
                "Table1": [{"ROWCNT": 1}]
            }
        }
        
        # Use helper function to create mock session
        mock_session_class, mock_session = create_mock_session_with_responses(responses_by_page)
        
        with patch('aiohttp.ClientSession', mock_session_class):
            # Test the full workflow
            transcript_list = await create_transcript_list(date(2025, 1, 29), date(2025, 1, 29))
            transcript_ids = transcripts_to_dbstate(transcript_list)
            
            assert len(transcript_list) == 1
            assert len(transcript_ids) == 1
            assert transcript_list[0]["SLONGNAME"] == "Test Company Ltd"
            assert isinstance(transcript_ids[0], str)
            
            # Verify the document was actually inserted into the database
            cursor = mock_db_connection.execute("SELECT * FROM documents WHERE transcript_uuid = ?", (transcript_ids[0],))
            row = cursor.fetchone()
            
            assert row is not None
            assert row["company_name"] == "Test Company Ltd"
            assert row["script_code"] == "12345"
            assert row["processing_status"] == "discovered"
