import asyncio
from datetime import date
from finsights.services.downloader.pdf_downloader import download_pdfs
from finsights.services.fetcher.link_fetcher import create_transcript_list, transcripts_to_dbstate
from finsights.services.converter.pdf_to_text import convert_pdfs

if __name__ == "__main__":
    transcript_list = asyncio.run(create_transcript_list(date(2025, 1, 17), date(2025, 1, 17)))
    transcript_uuids = transcripts_to_dbstate(transcript_list)
    asyncio.run(download_pdfs(transcript_uuids))
    convert_pdfs()