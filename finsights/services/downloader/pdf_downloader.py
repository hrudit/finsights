from time import perf_counter
import aiohttp  
import asyncio
from pathlib import Path
from finsights.config import PDF_DIR, MAX_CONCURRENT_PDF_REQUESTS
from finsights.db.connection import mark_document_failed, mark_document_downloaded, get_pdf_url

def get_pdf_path(transcript_uuid: str) -> Path:
    return PDF_DIR / f"{transcript_uuid}.pdf"


async def download_pdfs(transcript_uuids: list[str]):
    start = perf_counter()
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_PDF_REQUESTS)
    sem = asyncio.Semaphore(MAX_CONCURRENT_PDF_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        async def fetch_pdf_wrapper(url: str, transcript_uuid: str):
            async with sem:
                try:  
                    await fetch_pdf(url, transcript_uuid, session)
                    mark_document_downloaded(transcript_uuid, get_pdf_path(transcript_uuid).name)
                except Exception as e:
                    print(f"Error downloading PDF {url}: {e}")
                    mark_document_failed(transcript_uuid, str(e))
        

        await asyncio.gather(*[fetch_pdf_wrapper(get_pdf_url(transcript_uuid), transcript_uuid) for transcript_uuid in transcript_uuids])

    end = perf_counter()
    print(f"Time taken to download PDFs: {end - start:.2f} seconds")


async def fetch_pdf(url: str, transcript_uuid: str, session: aiohttp.ClientSession):
    if not url:
        raise ValueError(f"PDF URL is not set for transcript {transcript_uuid}")
    async with session.get(url) as resp:
        resp.raise_for_status()
        with open(get_pdf_path(transcript_uuid), "wb") as f:
            async for chunk in resp.content.iter_chunked(1024):
                f.write(chunk)




                



