import asyncio
import hashlib
import math
import sqlite3
from time import perf_counter
import uuid
from datetime import date, datetime, timedelta

import aiohttp

from finsights.config import BSE_BASE_URL, BSE_HEADERS, BSE_PDF_URL_PAST, BSE_PDF_URL_CURRENT, TIMEOUT, MAX_CONCURRENT_JSON_REQUESTS, BSE_FIXED_PARAMS
from finsights.db.connection import insert_document, debug_print_all_documents

def _format_bse_date(d: date) -> str:
    return d.strftime("%Y%m%d")


# def save_response_json(resp, filename="debug.json"):
#     """Save the requests.Response JSON payload to a file for inspection."""
#     data = resp.json()
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(data, f, ensure_ascii=False, indent=2)
#     print(f"Saved JSON to {filename}")

# Format for date is DD/MM/YYYY
# We will need to get the JSON and populate the DB with the documents
# Each fetch page is very slow with most of the time waiting for BSE to respond.
# We are using asyncio to fetch the pages in parallel so we have a bunch of calls waiting together on BSE.
# Each call can wait independently and not block the other calls.
# 30 pages sequentially would take 30 * 15 = 450 seconds.
# 30 pages async with the first one done separately to get the number of pages,
# with the remaining 29 pages async, and 10 max at a time to respect BSE we have 3*15 + 15 = 60 seconds.
async def _fetch_page(
    session: aiohttp.ClientSession, 
    prev_date: date, 
    to_date: date, 
    page: int
) -> dict[str, any]:
    params = {
        **BSE_FIXED_PARAMS,
        "strPrevDate": _format_bse_date(prev_date),
        "strToDate": _format_bse_date(to_date),
        "pageno": page,
    }
    async with session.get(BSE_BASE_URL, params=params, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        return await resp.json()


def _is_transcript(ann: dict) -> bool:
    newssub = (ann.get("NEWSSUB") or "").strip().lower()
    attachment = (ann.get("ATTACHMENTNAME") or "").strip().lower()
    
    looks_like_transcript = "earnings call transcript" in newssub
    return looks_like_transcript and attachment.endswith(".pdf")


def filter_transcripts_from_json(json: dict) -> list:
    transcripts = []
    all_announcements = json.get("Table", [])
    for announcement in all_announcements:
        if _is_transcript(announcement):
            transcripts.append(announcement)
    return transcripts


async def create_transcript_list(prev_date: date, to_date: date):
    # 🔹 TCPConnector controls the connection pool,
    # how many outgoing connections a session can have

    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_JSON_REQUESTS)
    async with aiohttp.ClientSession(headers=BSE_HEADERS, connector=connector) as session:
        transcript_list = []
        page = 1
        
        json_page_1 = await _fetch_page(session, prev_date, to_date, page)
        transcript_list.extend(filter_transcripts_from_json(json_page_1))

        page_size = len(json_page_1.get("Table", []))
        total_rows = json_page_1["Table1"][0]["ROWCNT"]
        total_pages = max(1, math.ceil(total_rows / page_size))
        sem = asyncio.Semaphore(MAX_CONCURRENT_JSON_REQUESTS)

        # We are introducing this nested function wrapper so we can cleanly pass the semaphore
        # and filter on the data to get just the transcripts.
        # We catch errors and return empty lists to avoid stopping the gather call and processing as many
        # pages as possible.
        async def fetch_and_filter(p: int) -> list[dict]:
            async with sem:
                try:
                    start_time = perf_counter()
                    data = await _fetch_page(session, prev_date, to_date, p)
                    end_time = perf_counter()
                    print(f"Time taken to retrieve page {p}: {end_time - start_time:.2f} seconds")
                    transcript_list = filter_transcripts_from_json(data)
                    print("Number of transcripts: ", len(transcript_list))
                    return transcript_list
                except asyncio.TimeoutError:
                    print(f"Timeout error on page {p} - returning empty results")
                    return []
                except aiohttp.ClientTimeout:
                    print(f"Client timeout on page {p} - returning empty results")
                    return []
                except Exception as e:
                    print(f"Error fetching page {p}: {e} - returning empty results")
                    return []

        if total_pages > 1:
            list_of_filtered_json_pages = await asyncio.gather(
                *[
                    fetch_and_filter(page)
                    for page in range(2, total_pages + 1)
                ]
            )
            for filtered_json_page in list_of_filtered_json_pages:
                transcript_list.extend(filtered_json_page)

    return transcript_list



def transcripts_to_dbstate(transcript_list: list) -> list:
    transcript_ids = []
    for transcript in transcript_list:
        
        dt = datetime.strptime(transcript["NEWS_DT"], "%Y-%m-%dT%H:%M:%S.%f")
        if dt < datetime.now() - timedelta(days=60):
            pdf_url = BSE_PDF_URL_PAST + transcript["ATTACHMENTNAME"]
        else:
            pdf_url = BSE_PDF_URL_CURRENT + transcript["ATTACHMENTNAME"]

        formatted = dt.strftime("%Y-%m-%dT%H:%M:%S")
        dbstate = {
            "transcript_uuid": str(uuid.uuid4()),
            "company_name": transcript["SLONGNAME"],
            "script_code": transcript["SCRIP_CD"],
            "pdf_url": pdf_url,
            "pdf_url_sha256": hashlib.sha256(pdf_url.encode()).hexdigest(),
            "announcement_date": formatted,
        }
        try:
            insert_document(dbstate)
            transcript_ids.append(dbstate["transcript_uuid"])
        except sqlite3.IntegrityError:
            # Duplicate (pdf_url_sha256 UNIQUE) — skip silently or log
            print(f"Error saving transcript {pdf_url} for {transcript['SLONGNAME']}")
            continue
    return transcript_ids


if __name__ == "__main__":

    start_time = perf_counter()
    transcript_list = asyncio.run(
        create_transcript_list(date(2025, 1, 29), date(2025, 1, 29))
    )
    end_time = perf_counter()
    # print(transcript_list)
    # transcript_ids = transcripts_to_dbstate(transcript_list)
    # print(transcript_ids)
    # debug_print_all_documents()
    
    print(f"Total execution time with semaphore {MAX_CONCURRENT_JSON_REQUESTS}: {end_time - start_time:.2f} seconds")