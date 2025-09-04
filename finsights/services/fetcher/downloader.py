import asyncio
import aiohttp
import requests
import sqlite3  
from finsights.db.connection import insert_document, debug_print_all_documents
from datetime import date, datetime
import hashlib
import json
import uuid
from finsights.config import BSE_BASE_URL, BSE_HEADERS, BSE_PDF_URL, TIMEOUT
import math

def _format_bse_date(d: date) -> str:
    return d.strftime("%Y%m%d")

BSE_FIXED_PARAMS = {
    "strCat": "-1",        # all categories
    "strType": "C",        # company announcements
    "strToCompany": "0",   # all companies
    "strSearch": "P",       # empty search
}
def save_response_json(resp, filename="debug.json"):
    """Save the requests.Response JSON payload to a file for inspection."""
    data = resp.json()
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved JSON to {filename}")

# format for date is DD/MM/YYYY
# We will need to get the JSON and populate the DB with the documents
# Each fetch page is very slow with most of the time waiting for BSE to respond.
# We are using asyncio to fetch the pages in parallel so we have a bunch of calls waiting together on BSE.
# each call can wait indepdently and not block the other calls.
# 30 pages sequentially would take 30 * 3 = 90 seconds.
# 30 pages async with the first one done seperately to get the number of pages,
# with the remaining 29 pages async, and 10 max at a time to respect bse we have 3*3 + 3 = 12 seconds.
async def _fetch_page(session: aiohttp.ClientSession, prev_date: date, to_date: date, page: int) -> dict[str, any]:
    params = {
        **BSE_FIXED_PARAMS,
        "strPrevDate": _format_bse_date(prev_date),
        "strToDate": _format_bse_date(to_date),
        "Pageno": page,
    }
    async with session.get(BSE_BASE_URL, params=params) as resp:
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
    async with aiohttp.ClientSession(headers=BSE_HEADERS) as session:
        transcript_list = []
        page = 1
        
        json_page_1 = await _fetch_page(session, prev_date, to_date, page)
        transcript_list.extend(filter_transcripts_from_json(json_page_1))

        page_size = len(json_page_1.get("Table",[]))
        total_rows = json_page_1["Table1"][0]["ROWCNT"]
        total_pages = max(1, math.ceil(total_rows / page_size))
        sem = asyncio.Semaphore(10)

        # we are introducing this nested function wrapper so we can cleanly pass the semaphore
        # and filter on the data to get just the transcripts.
        async def fetch_and_filter(p: int) -> list[dict]:
                async with sem:
                    data = await _fetch_page(session, prev_date, to_date, p)
                    print("Retrieved page: ", p)
                    transcript_list = filter_transcripts_from_json(data)
                    print("Number of transcripts: ", len(transcript_list))
                    return transcript_list

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
    
    # page = 2
    # while True:
    #     json_obj = fetch_json_from_bse(prev_date, to_date, page)
    #     print("does the page have rows?", len(json_obj.get("Table",[])) > 0)
    #     print("page", page)
    #     print("Total rows", json_obj["Table1"][0]["ROWCNT"])
    #     print("Number of records in this page",len(json_obj.get("Table",[])))
    #     if not len(json_obj.get("Table",[])) > 0:
    #         break
    #     transcript_list.extend(filter_transcripts_from_json(json_obj))
    #     page += 1

    return transcript_list

def transcripts_to_dbstate(transcript_list: list) -> list:
    transcript_ids = []
    for transcript in transcript_list:
        pdf_url = BSE_PDF_URL + transcript["ATTACHMENTNAME"]
        dt = datetime.strptime(transcript["NEWS_DT"], "%Y-%m-%dT%H:%M:%S.%f")
        formatted = dt.strftime("%Y-%m-%dT%H:%M:%S")
        dbstate = {
            "transcript_uuid": str(uuid.uuid4()),
            "company_name": transcript["SLONGNAME"],
            "script_code": transcript["SCRIP_CD"],
            "pdf_url": pdf_url,
            "pdf_url_sha256": hashlib.sha256(pdf_url.encode()).hexdigest(),
            "json_text": json.dumps(transcript),
            "announcement_date": formatted,
        }
        try:
            insert_document(dbstate)
            transcript_ids.append(dbstate["transcript_uuid"])
        except sqlite3.IntegrityError:
            # Duplicate (pdf_url_sha256 UNIQUE) — skip silently or log
            print(f"Duplicate transcript {pdf_url} found for {transcript['SLONGNAME']}")
            continue
    return transcript_ids


if __name__ == "__main__":
    transcript_list = asyncio.run(create_transcript_list(date(2025, 1, 31), date(2025, 1, 31)))
    print(transcript_list)
    transcript_ids = transcripts_to_dbstate(transcript_list)
    print(transcript_ids)
    debug_print_all_documents()