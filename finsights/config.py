import os
from pathlib import Path
# Project level constants and configurations

# Base directory of the project
# resolve() is used to get the absolute path of the file
# parent is used to get the parent directory of the file
# so the db path is valid no matter where the calling code is
BASE_DIR = Path(__file__).resolve().parent

# SQLite database file
# getenv is used to get the environment variable DB_PATH
# if the environment variable is not set, use the default path
DB_PATH = os.getenv("DB_PATH", BASE_DIR / "data.sqlite3")

BSE_BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
BSE_PDF_URL_PAST = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"
BSE_PDF_URL_CURRENT = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
TIMEOUT = 60
MAX_CONCURRENT_JSON_REQUESTS = 10
MAX_CONCURRENT_PDF_REQUESTS = 3
MAX_CONCURRENT_CONVERSION_WORKERS = 10
# BSE headers are used to pass BSE’s checks
# User-Agent is used to identify the browser
# Accept is used to identify the content type
# Referer is used to identify the request is coming from BSE internally
BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/139.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.bseindia.com/corporates/ann.html",
    "Origin": "https://www.bseindia.com",
    "Accept-Language": "en-US,en;q=0.9",
}

PDF_DOWNLOAD_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/139.0.0.0 Safari/537.36"),
    "Referer": "https://www.bseindia.com/corporates/ann.html",
}

BSE_FIXED_PARAMS = {
    "strCat": "-1",        # all categories
    "strType": "C",        # company announcements
    "strSearch": "P",      # PDF search
}

PDF_DIR = BASE_DIR / "pdfs"
TEXT_DIR = BASE_DIR / "text_files"