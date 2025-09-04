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
DB_PATH = os.getenv("DB_PATH", BASE_DIR / "data.sqlite")