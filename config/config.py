"""
Configuration file for OHLCV Data Ingestion System
"""
import os
from pathlib import Path

# =========================
# PATHS
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / "credentials"

# Local storage directory
LOCAL_DATA_DIR = BASE_DIR / "data"

# =========================
# GOOGLE DRIVE CONFIG
# =========================
DRIVE_FOLDER_ID = "YOUR DRIVER FOLDER ID (find it in the link of the drive folder eg: https://drive.google.com/drive/folders/XXXXXXXXXXXXXXXXXXXXX)"
DRIVE_FOLDER_NAME = "OHLCV_DATA"

# Google API Scopes
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# =========================
# DATA SCHEMA
# =========================
REQUIRED_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "symbol",
]

# =========================
# Coinbase API CONFIG
# =========================
# Coinbase-supported timeframes (can fetch directly from API)
SUPPORTED_TIMEFRAMES = ["1h", "6h", "1d"]

# Unsupported timeframes (need to be generated from supported ones)
UNSUPPORTED_TIMEFRAMES = ["4h", "1w"]

# All available timeframes
GRANULARITY_MAP = {
    "1h": 3600,
    "4h": 14400,
    "6h": 21600,
    "1d": 86400,
    "1w": 604800,
}

# Mapping of unsupported TF -> source TF needed to generate it
UNSUPPORTED_SOURCE_MAP = {
    "4h": "1h",  # Generate 4h from 1h data
    "1w": "1d",  # Generate 1w from 1d data
}

TF_SHEET_NAMES = {
    "1h": "H1",
    "4h": "H4",
    "6h": "H6",
    "1d": "D1",
    "1w": "W1",
}


COINBASE_BASE_URL = "https://api.exchange.coinbase.com"
API_TIMEOUT = 10  # seconds

# =========================
# GOOGLE SHEETS CONFIG
# =========================
DEFAULT_SHEET_ROWS = 5000
DEFAULT_SHEET_COLS = 20

# =========================
# LOGGING CONFIG
# =========================
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# =========================
# LOCAL STORAGE CONFIG
# =========================
# Whether to save locally by default
DEFAULT_SAVE_LOCAL = True

# Whether to prompt for Drive upload after local save
PROMPT_DRIVE_UPLOAD = True

