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
SERVICE_ACCOUNT_PATH = CREDENTIALS_DIR / "ohlcv-486219-a5b71eb1ae70.json"

# Local storage directory
LOCAL_DATA_DIR = BASE_DIR / "data"

# =========================
# GOOGLE DRIVE CONFIG
# =========================
DRIVE_FOLDER_ID = "1j7bx72geBnucQ6km_hfcd_Nl_9d5ofHq"
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
# API CONFIG
# =========================

# Exchange API supported timeframes (REST API)
EXCHANGE_API_TIMEFRAMES = ["5m", "1h", "6h", "1d"]

# Advanced Trade API required timeframes
ADVANCED_TRADE_TIMEFRAMES = ["30m"]

# Aggregated timeframes (created from daily data)
AGGREGATED_TIMEFRAMES = ["1w"]

# All available timeframes
ALL_TIMEFRAMES = ["5m", "30m", "1h", "6h", "1d", "1w"]

# Granularity mapping (in seconds)
GRANULARITY_MAP = {
    "5m": 300,
    "30m": 1800,
    "1h": 3600,
    "6h": 21600,
    "1d": 86400,
    "1w": 604800,  # 7 days
}

TF_SHEET_NAMES = {
    "5m": "M5",
    "30m": "M30",
    "1h": "H1",
    "6h": "H6",
    "1d": "D1",
    "1w": "W1",
}

COINBASE_BASE_URL = "https://api.exchange.coinbase.com"
COINBASE_ADVANCED_TRADE_URL = "https://api.coinbase.com/api/v3/brokerage"
API_TIMEOUT = 10  # seconds

# =========================
# PAGINATION CONFIG
# =========================
# Maximum candles per request (Coinbase limit)
MAX_CANDLES_PER_REQUEST = 300

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