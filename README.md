# 📊 OHLCV Data Ingestion System

A professional Python-based data pipeline for fetching, processing, and storing OHLCV (Open, High, Low, Close, Volume) cryptocurrency data from Coinbase Exchange with optional Google Drive backup.

## ✨ Features

- 🔄 **Multi-Timeframe Support**: 5m, 30m, 1h, 6h, 1d, 1w
- 📥 **Dual API Integration**: 
  - Coinbase Exchange API (5m, 1h, 6h, 1d)
  - Coinbase Advanced Trade API (30m) with CDP authentication
- 🔧 **Smart Weekly Aggregation**: Generates 1w timeframe from 1d data (Sunday-Sunday)
- 💾 **Local CSV Storage**: Stores data locally with automatic deduplication
- ☁️ **Google Drive Backup**: Optional upload to Google Sheets for cloud storage
- 🔐 **Dual Authentication**: 
  - OAuth2 for Google Drive/Sheets
  - CDP API key for Advanced Trade API (30m data)
- ✅ **Data Validation**: Comprehensive validation and error handling
- 📊 **Progress Tracking**: Real-time progress indicators and detailed logging
- 🔄 **Automatic Pagination**: Fetches historical data across multiple years

## 📋 Table of Contents

- [Installation](#installation)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Authentication Setup](#authentication-setup)
- [Usage](#usage)
- [Timeframe Support](#timeframe-support)
- [File Structure](#file-structure)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Google Cloud account (optional, for Drive backup)

### Clone Repository

```bash
git clone https://github.com/Melek390/OHLCV-data
cd OHLCV-data
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Required Packages

```
pandas>=1.3.0
requests>=2.26.0
gspread>=5.0.0
google-auth>=2.0.0
google-auth-oauthlib>=0.5.0
google-auth-httplib2>=0.1.0
google-api-python-client>=2.0.0
PyJWT>=2.8.0
cryptography>=41.0.0
```

## 📁 Project Structure

```
ohlcv-ingestion-system/
│
├── config/
│   └── config.py                 # Configuration settings
│
├── credentials/
│   ├── cdp_api_key.json         # Coinbase CDP API credentials (for 30m data)
│   ├── oauth_credentials.json    # OAuth2 credentials (for Google Drive)
│   └── token.pickle              # Cached OAuth tokens
│
├── data/                         # Local CSV storage
│   └── coinbase/
│       ├── BTC-USD_5m.csv
│       ├── BTC-USD_30m.csv
│       ├── BTC-USD_1h.csv
│       ├── BTC-USD_6h.csv
│       ├── BTC-USD_1d.csv
│       ├── BTC-USD_1w.csv
│       └── ...
│
├── drive/
│   ├── __init__.py
│   ├── data_manager.py           # Data transformation & validation
│   └── sheets.py                 # Google Sheets operations
│
├── exchanges/
│   └── coinbase/
│       ├── __init__.py
│       ├── coinbase.py           # Coinbase Exchange API (5m, 1h, 6h, 1d)
│       ├── advanced_trade.py     # Coinbase Advanced Trade API (30m)
│       └── weekly_aggregator.py  # Weekly aggregation (1d → 1w)
│
├── storage/
│   ├── __init__.py
│   └── local_storage.py          # Local CSV storage manager
│
├── utils/
│   ├── __init__.py
│   ├── exceptions.py             # Custom exceptions
│   ├── logger.py                 # Logging utility
│   └── oauth_auth.py             # OAuth2 authentication
│
├── main.py                       # Main application entry point
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## ⚙️ Configuration

### Basic Configuration

Edit `config/config.py` to customize your setup:

```python
# Local storage directory
LOCAL_DATA_DIR = BASE_DIR / "data"

# Google Drive folder ID (find in folder URL)
DRIVE_FOLDER_ID = "your-folder-id-here"

# Timeframes configuration
EXCHANGE_API_TIMEFRAMES = ["5m", "1h", "6h", "1d"]      # Coinbase Exchange API
ADVANCED_TRADE_TIMEFRAMES = ["30m"]                      # Coinbase Advanced Trade API
AGGREGATED_TIMEFRAMES = ["1w"]                           # Generated from daily data
ALL_TIMEFRAMES = ["5m", "30m", "1h", "6h", "1d", "1w"]  # All available
```

### API Configuration

The system uses two different Coinbase APIs:

1. **Coinbase Exchange API** (Public, no auth needed)
   - Timeframes: 5m, 1h, 6h, 1d
   - Endpoint: `https://api.exchange.coinbase.com`

2. **Coinbase Advanced Trade API** (Requires CDP API key)
   - Timeframe: 30m only
   - Authentication: JWT with EC private key
   - Credentials: `credentials/cdp_api_key.json`

### Authentication Mode

In `main.py`, choose your Google authentication method:

```
USE_OAUTH = True
```

## 🔐 Authentication Setup

### OAuth2 for Google Drive/Sheets (Recommended)

1. **Go to Google Cloud Console**: https://console.cloud.google.com/
2. **Create a new project** or select existing one
3. **Create OAuth2 credentials**:
   - Go to "APIs & Services" → "Credentials" → "Create Credentials" → "OAuth client ID"
   - Application type: "Desktop app"
   - Download the JSON file
4. **Enable APIs**:
   - Google Sheets API
   - Google Drive API
5. **Save credentials**:
   - Rename to `oauth_credentials.json`
   - Place in `credentials/` directory
6. **Add user email** (if using test mode):
   - Go to "OAuth consent screen" → "Test users"
   - Add your email
7. **First run**: Browser will open for authorization
   - Sign in to your Google account
   - Grant permissions
   - Token will be saved for future use

### Coinbase CDP API for 30m Data

1. **Go to Coinbase Developer Portal**: https://portal.cdp.coinbase.com/
2. **Create API Key**:
   - Navigate to "API Keys" section
   - Click "Create API Key" (for algorithm: choose ECDSA)
   - Select required permissions (read access for market data)
3. **Download credentials**:
   - Save the JSON file containing `name` and `privateKey`
4. **Save to project**:
   - Rename to `cdp_api_key.json`
   - Place in `credentials/` directory

**Example `cdp_api_key.json` format:**
```json
{
  "name": "organizations/YOUR_ORG_ID/apiKeys/YOUR_KEY_ID",
  "privateKey": "-----BEGIN EC PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END EC PRIVATE KEY-----"
}
```

> **Note**: The 30m timeframe REQUIRES CDP API credentials. All other timeframes use the public Exchange API.

## 🎯 Usage

### Basic Usage

```bash
python main.py
```

### Step-by-Step Workflow

1. **Select Exchange**: Choose data source (currently supports Coinbase)
2. **Enter Trading Pair**: e.g., `BTC-USD`, `ETH-USD`
3. **Select Timeframes**: Comma-separated, e.g., `1h,4h,1d`
4. **Data Fetching**: System fetches and processes data
5. **Local Storage**: Data saved to CSV files
6. **Drive Upload**: Optional - choose to upload to Google Drive

### Example Session

```
📊 OHLCV Data Ingestion System v4.2
======================================================================
   Professional REST → Local CSV → Google Sheets Pipeline
   📁 Local storage with deduplication
   ☁️  Optional Drive backup
   📈 Supports: 5m, 30m, 1h, 6h, 1d, 1w
   🔄 Automatic pagination for historical data
   🔑 30m data via CDP API (cdp_api_key.json)
======================================================================

🏢 Available Exchanges:
  • Coinbase

Select exchange (default: coinbase): 

📈 Enter trading pair (default: BTC-USD): ETH-USD

⏱️  Available Timeframes:
  5m, 30m, 1h, 6h, 1d, 1w

  📌 Notes:
    • 5m, 1h, 6h, 1d: Exchange API (REST)
    • 30m: Advanced Trade API (requires CDP credentials)
    • 1w: Aggregated from daily data (Sunday-Sunday)

Enter timeframes (comma-separated, e.g., 5m,1h,1d): 1h,30m,1w

📅 Historical Data Range (Optional)
Start year (e.g., 2022) [Press Enter to skip]: 2024
End year (e.g., 2024) [Press Enter to skip]: 2024

✅ Will fetch data from Jan 1, 2024 to Dec 31, 2024

📥 FETCHING & STORING DATA LOCALLY
======================================================================

📊 Processing Exchange API timeframes...

⏳ Processing 1H timeframe...
  🌐 Fetching 8784 candles from API
  ✅ Saved 8760 new rows to: ETH-USD_1h.csv

📊 Processing Advanced Trade API timeframes...

⏳ Processing 30M timeframe...
  🔑 Using CDP API authentication
  🌐 Fetching historical 30m data
  ✅ Saved 17520 new rows to: ETH-USD_30m.csv

📊 Processing Weekly aggregated timeframes...

⏳ Processing 1W timeframe...
  🔄 Aggregated 365 daily → 52 weekly candles (Sunday-Sunday)
  ✅ Saved 52 new rows to: ETH-USD_1w.csv

☁️  GOOGLE DRIVE BACKUP
Upload to Drive? (y/n): y
✅ All data uploaded successfully!
```

## ⏱️ Timeframe Support

### Exchange API Timeframes (Direct Fetch - No Auth Required)

| Timeframe | Description | Granularity | API Endpoint |
|-----------|-------------|-------------|--------------|
| **5m** | 5 Minutes | 300s | Coinbase Exchange API |
| **1h** | 1 Hour | 3600s | Coinbase Exchange API |
| **6h** | 6 Hours | 21600s | Coinbase Exchange API |
| **1d** | 1 Day | 86400s | Coinbase Exchange API |

### Advanced Trade API Timeframes (CDP Auth Required)

| Timeframe | Description | Granularity | Authentication |
|-----------|-------------|-------------|----------------|
| **30m** | 30 Minutes | "THIRTY_MINUTE" | CDP API Key (JWT) |

### Aggregated Timeframes (Generated from Daily Data)

| Timeframe | Description | Source | Aggregation Method |
|-----------|-------------|--------|-------------------|
| **1w** | 1 Week | 1d data | Sunday-Sunday grouping |

### Weekly Aggregation Details

The system aggregates daily candles into weekly candles with the following logic:

- **Week Boundaries**: Sunday 00:00 to next Sunday 00:00
- **Timestamp Label**: Start of week (Sunday midnight)
- **Open**: First daily open of the week (Sunday)
- **High**: Maximum daily high of the week
- **Low**: Minimum daily low of the week
- **Close**: Last daily close of the week (Saturday)
- **Volume**: Sum of daily volumes

**Important**: 
- Weekly timestamp represents the START of the week, not the end
- Aligns with TradingView's weekly chart timestamps
- First hourly candle of the week ends at Monday 1 AM

## 📂 File Structure

### Local CSV Format

All CSV files follow this schema:

```csv
timestamp,open,high,low,close,volume,symbol
2024-01-01T00:00:00+00:00,42000.0,42500.0,41800.0,42300.0,1234.56,BTC/USD
2024-01-01T01:00:00+00:00,42300.0,42800.0,42100.0,42600.0,1567.89,BTC/USD
```

### Directory Structure

```
data/
└── coinbase/
    ├── BTC-USD_5m.csv    # 5-minute Bitcoin data
    ├── BTC-USD_30m.csv   # 30-minute Bitcoin data (Advanced Trade API)
    ├── BTC-USD_1h.csv    # 1-hour Bitcoin data
    ├── BTC-USD_6h.csv    # 6-hour Bitcoin data
    ├── BTC-USD_1d.csv    # Daily Bitcoin data
    ├── BTC-USD_1w.csv    # Weekly Bitcoin data (aggregated, Sunday-Sunday)
    ├── ETH-USD_5m.csv    # 5-minute Ethereum data
    └── ...
```

## 🔧 API Reference

### Coinbase Exchange API Client

```python
from exchanges.coinbase import fetch_ohlcv, validate_symbol

# Fetch OHLCV data (5m, 1h, 6h, 1d)
data = fetch_ohlcv(
    symbol="BTC-USD",
    timeframe="1h",
    num_candles=300,           # Number of candles to fetch
    start_year=2024,            # Optional: start year
    end_year=2024               # Optional: end year
)

# Validate trading pair
is_valid = validate_symbol("BTC-USD")
```

### Coinbase Advanced Trade API Client

```python
from exchanges.coinbase.advanced_trade import fetch_ohlcv_advanced

# Fetch 30m OHLCV data (requires CDP API credentials)
data = fetch_ohlcv_advanced(
    symbol="BTC-USD",
    timeframe="30m",
    start_year=2024,            # Optional: start year
    end_year=2024               # Optional: end year
)
```

### Weekly Aggregator

```python
from exchanges.coinbase.weekly_aggregator import (
    aggregate_to_weekly, 
    calculate_required_daily_candles
)

# Aggregate daily data to weekly
weekly_data = aggregate_to_weekly(daily_data_list)

# Calculate how many daily candles needed
num_daily = calculate_required_daily_candles(
    start_year=2024,
    end_year=2024
)
```

### Local Storage Manager

```python
from storage.local_storage import LocalStorage

# Initialize
storage = LocalStorage(base_dir="data")

# Save data
storage.save_csv(
    df=dataframe,
    exchange="coinbase",
    pair="BTC-USD",
    timeframe="1h",
    deduplicate=True
)

# Load data
df = storage.load_csv(
    exchange="coinbase",
    pair="BTC-USD",
    timeframe="1h"
)

# Get stats
stats = storage.get_storage_stats()
```

## 🐛 Troubleshooting

### Common Issues

#### 1. CDP API Authentication Error (30m data)

**Problem**: "API credentials file not found" or "Authentication failed (401)"

**Solution**: 
1. Ensure `credentials/cdp_api_key.json` exists
2. Verify the JSON format is correct:
   ```json
   {
     "name": "organizations/YOUR_ORG/apiKeys/YOUR_KEY",
     "privateKey": "-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----"
   }
   ```
3. Check that the private key includes the BEGIN/END markers
4. Verify you created the API key at https://portal.cdp.coinbase.com/



#### 3. Weekly Timestamp Mismatch

**Problem**: Weekly candle timestamps don't match TradingView charts

**Solution**: 
- The system now uses `label='left'` in pandas resample
- Weekly timestamps represent the START of the week (Sunday 00:00)
- This aligns with TradingView's weekly chart labeling

#### 4. Missing Daily Data for Weekly Aggregation

**Problem**: Trying to generate 1w without 1d data

**Solution**: 
- Fetch daily data first before requesting weekly
- Or request both in the same run: `1d,1w`
- The system automatically fetches daily data if missing

#### 5. No Data Returned from API

**Problem**: Invalid trading pair or date range

**Solution**:
- Verify trading pair format: `BTC-USD` not `BTCUSD`
- Check if pair exists on Coinbase
- Adjust date range if specified

### Debug Mode

Enable detailed logging:

```python
from utils.logger import setup_logger
import logging

logger = setup_logger(__name__, level=logging.DEBUG)
```

## 📊 Data Validation

The system validates data at multiple stages:

1. **API Response Validation**: Ensures valid JSON and expected format
2. **Schema Validation**: Checks for required columns
3. **Type Validation**: Enforces numeric types for OHLCV values
4. **OHLC Relationship Validation**: Ensures high ≥ open/close/low
5. **Timestamp Validation**: Converts to UTC datetime
6. **Deduplication**: Removes duplicate timestamps


## 🚀 Advanced Usage

### Batch Processing Multiple Pairs

Create a script to process multiple pairs:

```python
pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]
timeframes = ["1h", "4h", "1d"]

for pair in pairs:
    for tf in timeframes:
        # Fetch and process
        pass
```


### Custom Timeframes

The system supports adding new aggregated timeframes. For example, to add a 2-week timeframe:

1. **Update `config.py`:**
```python
AGGREGATED_TIMEFRAMES = ["1w", "2w"]
ALL_TIMEFRAMES = ["5m", "30m", "1h", "6h", "1d", "1w", "2w"]
```

2. **Update `weekly_aggregator.py`:**
```python
# Add 2-week aggregation function
def aggregate_to_biweekly(daily_data: List[Dict]) -> List[Dict]:
    # Similar to weekly aggregation but with '2W' frequency
    weekly = df.resample('2W', label='left', closed='left').agg({...})
```

## 📈 Performance Tips

1. **Batch requests**: Fetch multiple timeframes at once to reuse source data
2. **Use caching**: Source data is cached during runtime
3. **Local-first**: Store locally and upload to Drive in batches
4. **Parallel processing**: Use multiprocessing for multiple pairs
5. **Incremental updates**: Only fetch new data, existing data is deduplicated

### Development Setup

```bash
# Clone your fork
git clone https://github.com/Melek390/OHLCV-data

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/
```


## 🙏 Acknowledgments

- Coinbase for providing the Exchange API
- Google for Sheets and Drive APIs
- pandas for powerful data manipulation
- The Python community for excellent libraries

## 📝 Version History

### Version 2.0 (Current)
- ✨ **New**: Added 5m and 30m timeframe support
- 🔐 **New**: CDP API authentication for Advanced Trade API (30m data)
- 🔧 **Changed**: Weekly aggregation now uses Sunday-Sunday grouping
- 🐛 **Fixed**: Weekly timestamp now shows START of week (aligns with TradingView)
- 📦 **Changed**: Removed environment variable auth, uses `cdp_api_key.json`
- 🎨 **Enhanced**: Automatic pagination for historical data (year ranges)
- 📊 **Enhanced**: Better error handling and logging


## 📚 Additional Resources

- [Coinbase Exchange API Documentation](https://docs.cloud.coinbase.com/exchange/docs)
- [Coinbase Advanced Trade API](https://docs.cdp.coinbase.com/advanced-trade/docs/welcome)
- [Coinbase CDP API Keys](https://portal.cdp.coinbase.com/access/api)
- [Google Sheets API Guide](https://developers.google.com/sheets/api)
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [Python Datetime Guide](https://docs.python.org/3/library/datetime.html)

---

