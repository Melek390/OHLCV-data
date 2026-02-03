# 📊 OHLCV Data Ingestion System

A professional Python-based data pipeline for fetching, processing, and storing OHLCV (Open, High, Low, Close, Volume) cryptocurrency data from Coinbase Exchange with optional Google Drive backup.

## ✨ Features

- 🔄 **Multi-Timeframe Support**: 1h, 4h, 6h, 1d, 1w
- 📥 **Direct API Integration**: Fetches data from Coinbase Exchange API
- 🔧 **Smart Timeframe Generation**: Automatically generates unsupported timeframes (4h, 1w) from supported ones
- 💾 **Local CSV Storage**: Stores data locally with automatic deduplication
- ☁️ **Google Drive Backup**: Optional upload to Google Sheets for cloud storage
- 🔐 **Dual Authentication**: Support for both OAuth2 and Service Account authentication
- ✅ **Data Validation**: Comprehensive validation and error handling
- 📊 **Progress Tracking**: Real-time progress indicators and detailed logging

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
git clone https://github.com/yourusername/ohlcv-ingestion-system.git
cd ohlcv-ingestion-system
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
```

## 📁 Project Structure

```
ohlcv-ingestion-system/
│
├── config/
│   └── config.py                 # Configuration settings
│
├── credentials/
│   ├── oauth_credentials.json    # OAuth2 credentials (if using OAuth)
│   ├── service_account.json      # Service account key (if using SA)
│   └── token.pickle              # Cached OAuth tokens
│
├── data/                         # Local CSV storage
│   └── coinbase/
│       ├── BTC-USD_1h.csv
│       ├── BTC-USD_4h.csv
│       └── ...
│
├── drive/
│   ├── __init__.py
│   ├── data_manager.py           # Data transformation & validation
│   └── sheets.py                 # Google Sheets operations
│
├── exchanges/
│   ├── __init__.py
│   ├── coinbase.py               # Coinbase API client
│   └── unsupported_tfs.py        # Unsupported timeframe generator
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
SUPPORTED_TIMEFRAMES = ["1h", "6h", "1d"]      # Natively supported by Coinbase
UNSUPPORTED_TIMEFRAMES = ["4h", "1w"]          # Generated from supported ones

# Timeframe generation mapping
UNSUPPORTED_SOURCE_MAP = {
    "4h": "1h",  # Generate 4h from 1h data
    "1w": "1d",  # Generate 1w from 1d data
}
```

### Authentication Mode

In `main.py`, choose your authentication method:

```python
# Set to True for OAuth2 (your personal Google account)
# Set to False for Service Account
USE_OAUTH = True
```

## 🔐 Authentication Setup

### Option 1: OAuth2 (Recommended for Personal Use)

1. **Go to Google Cloud Console**: https://console.cloud.google.com/
2. **Create a new project** or select existing one
3. **Enable APIs**:
   - Google Sheets API
   - Google Drive API
4. **Create OAuth2 credentials**:
   - Go to "Credentials" → "Create Credentials" → "OAuth client ID"
   - Application type: "Desktop app"
   - Download the JSON file
5. **Save credentials**:
   - Rename to `oauth_credentials.json`
   - Place in `credentials/` directory
6. **First run**: Browser will open for authorization
   - Sign in to your Google account
   - Grant permissions
   - Token will be saved for future use

### Option 2: Service Account (For Automation)

1. **Create Service Account**:
   - Google Cloud Console → "IAM & Admin" → "Service Accounts"
   - Create new service account
   - Download JSON key file
2. **Save credentials**:
   - Rename to match `SERVICE_ACCOUNT_PATH` in config
   - Place in `credentials/` directory
3. **Share Drive folder**:
   - Share your Google Drive folder with the service account email
   - Grant "Editor" permissions

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
📊 OHLCV Data Ingestion System v3.0
======================================================================

🏢 Available Exchanges:
  • Coinbase

Select exchange (default: coinbase): 

📈 Enter trading pair (default: BTC-USD): ETH-USD

⏱️  Available Timeframes:
  1h, 4h, 6h, 1d, 1w

Enter timeframes (comma-separated, e.g., 1h,4h,1d): 1h,4h,1d

✅ Configuration:
  Exchange:   Coinbase
  Pair:       ETH-USD
  Timeframes: 1h, 4h, 1d

📥 FETCHING & STORING DATA LOCALLY
======================================================================

🔹 Processing SUPPORTED timeframes (direct API fetch)...

⏳ Processing 1H timeframe...
  📥 Fetched 300 candles from API
  ✅ Processed 300 valid candles
  ✅ Saved 300 new rows to: ETH-USD_1h.csv

⏳ Processing 1D timeframe...
  📥 Fetched 365 candles from API
  ✅ Processed 365 valid candles
  ✅ Saved 365 new rows to: ETH-USD_1d.csv

🔸 Processing UNSUPPORTED timeframes (generating from source data)...

⏳ Processing 4H timeframe...
  ℹ️  Unsupported by API - will generate from 1H data
  📦 Using cached 1H data (300 rows)
  ✅ Generated 75 4H candles
  ✅ Saved 75 new rows to: ETH-USD_4h.csv

Upload to Drive? (y/n): y
```

## ⏱️ Timeframe Support

### Supported Timeframes (Direct API Fetch)

| Timeframe | Description | API Support | Storage |
|-----------|-------------|-------------|---------|
| **1h** | 1 Hour | ✅ Native | CSV |
| **6h** | 6 Hours | ✅ Native | CSV |
| **1d** | 1 Day | ✅ Native | CSV |

### Unsupported Timeframes (Generated)

| Timeframe | Description | Generated From | Method |
|-----------|-------------|----------------|--------|
| **4h** | 4 Hours | 1h data | Pandas resample |
| **1w** | 1 Week | 1d data | Pandas resample |

### How Timeframe Generation Works

The system uses pandas resampling to aggregate data:

- **4h from 1h**: Combines four 1-hour candles into one 4-hour candle
  - Open: First 1h open
  - High: Maximum 1h high
  - Low: Minimum 1h low
  - Close: Last 1h close
  - Volume: Sum of 1h volumes

- **1w from 1d**: Combines daily candles into weekly candles
  - Week starts on Monday
  - Same aggregation logic as 4h

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
    ├── BTC-USD_1h.csv    # 1-hour Bitcoin data
    ├── BTC-USD_4h.csv    # 4-hour Bitcoin data (generated)
    ├── BTC-USD_6h.csv    # 6-hour Bitcoin data
    ├── BTC-USD_1d.csv    # Daily Bitcoin data
    ├── BTC-USD_1w.csv    # Weekly Bitcoin data (generated)
    ├── ETH-USD_1h.csv    # 1-hour Ethereum data
    └── ...
```

## 🔧 API Reference

### Coinbase API Client

```python
from exchanges.coinbase import fetch_ohlcv, validate_symbol

# Fetch OHLCV data
data = fetch_ohlcv(
    symbol="BTC-USD",
    timeframe="1h",
    start="2024-01-01T00:00:00Z",  # Optional
    end="2024-01-31T23:59:59Z"      # Optional
)

# Validate trading pair
is_valid = validate_symbol("BTC-USD")
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

### Unsupported Timeframe Generator

```python
from exchanges.unsupported_tfs import generate_4h_from_1h, generate_1w_from_1d

# Generate 4h from 1h data
df_4h = generate_4h_from_1h(df_1h)

# Generate 1w from 1d data
df_1w = generate_1w_from_1d(df_1d)
```

## 🐛 Troubleshooting

### Common Issues

#### 1. API Error: "Unsupported granularity"

**Problem**: Trying to fetch 4h or 1w data directly from Coinbase

**Solution**: The system automatically handles this by generating these timeframes from supported ones. Make sure you have the source timeframe data (1h for 4h, 1d for 1w).

#### 2. Google Sheets Quota Exceeded

**Problem**: Service account exceeds quota limits

**Solution**: 
- Switch to OAuth2 authentication (`USE_OAUTH = True`)
- Use your personal Google account with higher quotas

#### 3. Missing Source Data for Unsupported Timeframe

**Problem**: Trying to generate 4h without 1h data

**Solution**: 
- Fetch the source timeframe first (e.g., fetch 1h before 4h)
- Or request both in the same run: `1h,4h`

#### 4. "Invalid frequency: H"

**Problem**: Pandas resampling frequency error

**Solution**: Use lowercase `"4h"` instead of uppercase `"4H"` in config

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

## 🔒 Security Best Practices

1. **Never commit credentials**: Add to `.gitignore`:
   ```
   credentials/*.json
   credentials/*.pickle
   ```

2. **Use environment variables** for sensitive config:
   ```python
   import os
   DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
   ```

3. **Restrict API permissions**: Grant minimum required scopes

4. **Rotate credentials**: Regularly update service account keys

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

### Automated Scheduling

Use cron or Task Scheduler:

```bash
# Fetch data every hour
0 * * * * cd /path/to/project && python main.py --auto --pair BTC-USD --tf 1h
```

### Custom Timeframes

Add new timeframes to `config.py`:

```python
UNSUPPORTED_SOURCE_MAP = {
    "4h": "1h",
    "1w": "1d",
    "2h": "1h",  # Add 2-hour timeframe
}
```

Update `unsupported_tfs.py`:

```python
freq_map = {
    "4h": "4h",
    "1w": "W-MON",
    "2h": "2h",  # Add 2-hour frequency
}
```

## 📈 Performance Tips

1. **Batch requests**: Fetch multiple timeframes at once to reuse source data
2. **Use caching**: Source data is cached during runtime
3. **Local-first**: Store locally and upload to Drive in batches
4. **Parallel processing**: Use multiprocessing for multiple pairs
5. **Incremental updates**: Only fetch new data, existing data is deduplicated

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/ohlcv-ingestion-system.git

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Coinbase for providing the Exchange API
- Google for Sheets and Drive APIs
- pandas for powerful data manipulation
- The Python community for excellent libraries

## 📧 Contact

For questions or support:
- Open an issue on GitHub
- Email: your.email@example.com
- Twitter: @yourhandle

## 🔄 Changelog

### Version 3.0 (Current)
- ✨ Added support for unsupported timeframes (4h, 1w)
- 🔧 Smart timeframe generation from source data
- 📦 Improved data caching mechanism
- 🎨 Enhanced user interface with progress indicators
- 🐛 Fixed pandas resampling frequency issues

### Version 2.0
- ☁️ Added Google Drive backup functionality
- 🔐 OAuth2 authentication support
- 💾 Local CSV storage with deduplication

### Version 1.0
- 🎉 Initial release
- 📥 Coinbase API integration
- 📊 Basic OHLCV data fetching

## 📚 Additional Resources

- [Coinbase API Documentation](https://docs.cloud.coinbase.com/exchange/docs)
- [Google Sheets API Guide](https://developers.google.com/sheets/api)
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [Python Datetime Guide](https://docs.python.org/3/library/datetime.html)

---

**Made with ❤️ for the crypto trading community**
