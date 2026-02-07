"""
Coinbase Advanced Trade API Client
Fetches 30m OHLCV data directly from Coinbase Advanced Trade API with JWT authentication
Uses cdp_api_key.json for credentials (name and privateKey)
Supports pagination for fetching all historical data
"""
import requests
import time
import jwt
import json
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
from cryptography.hazmat.primitives import serialization

from config.config import COINBASE_ADVANCED_TRADE_URL, API_TIMEOUT
from utils.logger import setup_logger
from utils.exceptions import APIException

logger = setup_logger(__name__)

# Coinbase API limit per request
MAX_CANDLES_PER_REQUEST = 300

# Time range covered by 300 candles for 30m timeframe
# 300 candles * 30 minutes = 9000 minutes = 150 hours = 6.25 days
TIMEFRAME_COVERAGE_30M = timedelta(minutes=30 * 300)

# Path to API credentials file
CREDENTIALS_DIR = Path(__file__).resolve().parent.parent.parent / "credentials"
KEY_FILE = CREDENTIALS_DIR / "cdp_api_key.json"


def load_api_credentials() -> tuple:
    """
    Load API credentials from cdp_api_key.json
    
    Returns:
        Tuple of (api_key_name, private_key)
    
    Raises:
        ValueError: If credentials file not found or invalid
    """
    if not KEY_FILE.exists():
        raise ValueError(
            f"API credentials file not found: {KEY_FILE}\n"
            f"Please create credentials/cdp_api_key.json with your Coinbase API key.\n"
            f"Get your API keys from: https://portal.cdp.coinbase.com/access/api"
        )
    
    try:
        key_data = json.loads(KEY_FILE.read_text())
        api_key_name = key_data.get("name")
        private_key = key_data.get("privateKey")
        
        if not api_key_name or not private_key:
            raise ValueError("Missing 'name' or 'privateKey' in cdp_api_key.json")
        
        logger.info(f"Loaded API credentials from {KEY_FILE.name}")
        return api_key_name, private_key
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {KEY_FILE}: {str(e)}")
    except Exception as e:
        raise ValueError(f"Failed to load API credentials: {str(e)}")


def build_jwt(api_key_name: str, private_key: str, uri: str) -> str:
    """
    Build JWT token with proper headers (kid and nonce) using cryptography library
    
    Args:
        api_key_name: API key name from cdp_api_key.json
        private_key: Private key PEM string from cdp_api_key.json
        uri: Request URI in format "METHOD host/path" (no query params)
    
    Returns:
        JWT token string
    
    Raises:
        Exception: If JWT creation fails
    """
    try:
        # Load private key properly using cryptography library
        private_key_bytes = private_key.encode('utf-8')
        private_key_obj = serialization.load_pem_private_key(
            private_key_bytes, 
            password=None
        )
        
        # Build JWT payload
        jwt_payload = {
            'sub': api_key_name,
            'iss': 'coinbase-cloud',
            'nbf': int(time.time()),
            'exp': int(time.time()) + 120,  # Token valid for 2 minutes
            'uri': uri,
        }
        
        # Generate JWT with kid and nonce headers
        token = jwt.encode(
            jwt_payload,
            private_key_obj,
            algorithm='ES256',
            headers={
                'kid': api_key_name,
                'nonce': secrets.token_hex()
            }
        )
        
        return token
        
    except Exception as e:
        logger.error(f"Failed to create JWT token: {str(e)}")
        raise ValueError(f"JWT token creation failed: {str(e)}")


def calculate_pagination_params(start_year: int, end_year: int) -> List[tuple]:
    """
    Calculate start/end date ranges for 30m pagination
    
    Args:
        start_year: Year to start fetching data from
        end_year: Year to fetch data up to
    
    Returns:
        List of (start_timestamp, end_timestamp) tuples as Unix timestamps
    """
    # End date: Dec 31 of the end_year at 23:59:59
    end_date = datetime(end_year, 12, 31, 23, 59, 59)
    
    # Start date: Jan 1 of start_year at 00:00:00
    start_date = datetime(start_year, 1, 1, 0, 0, 0)
    
    # Generate pagination chunks
    chunks = []
    current_end = end_date
    
    while current_end > start_date:
        current_start = current_end - TIMEFRAME_COVERAGE_30M
        
        # Don't go before our absolute start date
        if current_start < start_date:
            current_start = start_date
        
        chunks.append((
            int(current_start.timestamp()),
            int(current_end.timestamp())
        ))
        
        current_end = current_start
    
    # Reverse to fetch oldest to newest
    chunks.reverse()
    
    logger.info(f"Calculated {len(chunks)} pagination chunks for 30m timeframe ({start_year} to {end_year})")
    return chunks


def fetch_ohlcv_chunk_advanced(
    symbol: str,
    start_timestamp: int,
    end_timestamp: int,
    api_key_name: str,
    private_key: str,
) -> List[Dict]:
    """
    Fetch a single chunk of 30m OHLCV data from Advanced Trade API
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USD')
        start_timestamp: Unix timestamp for start
        end_timestamp: Unix timestamp for end
        api_key_name: Coinbase API key name
        private_key: Coinbase API private key (PEM format)
    
    Returns:
        List of OHLCV dictionaries
    """
    # Use STRING enum for granularity (CRITICAL!)
    granularity = "THIRTY_MINUTE"
    product_id = symbol
    
    # Build request path (without query parameters for JWT URI)
    request_path = f"/api/v3/brokerage/products/{product_id}/candles"
    request_host = "api.coinbase.com"
    
    # Build query parameters
    params = {
        "granularity": granularity,
        "start": str(start_timestamp),
        "end": str(end_timestamp),
    }
    
    # Build full URL
    base_url = "https://api.coinbase.com"
    url = f"{base_url}{request_path}"
    
    # Build URI for JWT (METHOD + host + path, NO query params!)
    uri = f"GET {request_host}{request_path}"
    
    try:
        # Create JWT token for authentication
        jwt_token = build_jwt(
            api_key_name=api_key_name,
            private_key=private_key,
            uri=uri
        )
        
        # Set authorization header
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
        }
        
        # Make request
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=API_TIMEOUT
        )
        response.raise_for_status()
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
        
        if e.response.status_code == 401:
            error_msg = (
                "Authentication failed. Please check:\n"
                "1. Your cdp_api_key.json file exists in credentials/ folder\n"
                "2. The 'name' field contains your API key name\n"
                "3. The 'privateKey' field contains the full private key (PEM format)\n"
                "4. Get API keys from: https://portal.cdp.coinbase.com/access/api"
            )
        elif e.response.status_code == 403:
            error_msg = "Access forbidden. Check API key permissions."
        elif e.response.status_code == 400:
            error_msg = f"Bad request: {e.response.text}\nMake sure granularity is 'THIRTY_MINUTE' (string, not integer)"
        
        raise APIException(error_msg)
        
    except requests.exceptions.Timeout:
        raise APIException(f"Request timeout after {API_TIMEOUT}s")
        
    except requests.exceptions.RequestException as e:
        raise APIException(f"Request failed: {str(e)}")
    
    # Parse response
    try:
        data = response.json()
    except ValueError as e:
        raise APIException(f"Invalid JSON response: {str(e)}")
    
    if not isinstance(data, dict) or "candles" not in data:
        raise APIException(f"Unexpected response format: {data}")
    
    candles = data.get("candles", [])
    
    # Standardize the candle data
    standardized = []
    for candle in candles:
        try:
            # Advanced Trade API returns timestamps as Unix timestamps in string format
            timestamp_unix = int(candle["start"])
            
            standardized.append({
                "timestamp": datetime.utcfromtimestamp(timestamp_unix).isoformat(),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle["volume"]),
                "symbol": symbol.replace("-", "/"),
            })
            
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Skipping malformed candle: {candle} - Error: {e}")
            continue
    
    return standardized


def fetch_ohlcv_advanced(
    symbol: str,
    timeframe: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch 30m OHLCV data from Coinbase Advanced Trade API with pagination
    Uses cdp_api_key.json for authentication
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USD')
        timeframe: Candle timeframe (must be '30m')
        start_year: Optional year to start fetching data from (e.g., 2022)
        end_year: Optional year to fetch data up to (e.g., 2024)
                  Will fetch data from Jan 1 of start_year to Dec 31 of end_year
    
    Returns:
        List of standardized OHLCV dictionaries
    
    Raises:
        APIException: If API request fails
        ValueError: If timeframe is not 30m or credentials missing
    """
    if timeframe != "30m":
        raise ValueError(
            f"Advanced Trade API handler only supports '30m' timeframe, got '{timeframe}'"
        )
    
    # Load API credentials from cdp_api_key.json
    try:
        api_key_name, private_key = load_api_credentials()
        logger.info("API credentials loaded successfully from cdp_api_key.json")
    except ValueError as e:
        raise APIException(str(e))
    
    # If no years specified, fetch latest 300 candles
    if not start_year and not end_year:
        logger.info(f"Fetching latest {MAX_CANDLES_PER_REQUEST} candles for {symbol} 30m")
        
        # Use current time as end
        end_timestamp = int(time.time())
        start_timestamp = end_timestamp - int(TIMEFRAME_COVERAGE_30M.total_seconds())
        
        return fetch_ohlcv_chunk_advanced(
            symbol=symbol,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            api_key_name=api_key_name,
            private_key=private_key
        )
    
    # Validate year inputs
    if start_year and end_year:
        if start_year > end_year:
            raise ValueError(f"start_year ({start_year}) cannot be greater than end_year ({end_year})")
    elif start_year and not end_year:
        # Default end_year to current year
        end_year = datetime.now().year
        logger.info(f"No end_year specified, using current year: {end_year}")
    elif end_year and not start_year:
        # Default start_year to 2 years before end_year
        start_year = end_year - 2
        logger.info(f"No start_year specified, using {start_year} (2 years before {end_year})")
    
    # Calculate pagination chunks
    logger.info(f"Fetching historical 30m data for {symbol} from {start_year} to {end_year}")
    chunks = calculate_pagination_params(start_year, end_year)
    
    all_data = []
    total_chunks = len(chunks)
    
    for idx, (start_ts, end_ts) in enumerate(chunks, 1):
        start_str = datetime.utcfromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M")
        end_str = datetime.utcfromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M")
        
        logger.info(f"Fetching chunk {idx}/{total_chunks}: {start_str} to {end_str}")
        
        try:
            chunk_data = fetch_ohlcv_chunk_advanced(
                symbol=symbol,
                start_timestamp=start_ts,
                end_timestamp=end_ts,
                api_key_name=api_key_name,
                private_key=private_key
            )
            
            if chunk_data:
                all_data.extend(chunk_data)
                logger.info(f"  → Received {len(chunk_data)} candles")
            else:
                logger.warning(f"  → No data in this chunk")
            
            # Sleep between requests to avoid rate limiting
            # Advanced Trade API has stricter limits, use 0.3s to be safe
            if idx < total_chunks:
                time.sleep(0.3)
                
        except APIException as e:
            logger.error(f"Error fetching chunk {idx}/{total_chunks}: {str(e)}")
            # Continue with next chunk instead of failing completely
            continue
    
    # Remove duplicates based on timestamp
    if all_data:
        seen = set()
        unique_data = []
        for candle in all_data:
            ts = candle["timestamp"]
            if ts not in seen:
                seen.add(ts)
                unique_data.append(candle)
        
        # Sort chronologically
        unique_data.sort(key=lambda x: x["timestamp"])
        
        duplicates_removed = len(all_data) - len(unique_data)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate candles")
        
        logger.info(f"Successfully fetched {len(unique_data)} total candles")
        return unique_data
    
    logger.warning("No data fetched")
    return []