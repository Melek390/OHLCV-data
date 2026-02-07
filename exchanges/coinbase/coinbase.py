"""
Coinbase Exchange API Client
Handles fetching OHLCV data from Coinbase Exchange API (REST)
Supports: 5m, 1h, 6h, 1d timeframes with pagination based on number of candles
"""
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from config.config import (
    COINBASE_BASE_URL,
    GRANULARITY_MAP,
    API_TIMEOUT,
    EXCHANGE_API_TIMEFRAMES,
)
from utils.logger import setup_logger
from utils.exceptions import APIException

logger = setup_logger(__name__)

# Coinbase API limit per request
MAX_CANDLES_PER_REQUEST = 300

# Granularity in seconds for each timeframe
TIMEFRAME_SECONDS = {
    "5m": 300,      # 5 minutes
    "1h": 3600,     # 1 hour
    "6h": 21600,    # 6 hours
    "1d": 86400,    # 1 day
}


def calculate_time_ranges(timeframe: str, num_candles: int) -> List[tuple]:
    """
    Calculate start/end timestamp ranges to fetch the requested number of candles
    Divides into chunks of MAX_CANDLES_PER_REQUEST (300)
    
    Args:
        timeframe: Timeframe string (5m, 1h, 6h, 1d)
        num_candles: Total number of candles to fetch
    
    Returns:
        List of (start_date, end_date) tuples in ISO format
    """
    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError(f"Unknown timeframe: {timeframe}")
    
    # Get seconds per candle for this timeframe
    seconds_per_candle = TIMEFRAME_SECONDS[timeframe]
    
    # Calculate total time span needed
    total_seconds = num_candles * seconds_per_candle
    
    # End time is now
    end_time = datetime.utcnow()
    
    # Start time is total_seconds ago
    start_time = end_time - timedelta(seconds=total_seconds)
    
    # Calculate number of chunks needed
    num_chunks = (num_candles + MAX_CANDLES_PER_REQUEST - 1) // MAX_CANDLES_PER_REQUEST
    
    chunks = []
    current_start = start_time
    
    for i in range(num_chunks):
        # Calculate how many candles in this chunk
        remaining_candles = num_candles - (i * MAX_CANDLES_PER_REQUEST)
        chunk_candles = min(MAX_CANDLES_PER_REQUEST, remaining_candles)
        
        # Calculate time span for this chunk
        chunk_seconds = chunk_candles * seconds_per_candle
        current_end = current_start + timedelta(seconds=chunk_seconds)
        
        chunks.append((
            current_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        ))
        
        current_start = current_end
    
    logger.info(f"Calculated {len(chunks)} chunks for {num_candles} candles ({timeframe} timeframe)")
    return chunks


def fetch_ohlcv_chunk(
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
) -> List[Dict]:
    """
    Fetch a single chunk of OHLCV data
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USD')
        timeframe: Candle timeframe
        start: ISO 8601 start date
        end: ISO 8601 end date
    
    Returns:
        List of OHLCV dictionaries
    """
    if timeframe not in GRANULARITY_MAP:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    params = {
        "granularity": GRANULARITY_MAP[timeframe],
        "start": start,
        "end": end,
    }

    url = f"{COINBASE_BASE_URL}/products/{symbol}/candles"

    try:
        response = requests.get(url, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        raise APIException(f"Request timeout after {API_TIMEOUT}s")
    except requests.exceptions.HTTPError as e:
        raise APIException(f"HTTP error: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        raise APIException(f"Request failed: {str(e)}")

    try:
        candles = response.json()
    except ValueError as e:
        raise APIException(f"Invalid JSON response: {str(e)}")

    if not isinstance(candles, list):
        raise APIException(f"Unexpected response format: {type(candles)}")

    # Coinbase returns newest → oldest, so reverse
    candles.reverse()

    standardized = []
    for candle in candles:
        try:
            # Coinbase format: [time, low, high, open, close, volume]
            standardized.append({
                "timestamp": datetime.utcfromtimestamp(candle[0]).isoformat(),
                "open": candle[3],
                "high": candle[2],
                "low": candle[1],
                "close": candle[4],
                "volume": candle[5],
                "symbol": symbol.replace("-", "/"),
            })
        except (IndexError, KeyError, TypeError) as e:
            logger.warning(f"Skipping malformed candle: {candle} - Error: {e}")
            continue

    return standardized


def fetch_ohlcv(
    symbol: str,
    timeframe: str,
    num_candles: int = 300,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch OHLCV (candlestick) data from Coinbase Exchange API
    Automatically handles pagination to fetch requested number of candles
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USD')
        timeframe: Candle timeframe ('5m', '1h', '6h', '1d')
        num_candles: Number of candles to fetch (default: 300, ignored if years specified)
        start_year: Optional start year for data range
        end_year: Optional end year for data range
    
    Returns:
        List of standardized OHLCV dictionaries
    
    Raises:
        APIException: If API request fails
        ValueError: If timeframe is invalid
    """
    if timeframe not in EXCHANGE_API_TIMEFRAMES:
        raise ValueError(
            f"Timeframe '{timeframe}' not supported by Exchange API. "
            f"Supported: {', '.join(EXCHANGE_API_TIMEFRAMES)}"
        )
    
    # If years are specified, calculate required candles
    if start_year or end_year:
        from datetime import datetime
        
        # Determine date range
        if start_year and end_year:
            start_date = datetime(start_year, 1, 1)
            end_date = datetime(end_year, 12, 31, 23, 59, 59)
        elif start_year:
            start_date = datetime(start_year, 1, 1)
            end_date = datetime.utcnow()
        elif end_year:
            # Default to 2 years before end_year
            start_date = datetime(end_year - 2, 1, 1)
            end_date = datetime(end_year, 12, 31, 23, 59, 59)
        
        # Calculate required candles based on date range
        total_seconds = (end_date - start_date).total_seconds()
        seconds_per_candle = TIMEFRAME_SECONDS[timeframe]
        num_candles = int(total_seconds / seconds_per_candle) + 100  # Add buffer
        
        logger.info(f"Calculated {num_candles} candles needed for date range")
        

    # Calculate time ranges for pagination
    logger.info(f"Fetching {num_candles} candles for {symbol} {timeframe}")
    chunks = calculate_time_ranges(timeframe, num_candles)
    
    all_data = []
    total_chunks = len(chunks)
    
    for idx, (start, end) in enumerate(chunks, 1):
        logger.info(f"Fetching chunk {idx}/{total_chunks}: {start} to {end}")
        
        try:
            chunk_data = fetch_ohlcv_chunk(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end
            )
            
            if chunk_data:
                all_data.extend(chunk_data)
                logger.info(f"  → Received {len(chunk_data)} candles")
            else:
                logger.warning(f"  → No data in this chunk")
            
            # Sleep between requests to avoid rate limiting
            # Coinbase allows ~10 requests per second, so 0.2s is safe
            if idx < total_chunks:  # Don't sleep after last request
                time.sleep(0.2)
                
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


def validate_symbol(symbol: str) -> bool:
    """
    Validate if a trading pair exists on Coinbase
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USD')
    
    Returns:
        True if symbol exists, False otherwise
    """
    url = f"{COINBASE_BASE_URL}/products/{symbol}"
    
    try:
        response = requests.get(url, timeout=API_TIMEOUT)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False