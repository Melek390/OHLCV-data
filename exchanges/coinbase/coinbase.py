"""
Coinbase Exchange API Client
Handles fetching OHLCV data from Coinbase Pro/Advanced Trade API
"""
import requests
from datetime import datetime
from typing import List, Dict, Optional
from config.config import COINBASE_BASE_URL, GRANULARITY_MAP, API_TIMEOUT
from utils.logger import setup_logger
from utils.exceptions import APIException

logger = setup_logger(__name__)


def fetch_ohlcv(
    symbol: str,
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[Dict]:
    """
    Fetch OHLCV (candlestick) data from Coinbase
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTC-USD')
        timeframe: Candle timeframe ('1h', '4h', '6h', '1d', '1w')
        start: Optional ISO 8601 start date
        end: Optional ISO 8601 end date
    
    Returns:
        List of standardized OHLCV dictionaries
    
    Raises:
        APIException: If API request fails
        ValueError: If timeframe is invalid
    """
    if timeframe not in GRANULARITY_MAP:
        raise ValueError(
            f"Unsupported timeframe: {timeframe}. "
            f"Supported: {', '.join(GRANULARITY_MAP.keys())}"
        )

    params = {"granularity": GRANULARITY_MAP[timeframe]}

    if start:
        params["start"] = start
    if end:
        params["end"] = end

    url = f"{COINBASE_BASE_URL}/products/{symbol}/candles"

    try:
        logger.info(f"Fetching {timeframe} candles for {symbol}")
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

    logger.info(f"Successfully fetched {len(standardized)} candles")
    return standardized


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
