"""
Unsupported Timeframes Handler
Generates unsupported timeframes (4h, 1w) from supported ones (1h, 1d)
"""
import pandas as pd
from typing import Optional
from utils.logger import setup_logger
from utils.exceptions import DataValidationException
from config.config import REQUIRED_COLUMNS

logger = setup_logger(__name__)


def resample_ohlcv(df: pd.DataFrame, target_timeframe: str) -> pd.DataFrame:
    """
    Resample OHLCV data to a different timeframe
    
    Args:
        df: Source DataFrame with OHLCV data
        target_timeframe: Target timeframe to resample to
    
    Returns:
        Resampled DataFrame
    
    Raises:
        DataValidationException: If resampling fails
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for resampling")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    
    # Validate required columns
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise DataValidationException(f"Missing required columns: {missing}")
    
    # Ensure timestamp is datetime and set as index
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp")
    
    # Define resampling rules
    resample_rules = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "symbol": "first",
    }
    
    # Map target timeframe to pandas freq
    freq_map = {
        "4h": "4h",      # 4 hours
        "1w": "W-MON",   # Week starting Monday
    }
    
    if target_timeframe not in freq_map:
        raise DataValidationException(
            f"Unsupported target timeframe: {target_timeframe}"
        )
    
    freq = freq_map[target_timeframe]
    
    try:
        logger.info(f"Resampling data to {target_timeframe} ({freq})...")
        
        # Resample
        resampled = df.resample(freq).agg(resample_rules)
        
        # Drop rows where no data exists (all NaN)
        resampled = resampled.dropna(subset=["open", "close"])
        
        # Reset index to make timestamp a column again
        resampled = resampled.reset_index()
        
        logger.info(f"Resampled to {len(resampled)} {target_timeframe} candles")
        
        return resampled
        
    except Exception as e:
        raise DataValidationException(f"Resampling failed: {str(e)}")


def generate_4h_from_1h(df_1h: pd.DataFrame) -> pd.DataFrame:
    """
    Generate 4-hour timeframe data from 1-hour data
    
    Args:
        df_1h: DataFrame with 1-hour OHLCV data
    
    Returns:
        DataFrame with 4-hour OHLCV data
    """
    logger.info("Generating 4H timeframe from 1H data...")
    return resample_ohlcv(df_1h, "4h")


def generate_1w_from_1d(df_1d: pd.DataFrame) -> pd.DataFrame:
    """
    Generate 1-week timeframe data from 1-day data
    
    Args:
        df_1d: DataFrame with 1-day OHLCV data
    
    Returns:
        DataFrame with 1-week OHLCV data
    """
    logger.info("Generating 1W timeframe from 1D data...")
    return resample_ohlcv(df_1d, "1w")


def is_supported_timeframe(tf: str) -> bool:
    """
    Check if timeframe is natively supported by Coinbase
    
    Args:
        tf: Timeframe string
    
    Returns:
        True if supported, False otherwise
    """
    # Coinbase supports: 1h, 6h, 1d (but NOT 4h or 1w)
    supported = ["1h", "6h", "1d"]
    return tf in supported