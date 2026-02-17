"""
Weekly Data Aggregator
Aggregates daily (1d) OHLCV data into weekly (1w) timeframe
"""
import pandas as pd
from typing import List, Dict
from utils.logger import setup_logger
from utils.exceptions import DataValidationException

logger = setup_logger(__name__)


def aggregate_to_weekly(daily_data: List[Dict]) -> List[Dict]:
    """
    Aggregate daily OHLCV data into weekly candles
    
    Week starts on Sunday midnight (00:00) and ends on next Sunday midnight (00:00)
    This means the first hourly candle of the week ends at Monday 1 AM
    
    Args:
        daily_data: List of daily OHLCV dictionaries
    
    Returns:
        List of weekly OHLCV dictionaries
    
    Raises:
        DataValidationException: If data is invalid
    """
    if not daily_data:
        logger.warning("Empty daily data provided")
        return []
    
    # Convert to DataFrame
    df = pd.DataFrame(daily_data)
    
    # Validate required columns
    required = ["timestamp", "open", "high", "low", "close", "volume", "symbol"]
    missing = set(required) - set(df.columns)
    if missing:
        raise DataValidationException(f"Missing required columns: {missing}")
    
    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    
    # Sort by timestamp
    df = df.sort_values("timestamp")
    
    # Get symbol (should be same for all rows)
    symbol = df["symbol"].iloc[0]
    
    # Set timestamp as index
    df = df.set_index("timestamp")
    
    # Resample to weekly starting on Monday (matches TradingView's week start)
    # 'W-MON' groups Monday to Sunday and labels with the END (next Monday)
    # We use label='left' to label with the START of the week (Monday 00:00)
    weekly = df.resample('W-MON', label='left', closed='left').agg({
        'open': 'first',    # First open of the week (Sunday midnight)
        'high': 'max',      # Highest high of the week
        'low': 'min',       # Lowest low of the week
        'close': 'last',    # Last close of the week (Saturday close)
        'volume': 'sum',    # Total volume for the week
    })
    
    # Remove rows with NaN (incomplete weeks)
    weekly = weekly.dropna()
    
    # Reset index to get timestamp back as column
    weekly = weekly.reset_index()
    
    # Add symbol column
    weekly['symbol'] = symbol
    
    # Convert to list of dicts
    weekly_data = []
    for _, row in weekly.iterrows():
        weekly_data.append({
            'timestamp': row['timestamp'].isoformat(),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row['volume']),
            'symbol': row['symbol'],
        })
    
    logger.info(f"Aggregated {len(daily_data)} daily candles into {len(weekly_data)} weekly candles (Sunday-Sunday)")
    
    return weekly_data


def calculate_required_daily_candles(num_weekly_candles: int = None, start_year: int = None, end_year: int = None) -> int:
    """
    Calculate how many daily candles are needed to produce the requested weekly candles
    
    Args:
        num_weekly_candles: Number of weekly candles desired (optional if years provided)
        start_year: Start year for data range (optional)
        end_year: End year for data range (optional)
    
    Returns:
        Number of daily candles to fetch
    """
    # If years are provided, calculate based on date range
    if start_year and end_year:
        from datetime import datetime
        
        # Calculate number of days between start and end year
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        
        days_in_range = (end_date - start_date).days + 1
        
        # Add 10% buffer to account for missing data
        daily_needed = int(days_in_range * 1.1)
        
        logger.info(f"Need ~{daily_needed} daily candles for range {start_year} to {end_year}")
        return daily_needed
    
    # Fall back to weekly candle count if provided
    if num_weekly_candles:
        # Each week has ~7 days
        # Add 20% buffer to account for incomplete weeks at boundaries
        daily_needed = int(num_weekly_candles * 7 * 1.2)
        
        logger.info(f"Need ~{daily_needed} daily candles to produce {num_weekly_candles} weekly candles")
        return daily_needed
    
    # Default to 500 daily candles (~71 weeks) if nothing specified
    logger.warning("No parameters provided, defaulting to 500 daily candles")
    return 500