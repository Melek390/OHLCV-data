"""
Data transformation and validation module
Converts API responses to clean pandas DataFrames
"""
import pandas as pd
from typing import List, Dict
from config.config import REQUIRED_COLUMNS
from utils.logger import setup_logger
from utils.exceptions import DataValidationException

logger = setup_logger(__name__)


def rest_to_dataframe(data: List[Dict]) -> pd.DataFrame:
    """
    Convert REST OHLCV data into a clean pandas DataFrame
    following the standardized schema.

    Args:
        data: List of OHLCV dictionaries from REST API

    Returns:
        Cleaned and validated pandas DataFrame

    Raises:
        DataValidationException: If data validation fails
    """
    if not data:
        logger.warning("Empty data provided, returning empty DataFrame")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    if not isinstance(data, list):
        raise DataValidationException(
            f"Expected list, got {type(data)}"
        )

    try:
        df = pd.DataFrame(data)
    except Exception as e:
        raise DataValidationException(
            f"Failed to create DataFrame: {str(e)}"
        )

    # Ensure all required columns exist
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise DataValidationException(
            f"Missing required columns: {missing_cols}"
        )

    # Keep only required columns in correct order
    df = df[REQUIRED_COLUMNS].copy()

    # Convert timestamp to datetime (UTC-safe)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        raise DataValidationException(
            f"Failed to parse timestamps: {str(e)}"
        )

    # Enforce numeric types for OHLCV values
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Count rows with invalid data before dropping
    invalid_rows = df[numeric_cols].isnull().any(axis=1).sum()
    if invalid_rows > 0:
        logger.warning(f"Dropping {invalid_rows} rows with invalid numeric data")

    # Drop rows with any NaN values
    df = df.dropna()

    if df.empty:
        logger.warning("All rows were invalid, returning empty DataFrame")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    # Deduplicate by timestamp and symbol
    initial_count = len(df)
    df = df.drop_duplicates(subset=["timestamp", "symbol"], keep="last")
    duplicates_removed = initial_count - len(df)
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate rows")

    # Sort chronologically
    df = df.sort_values("timestamp").reset_index(drop=True)

    logger.info(f"DataFrame created with {len(df)} valid rows")
    return df


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Validate DataFrame structure and content
    
    Args:
        df: DataFrame to validate
    
    Returns:
        True if valid
    
    Raises:
        DataValidationException: If validation fails
    """
    if df.empty:
        raise DataValidationException("DataFrame is empty")
    
    # Check columns
    if not all(col in df.columns for col in REQUIRED_COLUMNS):
        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        raise DataValidationException(f"Missing columns: {missing}")
    
    # Check for nulls
    if df.isnull().any().any():
        raise DataValidationException("DataFrame contains null values")
    
    # Validate OHLC relationships
    invalid_ohlc = (
        (df["high"] < df["low"]) |
        (df["high"] < df["open"]) |
        (df["high"] < df["close"]) |
        (df["low"] > df["open"]) |
        (df["low"] > df["close"])
    ).any()
    
    if invalid_ohlc:
        raise DataValidationException("Invalid OHLC relationships detected")
    
    return True
