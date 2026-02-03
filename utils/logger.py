"""
Logging utility for the OHLCV ingestion system
"""
import logging
import sys
from config.config import LOG_FORMAT, LOG_DATE_FORMAT


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Setup and configure logger with consistent formatting
    
    Args:
        name: Logger name (typically __name__ of the module)
        level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger
