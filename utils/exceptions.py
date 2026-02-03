"""
Custom exceptions for the OHLCV ingestion system
"""


class OHLCVException(Exception):
    """Base exception for OHLCV system"""
    pass


class APIException(OHLCVException):
    """Exception for API-related errors"""
    pass


class GoogleSheetsException(OHLCVException):
    """Exception for Google Sheets operations"""
    pass


class StorageQuotaException(GoogleSheetsException):
    """Exception when Drive storage quota is exceeded"""
    pass


class DataValidationException(OHLCVException):
    """Exception for data validation errors"""
    pass


class ConfigurationException(OHLCVException):
    """Exception for configuration errors"""
    pass
