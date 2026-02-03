"""
Local Storage Manager
Handles saving OHLCV data to local CSV files with deduplication
"""
import os
import pandas as pd
from pathlib import Path
from typing import List, Optional
from utils.logger import setup_logger
from utils.exceptions import DataValidationException
from config.config import REQUIRED_COLUMNS

logger = setup_logger(__name__)


class LocalStorage:
    """Manages local CSV storage for OHLCV data"""
    
    def __init__(self, base_dir: str = "data"):
        """
        Initialize local storage manager
        
        Args:
            base_dir: Base directory for storing data (default: 'data')
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        logger.info(f"Local storage initialized at: {self.base_dir.absolute()}")
    
    def get_exchange_dir(self, exchange: str) -> Path:
        """
        Get or create exchange directory
        
        Args:
            exchange: Exchange name (e.g., 'coinbase')
        
        Returns:
            Path to exchange directory
        """
        exchange_dir = self.base_dir / exchange.lower()
        exchange_dir.mkdir(exist_ok=True)
        return exchange_dir
    
    def get_csv_path(self, exchange: str, pair: str, timeframe: str) -> Path:
        """
        Get path for CSV file
        Format: data/{exchange}/{pair}_{timeframe}.csv
        Example: data/coinbase/BTC-USD_1h.csv
        
        Args:
            exchange: Exchange name
            pair: Trading pair (e.g., 'BTC-USD')
            timeframe: Timeframe (e.g., '1h', '4h', '1d')
        
        Returns:
            Path to CSV file
        """
        exchange_dir = self.get_exchange_dir(exchange)
        # Clean pair name for filename (replace / with -)
        clean_pair = pair.replace("/", "-").upper()
        filename = f"{clean_pair}_{timeframe}.csv"
        return exchange_dir / filename
    
    def csv_exists(self, exchange: str, pair: str, timeframe: str) -> bool:
        """
        Check if CSV file exists
        
        Args:
            exchange: Exchange name
            pair: Trading pair
            timeframe: Timeframe
        
        Returns:
            True if file exists, False otherwise
        """
        csv_path = self.get_csv_path(exchange, pair, timeframe)
        return csv_path.exists()
    
    def load_csv(self, exchange: str, pair: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Load existing CSV file
        
        Args:
            exchange: Exchange name
            pair: Trading pair
            timeframe: Timeframe
        
        Returns:
            DataFrame if file exists, None otherwise
        """
        csv_path = self.get_csv_path(exchange, pair, timeframe)
        
        if not csv_path.exists():
            logger.info(f"CSV file does not exist: {csv_path}")
            return None
        
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} rows from: {csv_path.name}")
            
            # Validate structure
            if not all(col in df.columns for col in REQUIRED_COLUMNS):
                missing = set(REQUIRED_COLUMNS) - set(df.columns)
                raise DataValidationException(
                    f"CSV file missing columns: {missing}"
                )
            
            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load CSV {csv_path}: {str(e)}")
            raise DataValidationException(
                f"Failed to load CSV: {str(e)}"
            )
    
    def save_csv(
        self,
        df: pd.DataFrame,
        exchange: str,
        pair: str,
        timeframe: str,
        deduplicate: bool = True,
    ) -> int:
        """
        Save DataFrame to CSV with optional deduplication
        
        Args:
            df: DataFrame to save
            exchange: Exchange name
            pair: Trading pair
            timeframe: Timeframe
            deduplicate: Whether to merge with existing data (default: True)
        
        Returns:
            Number of new rows added
        
        Raises:
            DataValidationException: If data validation fails
        """
        if df.empty:
            logger.warning("Empty DataFrame provided, nothing to save")
            return 0
        
        # Validate columns
        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise DataValidationException(
                f"Missing required columns: {missing}"
            )
        
        csv_path = self.get_csv_path(exchange, pair, timeframe)
        
        # Ensure timestamp is datetime
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        
        # Load existing data if deduplication is enabled
        if deduplicate and csv_path.exists():
            existing_df = self.load_csv(exchange, pair, timeframe)
            
            if existing_df is not None and not existing_df.empty:
                logger.info(f"Merging with {len(existing_df)} existing rows")
                
                # Combine dataframes
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                
                # Remove duplicates based on timestamp and symbol
                initial_count = len(combined_df)
                combined_df = combined_df.drop_duplicates(
                    subset=["timestamp", "symbol"],
                    keep="last"
                )
                duplicates_removed = initial_count - len(combined_df)
                
                if duplicates_removed > 0:
                    logger.info(f"Removed {duplicates_removed} duplicate rows")
                
                # Sort by timestamp
                combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)
                
                new_rows = len(combined_df) - len(existing_df)
                df = combined_df
            else:
                new_rows = len(df)
        else:
            new_rows = len(df)
            # Sort by timestamp
            df = df.sort_values("timestamp").reset_index(drop=True)
        
        # Save to CSV
        try:
            df.to_csv(csv_path, index=False)
            logger.info(
                f"Saved {len(df)} total rows ({new_rows} new) to: {csv_path.name}"
            )
            return new_rows
            
        except Exception as e:
            logger.error(f"Failed to save CSV {csv_path}: {str(e)}")
            raise DataValidationException(
                f"Failed to save CSV: {str(e)}"
            )
    
    def get_all_csv_files(self, exchange: Optional[str] = None) -> List[Path]:
        """
        Get list of all CSV files
        
        Args:
            exchange: Optional exchange name to filter by
        
        Returns:
            List of CSV file paths
        """
        if exchange:
            exchange_dir = self.get_exchange_dir(exchange)
            if exchange_dir.exists():
                return list(exchange_dir.glob("*.csv"))
            return []
        
        # Get all CSVs from all exchanges
        csv_files = []
        for exchange_dir in self.base_dir.iterdir():
            if exchange_dir.is_dir():
                csv_files.extend(exchange_dir.glob("*.csv"))
        
        return csv_files
    
    def get_storage_stats(self) -> dict:
        """
        Get statistics about local storage
        
        Returns:
            Dictionary with storage statistics
        """
        all_files = self.get_all_csv_files()
        
        total_size = sum(f.stat().st_size for f in all_files)
        
        # Count by exchange
        exchange_counts = {}
        for f in all_files:
            exchange = f.parent.name
            exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
        
        return {
            "total_files": len(all_files),
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "exchanges": exchange_counts,
            "base_dir": str(self.base_dir.absolute()),
        }
    
    def print_storage_summary(self):
        """Print a summary of local storage"""
        stats = self.get_storage_stats()
        
        print("\n" + "=" * 70)
        print("📁 LOCAL STORAGE SUMMARY")
        print("=" * 70)
        print(f"Location:     {stats['base_dir']}")
        print(f"Total Files:  {stats['total_files']}")
        print(f"Total Size:   {stats['total_size_mb']:.2f} MB")
        
        if stats['exchanges']:
            print("\nFiles by Exchange:")
            for exchange, count in stats['exchanges'].items():
                print(f"  • {exchange.capitalize()}: {count} files")
        
        print("=" * 70)
