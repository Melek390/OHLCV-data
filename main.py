"""
OHLCV Data Ingestion System - Main Application
Fetches OHLCV data from exchanges and stores locally and/or in Google Sheets
Supports: 5m, 30m, 1h, 6h, 1d timeframes
"""
import sys
import pandas as pd
from typing import List, Tuple, Optional

from exchanges.coinbase import fetch_ohlcv as fetch_exchange, validate_symbol
from exchanges.coinbase.advanced_trade import fetch_ohlcv_advanced
from exchanges.coinbase.weekly_aggregator import aggregate_to_weekly, calculate_required_daily_candles

# TOGGLE: Set to True to use OAuth2, False for service account
USE_OAUTH = True
from drive.sheets import (

        get_or_create_spreadsheet_in_folder,
        ensure_timeframe_tables,
        append_ohlcv_dataframe,
    
        get_spreadsheet_url,
    )

if USE_OAUTH:
    from utils.oauth_auth import connect_gsheets_oauth
    
from drive.data_manager import rest_to_dataframe
from storage.local_storage import LocalStorage
from config.config import (
    DRIVE_FOLDER_ID,
    TF_SHEET_NAMES,
    LOCAL_DATA_DIR,
    ALL_TIMEFRAMES,
    ADVANCED_TRADE_TIMEFRAMES,
    GRANULARITY_MAP,
)
from utils.logger import setup_logger
from utils.exceptions import (
    OHLCVException,
    APIException,
    GoogleSheetsException,
    StorageQuotaException,
    DataValidationException,
)

logger = setup_logger(__name__)


def print_banner():
    """Print application banner"""
    print("=" * 70)
    print("📊 OHLCV Data Ingestion System v4.2")
    print("   Professional REST → Local CSV → Google Sheets Pipeline")
    print("   📁 Local storage with deduplication")
    print("   ☁️  Optional Drive backup")
    print("   📈 Supports: 5m, 30m, 1h, 6h, 1d, 1w")
    print("   🔄 Automatic pagination for historical data")
    print("   🔑 30m data via CDP API (cdp_api_key.json)")
    print("=" * 70)


def print_separator():
    """Print visual separator"""
    print("-" * 70)


def select_exchange() -> str:
    """
    Prompt user to select exchange
    
    Returns:
        Exchange name
    """
    print("\n🏢 Available Exchanges:")
    print("  • Coinbase")
    
    while True:
        exchange = input("\nSelect exchange (default: coinbase): ").strip().lower()
        
        if not exchange:
            exchange = "coinbase"
        
        if exchange == "coinbase":
            return exchange
        
        print("❌ Invalid exchange. Only 'coinbase' is supported.")


def get_user_inputs() -> Tuple[str, List[str], Optional[int], Optional[int]]:
    """
    Get trading pair, timeframes, start year, and end year from user
    
    Returns:
        Tuple of (pair, timeframes list, start_year, end_year)
    
    Raises:
        ValueError: If inputs are invalid
    """
    # Get trading pair
    pair = input("\n📈 Enter trading pair (default: BTC-USD): ").strip().upper()
    if not pair:
        pair = "BTC-USD"
    
    # Validate pair format
    if "-" not in pair:
        raise ValueError("Trading pair must be in format: XXX-YYY (e.g., BTC-USD)")
    
    # Get timeframes
    print("\n⏱️  Available Timeframes:")
    print(f"  {', '.join(ALL_TIMEFRAMES)}")
    print("\n  📌 Notes:")
    print("    • 5m, 1h, 6h, 1d: Exchange API (REST)")
    print("    • 30m: Advanced Trade API")
    
    tf_input = input("\nEnter timeframes (comma-separated, e.g., 5m,1h,1d): ").strip().lower()
    
    if not tf_input:
        raise ValueError("At least one timeframe is required")
    
    timeframes = [tf.strip() for tf in tf_input.split(",")]
    
    # Validate timeframes
    invalid_tfs = [tf for tf in timeframes if tf not in ALL_TIMEFRAMES]
    if invalid_tfs:
        raise ValueError(
            f"Invalid timeframes: {', '.join(invalid_tfs)}. "
            f"Supported: {', '.join(ALL_TIMEFRAMES)}"
        )
    
    # Get date range for historical data
    print("\n📅 Historical Data Range (Optional)")
    print("   Enter start and end years to fetch data for a specific period")
    print("   System will automatically paginate to get all data in the range")
    print("   Press Enter to skip both (fetch latest ~300 candles only)")
    print("\n   Examples:")
    print("     • Start: 2022, End: 2024  → Fetches Jan 1, 2022 to Dec 31, 2024")
    print("     • Start: 2023, End: 2023  → Fetches only year 2023")
    print("     • Start: (skip), End: 2024 → Fetches 2 years before 2024 to Dec 31, 2024")
    
    # Get start year
    start_year_input = input("\nStart year (e.g., 2022) [Press Enter to skip]: ").strip()
    start_year = None
    if start_year_input:
        try:
            start_year = int(start_year_input)
            if start_year < 2010 or start_year > 2030:
                raise ValueError("Start year must be between 2010 and 2030")
            logger.info(f"Start year: {start_year}")
        except ValueError as e:
            raise ValueError(f"Invalid start year: {start_year_input}. Must be a 4-digit year.")
    
    # Get end year
    end_year_input = input("End year (e.g., 2024) [Press Enter to skip]: ").strip()
    end_year = None
    if end_year_input:
        try:
            end_year = int(end_year_input)
            if end_year < 2010 or end_year > 2030:
                raise ValueError("End year must be between 2010 and 2030")
            
            # Validate year range
            if start_year and end_year < start_year:
                raise ValueError(f"End year ({end_year}) cannot be before start year ({start_year})")
            
            logger.info(f"End year: {end_year}")
        except ValueError as e:
            raise ValueError(f"Invalid end year: {end_year_input}. Must be a 4-digit year.")
    
    # Display selected range
    if start_year and end_year:
        print(f"\n✅ Will fetch data from Jan 1, {start_year} to Dec 31, {end_year}")
    elif start_year:
        print(f"\n✅ Will fetch data starting from {start_year}")
    elif end_year:
        print(f"\n✅ Will fetch data up to Dec 31, {end_year} (auto-determined start date)")
    else:
        print(f"\n✅ Will fetch latest ~300 candles (no year range specified)")
    
    return pair, timeframes, start_year, end_year


def ask_drive_upload() -> bool:
    """
    Ask user if they want to upload to Google Drive
    
    Returns:
        True if user wants to upload, False otherwise
    """
    print("\n" + "=" * 70)
    print("☁️  GOOGLE DRIVE BACKUP")
    print("=" * 70)
    print("Data has been saved locally successfully!")
    print("\nWould you like to also upload a copy to Google Drive?")
    print("  • Yes: Data will be synced to Google Sheets")
    print("  • No:  Data stays local only")
    
    while True:
        response = input("\nUpload to Drive? (y/n): ").strip().lower()
        
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no")


def upload_to_drive(
    local_storage: LocalStorage,
    exchange: str,
    pair: str,
    timeframes: List[str],
    client,
    creds,
) -> None:
    """
    Upload local CSV data to Google Drive
    
    Args:
        local_storage: LocalStorage instance
        exchange: Exchange name
        pair: Trading pair
        timeframes: List of timeframes
        client: Google Sheets client
        creds: Google credentials
    """
    print("\n📤 Uploading to Google Drive...")
    print_separator()
    
    # Get or create spreadsheet
    logger.info(f"Setting up spreadsheet for {exchange}({pair})...")
    spreadsheet = get_or_create_spreadsheet_in_folder(
        client=client,
        creds=creds,
        exchange=exchange,
        pair=pair,
        folder_id=DRIVE_FOLDER_ID,
    )
    
    spreadsheet_url = get_spreadsheet_url(spreadsheet)
    print(f"✅ Spreadsheet ready: {spreadsheet.title}")
    print(f"   URL: {spreadsheet_url}")
    
    # Ensure timeframe worksheets exist
    logger.info("Setting up timeframe worksheets...")
    ensure_timeframe_tables(spreadsheet, timeframes)
    print("✅ Timeframe worksheets verified")
    
    print_separator()
    
    # Upload data for each timeframe
    total_uploaded = 0
    
    for tf in timeframes:
        print(f"\n📤 Uploading {tf.upper()} data...")
        
        try:
            # Check if CSV exists first
            if not local_storage.csv_exists(exchange, pair, tf):
                print(f"  ⚠️  No local CSV file found for {tf} (skipping)")
                continue
            
            # Load from local CSV
            df = local_storage.load_csv(exchange, pair, tf)
            
            if df is None or df.empty:
                print(f"  ⚠️  Local CSV is empty for {tf} (skipping)")
                continue
            
            print(f"  📁 Loaded {len(df)} rows from local CSV")
            
            # Append to worksheet
            ws_name = TF_SHEET_NAMES[tf]
            worksheet = spreadsheet.worksheet(ws_name)
            
            logger.info(f"Uploading data to worksheet {ws_name}...")
            
            added = append_ohlcv_dataframe(
                worksheet=worksheet,
                df=df,
                index_col="timestamp",
                batch_size=100,
            )
            
            if added > 0:
                print(f"  ✅ Uploaded {added} new rows to {ws_name}")
                total_uploaded += added
            else:
                print(f"  ℹ️  All data already exists in Drive (0 new rows)")
            
        except Exception as e:
            print(f"  ❌ Upload Error for {tf}: {str(e)}")
            logger.error(f"Upload error for {tf}: {str(e)}")
            continue
    
    # Summary
    print_separator()
    print(f"\n🎯 Drive Upload Completed!")
    print(f"   Total new rows uploaded: {total_uploaded}")
    print(f"   Spreadsheet: {spreadsheet_url}")


def main():
    """Main application entry point"""
    print_banner()
    
    try:
        
        # Initialize local storage
        local_storage = LocalStorage(base_dir=str(LOCAL_DATA_DIR))
        
        # Get user inputs
        exchange = select_exchange()
        pair, timeframes, start_year, end_year = get_user_inputs()
        
        print("\n✅ Configuration:")
        print(f"  Exchange:   {exchange.capitalize()}")
        print(f"  Pair:       {pair}")
        print(f"  Timeframes: {', '.join(timeframes)}")
        if start_year and end_year:
            print(f"  Start Date: January 1, {start_year}")
            print(f"  End Date:   December 31, {end_year}")
        elif start_year:
            print(f"  Start Date: January 1, {start_year}")
            print(f"  End Date:   Latest available")
        elif end_year:
            print(f"  Start Date: Auto-determined (2 years before {end_year})")
            print(f"  End Date:   December 31, {end_year}")
        else:
            print(f"  Date Range: Latest ~300 candles")
        print_separator()
        
        # Validate trading pair
        logger.info(f"Validating trading pair: {pair}")
        if not validate_symbol(pair):
            raise ValueError(
                f"Trading pair '{pair}' not found on {exchange.capitalize()}. "
                "Please check the symbol and try again."
            )
        
        # Separate timeframes by source
        # Exchange API: any TF present in GRANULARITY_MAP that isn't handled by another source
        exchange_tfs = [tf for tf in timeframes if tf in GRANULARITY_MAP and tf not in ADVANCED_TRADE_TIMEFRAMES and tf != '1w']
        advanced_tfs = [tf for tf in timeframes if tf in ADVANCED_TRADE_TIMEFRAMES]
        weekly_tfs = [tf for tf in timeframes if tf == '1w']

        logger.info(f"Timeframe routing — Exchange API: {exchange_tfs}, Advanced Trade: {advanced_tfs}, Weekly: {weekly_tfs}")
        print(f"\n📋 Timeframe routing:")
        print(f"   Exchange API (direct fetch): {', '.join(exchange_tfs) if exchange_tfs else 'none'}")
        print(f"   Advanced Trade API:          {', '.join(advanced_tfs) if advanced_tfs else 'none'}")
        print(f"   Weekly aggregation:          {', '.join(weekly_tfs) if weekly_tfs else 'none'}")  # Weekly requires aggregation
        
        # Fetch and store data locally for each timeframe
        total_new_rows = 0
        
        print("\n📥 FETCHING & STORING DATA LOCALLY")
        print("=" * 70)
        
        # Process Exchange API timeframes (5m, 1h, 6h, 1d)
        if exchange_tfs:
            print("\n🔹 Processing EXCHANGE API timeframes...")
            
            for tf in exchange_tfs:
                print(f"\n⏳ Processing {tf.upper()} timeframe...")
                
                try:
                    # Fetch data from exchange
                    logger.info(f"Fetching {tf} data from {exchange} Exchange API...")
                    raw_data = fetch_exchange(
                        symbol=pair,
                        timeframe=tf,
                        start_year=start_year,
                        end_year=end_year,
                    )
                    
                    if not raw_data:
                        print(f"  ⚠️  No data returned from API")
                        continue
                    
                    print(f"  📥 Fetched {len(raw_data)} candles from API")
                    
                    # Convert to DataFrame
                    logger.info("Converting to DataFrame...")
                    df = rest_to_dataframe(raw_data)
                    
                    if df.empty:
                        print(f"  ⚠️  No valid data after processing")
                        continue
                    
                    print(f"  ✅ Processed {len(df)} valid candles")
                    
                    # Save to local CSV
                    logger.info(f"Saving to local CSV...")
                    csv_path = local_storage.get_csv_path(exchange, pair, tf)
                    logger.info(f"Target CSV path: {csv_path}")
                    
                    added = local_storage.save_csv(
                        df=df,
                        exchange=exchange,
                        pair=pair,
                        timeframe=tf,
                        deduplicate=True,
                    )
                    
                    if added > 0:
                        print(f"  ✅ Saved {added} new rows to: {csv_path.name}")
                        print(f"     Full path: {csv_path}")
                        total_new_rows += added
                    else:
                        print(f"  ℹ️  No new data (all existing in: {csv_path.name})")
                        print(f"     Full path: {csv_path}")
                    
                except APIException as e:
                    print(f"  ❌ API Error for {tf}: {str(e)}")
                    logger.error(f"API error for {tf}: {str(e)}")
                    continue
                except DataValidationException as e:
                    print(f"  ❌ Data Validation Error for {tf}: {str(e)}")
                    logger.error(f"Data validation error for {tf}: {str(e)}")
                    continue
                except Exception as e:
                    print(f"  ❌ Error for {tf}: {str(e)}")
                    logger.error(f"Error for {tf}: {str(e)}")
                    continue
        
        # Process Advanced Trade API timeframes (30m)
        if advanced_tfs:
            print("\n🔸 Processing ADVANCED TRADE API timeframes...")
            print("   (Direct API fetch - no data generation)")
            
            for tf in advanced_tfs:
                print(f"\n⏳ Processing {tf.upper()} timeframe...")
                
                try:
                    # Fetch directly from Advanced Trade API
                    logger.info(f"Fetching {tf} data from Advanced Trade API...")
                    raw_data = fetch_ohlcv_advanced(
                        symbol=pair,
                        timeframe=tf,
                        start_year=start_year,
                        end_year=end_year,
                    )
                    
                    if not raw_data:
                        print(f"  ⚠️  No data returned from API")
                        continue
                    
                    print(f"  📥 Fetched {len(raw_data)} candles from API")
                    
                    # Convert to DataFrame
                    logger.info("Converting to DataFrame...")
                    df = rest_to_dataframe(raw_data)
                    
                    if df.empty:
                        print(f"  ⚠️  No valid data after processing")
                        continue
                    
                    print(f"  ✅ Processed {len(df)} valid candles")
                    
                    # Save to local CSV
                    logger.info(f"Saving to local CSV...")
                    csv_path = local_storage.get_csv_path(exchange, pair, tf)
                    logger.info(f"Target CSV path: {csv_path}")
                    
                    added = local_storage.save_csv(
                        df=df,
                        exchange=exchange,
                        pair=pair,
                        timeframe=tf,
                        deduplicate=True,
                    )
                    
                    if added > 0:
                        print(f"  ✅ Saved {added} new rows to: {csv_path.name}")
                        print(f"     Full path: {csv_path}")
                        total_new_rows += added
                    else:
                        print(f"  ℹ️  No new data (all existing in: {csv_path.name})")
                        print(f"     Full path: {csv_path}")
                    
                except APIException as e:
                    print(f"  ❌ API Error for {tf}: {str(e)}")
                    logger.error(f"API error for {tf}: {str(e)}")
                    continue
                except DataValidationException as e:
                    print(f"  ❌ Data Validation Error for {tf}: {str(e)}")
                    logger.error(f"Data validation error for {tf}: {str(e)}")
                    continue
                except Exception as e:
                    print(f"  ❌ Error for {tf}: {str(e)}")
                    logger.error(f"Error for {tf}: {str(e)}")
                    continue
        
        # Process Weekly timeframes (1w) - requires aggregation from daily data
        if weekly_tfs:
            print("\n🔷 Processing WEEKLY AGGREGATION timeframes...")
            print("   (Aggregated from daily 1d data)")
            
            for tf in weekly_tfs:
                print(f"\n⏳ Processing {tf.upper()} timeframe...")
                
                try:
                    # Calculate required daily candles for the requested date range
                    if start_year or end_year:
                        num_daily = calculate_required_daily_candles(
                            start_year=start_year,
                            end_year=end_year
                        )
                        print(f"  📊 Need ~{num_daily} daily candles for {start_year or 'start'} to {end_year or 'now'}")
                    else:
                        # Default to ~100 weekly candles = ~700 daily
                        num_daily = 700
                        print(f"  📊 Need ~{num_daily} daily candles (default range)")
                    
                    # Check if we have daily data
                    daily_csv_path = local_storage.get_csv_path(exchange, pair, '1d')
                    should_fetch = False
                    
                    if not daily_csv_path.exists():
                        print(f"  ⚠️  Daily (1d) data not found - need to fetch")
                        should_fetch = True
                    else:
                        # Load existing daily data to check date range coverage
                        print(f"  📁 Checking existing daily data coverage...")
                        df_daily = local_storage.load_csv(exchange, pair, '1d')
                        
                        if df_daily is None or df_daily.empty:
                            print(f"  ⚠️  Daily CSV is empty - need to fetch")
                            should_fetch = True
                        else:
                            # Check date range coverage
                            existing_start = df_daily['timestamp'].min()
                            existing_end = df_daily['timestamp'].max()
                            
                            print(f"  📅 Existing data: {existing_start.strftime('%Y-%m-%d')} to {existing_end.strftime('%Y-%m-%d')} ({len(df_daily)} candles)")
                            
                            # Determine required date range
                            from datetime import datetime
                            if start_year and end_year:
                                required_start = pd.Timestamp(datetime(start_year, 1, 1), tz='UTC')
                                required_end = pd.Timestamp(datetime(end_year, 12, 31, 23, 59, 59), tz='UTC')
                            elif start_year:
                                required_start = pd.Timestamp(datetime(start_year, 1, 1), tz='UTC')
                                required_end = pd.Timestamp(datetime.utcnow(), tz='UTC')
                            elif end_year:
                                required_start = pd.Timestamp(datetime(end_year - 2, 1, 1), tz='UTC')
                                required_end = pd.Timestamp(datetime(end_year, 12, 31, 23, 59, 59), tz='UTC')
                            else:
                                # No year range, existing data is fine
                                required_start = None
                                required_end = None
                            
                            # Check if existing data covers required range
                            if required_start and required_end:
                                print(f"  📅 Required data: {required_start.strftime('%Y-%m-%d')} to {required_end.strftime('%Y-%m-%d')}")
                                
                                # Allow some tolerance (7 days on each end)
                                start_gap = (existing_start - required_start).days
                                end_gap = (required_end - existing_end).days
                                
                                if start_gap > 7:
                                    print(f"  ⚠️  Missing data at start: {abs(start_gap)} days gap")
                                    should_fetch = True
                                elif end_gap > 7:
                                    print(f"  ⚠️  Missing data at end: {end_gap} days gap")
                                    should_fetch = True
                                else:
                                    print(f"  ✅ Existing data covers requested range")
                                    should_fetch = False
                            else:
                                # No specific range required, use existing
                                print(f"  ✅ Using existing daily data (no specific range requested)")
                                should_fetch = False
                    
                    # Fetch daily data if needed
                    if should_fetch:
                        print(f"  🌐 Fetching {num_daily} daily candles from API...")
                        logger.info(f"Fetching {num_daily} daily candles for weekly aggregation...")
                        
                        # Fetch daily data
                        raw_daily = fetch_exchange(
                            symbol=pair,
                            timeframe='1d',
                            num_candles=num_daily,
                            start_year=start_year,
                            end_year=end_year,
                        )
                        
                        if not raw_daily:
                            print(f"  ❌ Failed to fetch daily data - cannot create weekly")
                            continue
                        
                        print(f"  📥 Fetched {len(raw_daily)} daily candles from API")
                        
                        # Convert to DataFrame
                        df_daily = rest_to_dataframe(raw_daily)
                        
                        # Save daily data (will merge with existing if any)
                        added = local_storage.save_csv(
                            df=df_daily,
                            exchange=exchange,
                            pair=pair,
                            timeframe='1d',
                            deduplicate=True,
                        )
                        print(f"  💾 Saved {added} new daily candles to: {daily_csv_path.name}")
                        
                        # Reload to get merged data
                        df_daily = local_storage.load_csv(exchange, pair, '1d')
                    
                    # At this point df_daily should be loaded (either existing or freshly fetched)
                    if df_daily is None or df_daily.empty:
                        print(f"  ❌ No daily data available - cannot aggregate")
                        continue
                    
                    print(f"  ✅ Using {len(df_daily)} daily candles for aggregation")
                    
                    # Convert DataFrame back to list of dicts for aggregation
                    logger.info("Aggregating daily data to weekly...")
                    daily_data = df_daily.to_dict('records')
                    
                    # Aggregate to weekly
                    weekly_data = aggregate_to_weekly(daily_data)
                    
                    if not weekly_data:
                        print(f"  ⚠️  No weekly data generated from aggregation")
                        continue
                    
                    print(f"  🔄 Aggregated {len(df_daily)} daily → {len(weekly_data)} weekly candles")
                    
                    # Convert to DataFrame
                    df_weekly = rest_to_dataframe(weekly_data)
                    
                    if df_weekly.empty:
                        print(f"  ⚠️  No valid weekly data after processing")
                        continue
                    
                    # Save to local CSV
                    logger.info(f"Saving weekly data to local CSV...")
                    csv_path = local_storage.get_csv_path(exchange, pair, tf)
                    
                    added = local_storage.save_csv(
                        df=df_weekly,
                        exchange=exchange,
                        pair=pair,
                        timeframe=tf,
                        deduplicate=True,
                    )
                    
                    if added > 0:
                        print(f"  ✅ Saved {added} new rows to: {csv_path.name}")
                        print(f"     Full path: {csv_path}")
                        total_new_rows += added
                    else:
                        print(f"  ℹ️  No new data (all existing in: {csv_path.name})")
                        print(f"     Full path: {csv_path}")
                    
                except APIException as e:
                    print(f"  ❌ API Error for {tf}: {str(e)}")
                    logger.error(f"API error for {tf}: {str(e)}")
                    continue
                except DataValidationException as e:
                    print(f"  ❌ Data Validation Error for {tf}: {str(e)}")
                    logger.error(f"Data validation error for {tf}: {str(e)}")
                    continue
                except Exception as e:
                    print(f"  ❌ Error for {tf}: {str(e)}")
                    logger.error(f"Error for {tf}: {str(e)}")
                    continue
        
        # Local storage summary
        print_separator()
        local_storage.print_storage_summary()
        
        print(f"\n🎯 Local Storage Completed!")
        print(f"   Total new rows saved: {total_new_rows}")
        
        # Ask if user wants to upload to Drive (only if we have data)
        if total_new_rows > 0 or any(local_storage.csv_exists(exchange, pair, tf) for tf in timeframes):
            if ask_drive_upload():
                try:
                    # Connect to Google Sheets
                    logger.info("Connecting to Google Sheets API...")
                    
                    if USE_OAUTH:
                        client, creds = connect_gsheets_oauth()
                        print("✅ Connected via OAuth2 (your Google account)")
                    else:
                        pass
                    # Upload to Drive
                    upload_to_drive(
                        local_storage=local_storage,
                        exchange=exchange,
                        pair=pair,
                        timeframes=timeframes,
                        client=client,
                        creds=creds,
                    )
                    
                except GoogleSheetsException as e:
                    print(f"\n❌ Google Sheets Error:")
                    print(f"   {str(e)}")
                    print("\n💡 Your data is still saved locally!")
                    logger.error(f"Google Sheets error: {str(e)}")
                except StorageQuotaException as e:
                    print(f"\n❌ Storage Quota Error:")
                    print(f"   {str(e)}")
                    print("\n💡 Your data is still saved locally!")
                    logger.error(f"Storage quota exceeded: {str(e)}")
            else:
                print("\n✅ Data saved locally only (Drive upload skipped)")
        else:
            print("\n⚠️  No data available to upload to Drive")
        
        print("\n" + "=" * 70)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print()
        
    except ValueError as e:
        print(f"\n❌ Validation Error:")
        print(f"   {str(e)}")
        logger.error(f"Validation error: {str(e)}")
        sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Unexpected Error:")
        print(f"   {str(e)}")
        logger.exception("Unexpected error occurred")
        sys.exit(1)


if __name__ == "__main__":
    main()