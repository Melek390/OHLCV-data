import sys
from typing import List, Tuple
from exchanges.coinbase import fetch_ohlcv, validate_symbol, GRANULARITY_MAP

# TOGGLE: Set to True to use OAuth2, False for local storage only 
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
from exchanges.coinbase.unsupported_tfs import (
    generate_4h_from_1h,
    generate_1w_from_1d,
)
from config.config import (
    DRIVE_FOLDER_ID,
    TF_SHEET_NAMES,
    LOCAL_DATA_DIR,
    SUPPORTED_TIMEFRAMES,
    UNSUPPORTED_TIMEFRAMES,
    UNSUPPORTED_SOURCE_MAP,
)
from utils.logger import setup_logger
from utils.exceptions import (
    APIException,
    GoogleSheetsException,
    StorageQuotaException,
    DataValidationException,
)

logger = setup_logger(__name__)


def print_banner():
    """Print application banner"""
    print("=" * 70)
    print("📊 OHLCV Data Ingestion System v3.0")
    print("   Professional REST → Local CSV → Google Sheets Pipeline")
    print("   📁 Local storage with deduplication")
    print("   ☁️  Optional Drive backup")
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
    print("\n🔍 Available Exchanges:")
    print("  • Coinbase")
    
    while True:
        exchange = input("\nSelect exchange (default: coinbase): ").strip().lower()
        
        if not exchange:
            exchange = "coinbase"
        
        if exchange == "coinbase":
            return exchange
        
        print("❌ Invalid exchange. Only 'coinbase' is supported.")


def get_user_inputs() -> Tuple[str, List[str]]:
    """
    Get trading pair and timeframes from user
    
    Returns:
        Tuple of (pair, timeframes list)
    
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
    print(f"  {', '.join(GRANULARITY_MAP.keys())}")
    
    tf_input = input("\nEnter timeframes (comma-separated, e.g., 1h,4h,1d): ").strip().lower()
    
    if not tf_input:
        raise ValueError("At least one timeframe is required")
    
    timeframes = [tf.strip() for tf in tf_input.split(",")]
    
    # Validate timeframes
    invalid_tfs = [tf for tf in timeframes if tf not in GRANULARITY_MAP]
    if invalid_tfs:
        raise ValueError(
            f"Invalid timeframes: {', '.join(invalid_tfs)}. "
            f"Supported: {', '.join(GRANULARITY_MAP.keys())}"
        )
    
    return pair, timeframes


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
        pair, timeframes = get_user_inputs()
        
        print("\n✅ Configuration:")
        print(f"  Exchange:   {exchange.capitalize()}")
        print(f"  Pair:       {pair}")
        print(f"  Timeframes: {', '.join(timeframes)}")
        print_separator()
        
        # Validate trading pair
        logger.info(f"Validating trading pair: {pair}")
        if not validate_symbol(pair):
            raise ValueError(
                f"Trading pair '{pair}' not found on {exchange.capitalize()}. "
                "Please check the symbol and try again."
            )
        
        # Separate supported and unsupported timeframes
        supported_tfs = [tf for tf in timeframes if tf in SUPPORTED_TIMEFRAMES]
        unsupported_tfs = [tf for tf in timeframes if tf in UNSUPPORTED_TIMEFRAMES]
        
        # Cache for source data needed for unsupported timeframes
        source_data_cache = {}
        
        # Fetch and store data locally for each timeframe
        total_new_rows = 0
        
        print("\n📥 FETCHING & STORING DATA LOCALLY")
        print("=" * 70)
        
        # Process supported timeframes first (direct API fetch)
        if supported_tfs:
            print("\n🔹 Processing SUPPORTED timeframes (direct API fetch)...")
            for tf in supported_tfs:
                print(f"\n⏳ Processing {tf.upper()} timeframe...")
                
                try:
                    # Fetch data from exchange
                    logger.info(f"Fetching {tf} data from {exchange}...")
                    raw_data = fetch_ohlcv(
                        symbol=pair,
                        timeframe=tf,
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
                    
                    # Cache this data if it's needed for unsupported timeframes
                    if tf in UNSUPPORTED_SOURCE_MAP.values():
                        source_data_cache[tf] = df.copy()
                        logger.info(f"Cached {tf} data for generating unsupported timeframes")
                    
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
        
        # Process unsupported timeframes (generate from source data)
        if unsupported_tfs:
            print("\n🔸 Processing UNSUPPORTED timeframes (generating from source data)...")
            
            for tf in unsupported_tfs:
                print(f"\n⏳ Processing {tf.upper()} timeframe...")
                
                try:
                    # Get source timeframe needed
                    source_tf = UNSUPPORTED_SOURCE_MAP.get(tf)
                    
                    if not source_tf:
                        print(f"  ❌ No source timeframe mapping for {tf}")
                        continue
                    
                    print(f"  ℹ️  Unsupported by API - will generate from {source_tf.upper()} data")
                    
                    # Check if we have source data in cache
                    if source_tf in source_data_cache:
                        source_df = source_data_cache[source_tf]
                        print(f"  📦 Using cached {source_tf.upper()} data ({len(source_df)} rows)")
                    else:
                        # Try to load from local CSV
                        print(f"  📂 Loading {source_tf.upper()} data from local CSV...")
                        source_df = local_storage.load_csv(exchange, pair, source_tf)
                        
                        if source_df is None or source_df.empty:
                            print(f"  ❌ No {source_tf.upper()} data available locally")
                            print(f"     Please fetch {source_tf.upper()} data first!")
                            continue
                        
                        print(f"  ✅ Loaded {len(source_df)} rows from {source_tf.upper()} CSV")
                    
                    # Generate target timeframe data
                    logger.info(f"Generating {tf} from {source_tf}...")
                    
                    if tf == "4h":
                        df = generate_4h_from_1h(source_df)
                    elif tf == "1w":
                        df = generate_1w_from_1d(source_df)
                    else:
                        print(f"  ❌ Unknown unsupported timeframe: {tf}")
                        continue
                    
                    if df.empty:
                        print(f"  ⚠️  No data generated")
                        continue
                    
                    print(f"  ✅ Generated {len(df)} {tf.upper()} candles")
                    
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
        
        # Ask if user wants to upload to Drive (only if we have new data)
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