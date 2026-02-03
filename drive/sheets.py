"""
Google Sheets Manager
Handles all Google Sheets and Drive operations with improved error handling
"""
import gspread
import pandas as pd
from typing import List, Tuple, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import time

from config.config import (
    REQUIRED_COLUMNS,
    TF_SHEET_NAMES,
    GOOGLE_SCOPES,
    DEFAULT_SHEET_ROWS,
    DEFAULT_SHEET_COLS,
)
from utils.logger import setup_logger
from utils.exceptions import GoogleSheetsException, StorageQuotaException

logger = setup_logger(__name__)






def get_or_create_spreadsheet_in_folder(
    client: gspread.Client,
    creds: Credentials,
    exchange: str,
    pair: str,
    folder_id: str,
) -> gspread.Spreadsheet:
    """
    Create or retrieve spreadsheet in specified Drive folder
    Format: Exchange(pair) - e.g., Coinbase(BTC-USD)
    
    ULTIMATE WORKAROUND: Creates spreadsheet using Sheets API directly,
    completely bypassing Drive API to avoid service account quota bugs
    
    Args:
        client: Authorized gspread client
        creds: Google credentials
        exchange: Exchange name (e.g., 'coinbase')
        pair: Trading pair (e.g., 'BTC-USD')
        folder_id: Google Drive folder ID
    
    Returns:
        gspread Spreadsheet object
    
    Raises:
        GoogleSheetsException: If creation/retrieval fails
    """
    spreadsheet_name = f"{exchange.capitalize()}({pair})"
    
    try:
        drive_service = build("drive", "v3", credentials=creds)

        # Search for existing spreadsheet in folder
        query = (
            f"mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name='{spreadsheet_name}' "
            f"and '{folder_id}' in parents "
            f"and trashed=false"
        )

        results = drive_service.files().list(
            q=query,
            fields="files(id, name, createdTime, size)"
        ).execute()

        files = results.get("files", [])

        if files:
            logger.info(f"Found existing spreadsheet: {spreadsheet_name}")
            return client.open_by_key(files[0]["id"])

        # ULTIMATE WORKAROUND: Use Sheets API directly instead of Drive API
        logger.info(f"Creating spreadsheet via Sheets API (bypassing Drive quota): {spreadsheet_name}")
        
        spreadsheet_id = None
        
        try:
            from googleapiclient.discovery import build as api_build
            sheets_service = api_build('sheets', 'v4', credentials=creds)
            
            # Create spreadsheet using Sheets API directly
            spreadsheet_body = {
                'properties': {
                    'title': spreadsheet_name
                }
            }
            
            request = sheets_service.spreadsheets().create(body=spreadsheet_body)
            response = request.execute()
            spreadsheet_id = response['spreadsheetId']
            
            logger.info(f"Successfully created spreadsheet with ID: {spreadsheet_id}")
            
        except HttpError as sheets_error:
            # If Sheets API fails, try one more fallback
            logger.warning(f"Sheets API failed: {sheets_error}")
            logger.info("Trying final fallback: direct gspread creation in root...")
            
            try:
                # Last resort: create in root My Drive, don't move
                temp_sheet = client.create(spreadsheet_name)
                spreadsheet_id = temp_sheet.id
                logger.info(f"Created in root Drive with ID: {spreadsheet_id}")
                logger.warning(f"⚠️  Spreadsheet created in root 'My Drive', not in folder")
                logger.warning(f"   You may need to manually move it to your OHLCV folder")
            except Exception as final_error:
                logger.error(f"All creation methods failed: {final_error}")
                raise GoogleSheetsException(
                    "Service account cannot create spreadsheets.\n\n"
                    "🔴 CRITICAL: All workarounds exhausted.\n\n"
                    "SOLUTION: Switch to OAuth2 (user account):\n"
                    "1. Run: python oauth_auth.py\n"
                    "2. Follow setup instructions\n"
                    "3. Update main.py to use OAuth2\n\n"
                    "OAuth2 bypasses service account issues entirely."
                )
        
        # Now move it to the target folder using Drive API
        # This operation doesn't hit quota issues
        logger.info(f"Moving spreadsheet to folder: {folder_id}")
        
        try:
            # Get current parents
            file = drive_service.files().get(
                fileId=spreadsheet_id,
                fields='parents'
            ).execute()
            
            previous_parents = ",".join(file.get('parents', []))
            
            # Move to target folder
            drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            logger.info(f"Spreadsheet moved to target folder successfully")
        except HttpError as move_error:
            # If move fails, that's okay - spreadsheet is still created
            logger.warning(f"Could not move to folder (but spreadsheet created): {move_error}")
            logger.warning(f"You can manually move it to your folder in Drive")
        
        # Open with gspread
        return client.open_by_key(spreadsheet_id)
        
    except HttpError as e:
        # Enhanced error logging
        logger.error(f"HTTP Error Details:")
        logger.error(f"  Status: {e.resp.status}")
        logger.error(f"  Error details: {e.error_details}")
        logger.error(f"  Full error: {str(e)}")
        
        raise GoogleSheetsException(
            f"Failed to create spreadsheet: {e.resp.status} - {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__} - {str(e)}")
        raise GoogleSheetsException(
            f"Failed to create/get spreadsheet: {str(e)}"
        )


def ensure_timeframe_tables(
    spreadsheet: gspread.Spreadsheet,
    timeframes: List[str],
) -> None:
    """
    Ensure worksheets for each timeframe exist with proper headers
    Prevents duplicate creation
    
    Args:
        spreadsheet: gspread Spreadsheet object
        timeframes: List of timeframes to create (e.g., ['1h', '4h'])
    
    Raises:
        GoogleSheetsException: If worksheet creation fails
    """
    try:
        existing_titles = {ws.title for ws in spreadsheet.worksheets()}
        logger.info(f"Existing worksheets: {existing_titles}")

        for tf in timeframes:
            if tf not in TF_SHEET_NAMES:
                raise ValueError(
                    f"Unsupported timeframe: {tf}. "
                    f"Supported: {', '.join(TF_SHEET_NAMES.keys())}"
                )

            tab_name = TF_SHEET_NAMES[tf]

            # Create worksheet if it doesn't exist
            if tab_name not in existing_titles:
                logger.info(f"Creating worksheet: {tab_name}")
                ws = spreadsheet.add_worksheet(
                    title=tab_name,
                    rows=DEFAULT_SHEET_ROWS,
                    cols=DEFAULT_SHEET_COLS,
                )
                ws.append_row(REQUIRED_COLUMNS)
                logger.info(f"Created worksheet {tab_name} with headers")
                continue

            # If exists, verify header exists
            ws = spreadsheet.worksheet(tab_name)
            values = ws.get_all_values()

            if not values or values[0] != REQUIRED_COLUMNS:
                logger.warning(f"Worksheet {tab_name} has incorrect/missing headers")
                if not values:
                    ws.append_row(REQUIRED_COLUMNS)
                else:
                    # Update first row with correct headers
                    ws.update('A1', [REQUIRED_COLUMNS])
                logger.info(f"Fixed headers for worksheet {tab_name}")
            else:
                logger.info(f"Worksheet {tab_name} already exists with correct headers")
                
    except Exception as e:
        raise GoogleSheetsException(
            f"Failed to ensure timeframe worksheets: {str(e)}"
        )


def append_ohlcv_dataframe(
    worksheet: gspread.Worksheet,
    df: pd.DataFrame,
    index_col: str = "timestamp",
    batch_size: int = 100,
) -> int:
    """
    Append OHLCV DataFrame to worksheet with duplicate prevention
    Deduplication based on timestamp column
    Data is appended in batches to avoid API limits
    
    Args:
        worksheet: gspread Worksheet object
        df: Pandas DataFrame with OHLCV data
        index_col: Column name to use for deduplication (default: 'timestamp')
        batch_size: Number of rows to append per batch (default: 100)
    
    Returns:
        Number of new rows appended
    
    Raises:
        GoogleSheetsException: If append operation fails
    """
    if df.empty:
        logger.warning("Empty DataFrame provided, nothing to append")
        return 0

    try:
        # Validate schema
        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        df = df[REQUIRED_COLUMNS].copy()

        # Normalize timestamp to string (Sheets-compatible)
        df[index_col] = (
            pd.to_datetime(df[index_col], utc=True)
            .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        # Get existing data
        existing_values = worksheet.get_all_values()

        # If only header or empty, append all data
        if len(existing_values) <= 1:
            logger.info("Worksheet is empty, appending all data in batches")
            total_appended = _append_in_batches(worksheet, df, batch_size)
            return total_appended

        # Extract existing timestamps for deduplication
        header = existing_values[0]
        try:
            ts_idx = header.index(index_col)
        except ValueError:
            raise GoogleSheetsException(
                f"Column '{index_col}' not found in worksheet header"
            )

        existing_timestamps = {
            row[ts_idx]
            for row in existing_values[1:]
            if len(row) > ts_idx and row[ts_idx]
        }

        logger.info(f"Found {len(existing_timestamps)} existing timestamps")

        # Filter out duplicates
        new_df = df[~df[index_col].isin(existing_timestamps)]

        if new_df.empty:
            logger.info("No new data to append (all duplicates)")
            return 0

        # Append new rows in batches
        logger.info(f"Appending {len(new_df)} new rows in batches of {batch_size}")
        total_appended = _append_in_batches(worksheet, new_df, batch_size)
        
        return total_appended
        
    except HttpError as e:
        # Enhanced error logging
        logger.error(f"HTTP Error Details:")
        logger.error(f"  Status: {e.resp.status}")
        logger.error(f"  Error details: {e.error_details}")
        logger.error(f"  Full error: {str(e)}")
        
        if e.resp.status == 403 and "storageQuotaExceeded" in str(e):
            raise StorageQuotaException(
                "Storage quota exceeded while appending data"
            )
        raise GoogleSheetsException(
            f"HTTP error while appending data: {e.resp.status} - {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {type(e).__name__} - {str(e)}")
        raise GoogleSheetsException(
            f"Failed to append data to worksheet: {str(e)}"
        )


def _append_in_batches(
    worksheet: gspread.Worksheet,
    df: pd.DataFrame,
    batch_size: int = 100,
) -> int:
    """
    Helper function to append DataFrame rows in batches
    Adds small delay between batches to respect API rate limits
    
    Args:
        worksheet: gspread Worksheet object
        df: DataFrame to append
        batch_size: Number of rows per batch
    
    Returns:
        Total number of rows appended
    """
    total_rows = len(df)
    rows_appended = 0
    
    # Convert DataFrame to list of lists once
    all_rows = df.values.tolist()
    
    # Process in batches
    for i in range(0, total_rows, batch_size):
        batch_end = min(i + batch_size, total_rows)
        batch_rows = all_rows[i:batch_end]
        
        logger.info(f"Appending batch: rows {i+1} to {batch_end} of {total_rows}")
        
        try:
            worksheet.append_rows(batch_rows)
            rows_appended += len(batch_rows)
            
            # Small delay to avoid rate limits (only if more batches remain)
            if batch_end < total_rows:
                time.sleep(0.5)  # 500ms delay between batches
                
        except HttpError as e:
            logger.error(f"Failed to append batch {i}-{batch_end}: {str(e)}")
            raise
    
    logger.info(f"Successfully appended {rows_appended} rows in {(total_rows // batch_size) + 1} batches")
    return rows_appended


def get_spreadsheet_url(spreadsheet: gspread.Spreadsheet) -> str:
    """
    Get the URL for a spreadsheet
    
    Args:
        spreadsheet: gspread Spreadsheet object
    
    Returns:
        URL string
    """
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"