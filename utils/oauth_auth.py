"""
OAuth2 Authentication Alternative
Use this instead of service account if quota issues persist
"""
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
from pathlib import Path

# Scopes needed
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

def get_oauth_credentials(credentials_file: str = 'credentials/oauth_credentials.json') -> Credentials:
    """
    Get OAuth2 credentials using user's Google account
    
    This uses YOUR Google account instead of a service account,
    which means files will be created in YOUR Drive with YOUR quota.
    
    Args:
        credentials_file: Path to OAuth2 client credentials JSON
    
    Returns:
        Credentials object
    """
    creds = None
    token_file = Path('credentials/token.pickle')
    
    # Check if we have saved credentials
    if token_file.exists():
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Refreshing expired credentials...")
            creds.refresh(Request())
        else:
            print("\n🔐 OAuth2 Setup Required")
            print("=" * 60)
            print("This will open a browser window for you to:")
            print("1. Sign in to your Google account")
            print("2. Grant access to Google Sheets and Drive")
            print("3. Files will be created in YOUR Drive (not service account)")
            print("=" * 60)
            input("Press Enter to continue...")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
        
        print("✅ OAuth2 credentials saved!")
    
    return creds


def connect_gsheets_oauth():
    """
    Connect to Google Sheets using OAuth2 (user account)
    
    Returns:
        Tuple of (gspread client, credentials)
    """
    creds = get_oauth_credentials()
    client = gspread.authorize(creds)
    return client, creds



