#!/usr/bin/env python3
"""
Configuration settings for SEC EDGAR downloader
"""
from pathlib import Path
import pdfkit

# Storage Paths
BASE_DATA_DIR = Path("sec-xbrl")
DATABASE_PATH = BASE_DATA_DIR / "edgar_filings.db"
OLD_JSON_LOG = "download_log.json"  # For migration

# pdf_config = pdfkit.configuration(wkhtmltopdf='C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe')
# SEC API Settings
SEC_RATE_LIMIT = 9  # Safe buffer under 10 req/sec
TOKEN_BUCKET_CAPACITY = 10
TOKEN_BUCKET_REFILL_RATE = 9
SEC_USER_AGENT = 'MIT  data@mit.edu'
SEC_HEADERS = {
    'User-Agent': SEC_USER_AGENT,
    'Accept-Encoding': 'gzip, deflate',
    'Accept': 'application/json, text/html, */*'
}

# Download Settings
FORM_TYPES = ['10-K', '10-Q', '10-K/A', '10-Q/A']
COMPANY_CSV = "merged_companies.csv"
CIK_LOOKUP = "data/cik-lookup-data.txt"
# File Types to Download (only these two)
DOWNLOAD_FILES = ['complete_filing', 'excel_financial']  # .txt and .xlsx only

# Auto-retry settings
MAX_RETRY_ATTEMPTS = 50
USER_AGENTS = [
    'SBU  stony@sbu.edu',
    'SAU  show@sau.edu', 
    'NYU  research@nyu.edu',
    'MIT  data@mit.edu',
    'UCLA  analysis@ucla.edu'
]

# Ensure base directory exists
BASE_DATA_DIR.mkdir(exist_ok=True)