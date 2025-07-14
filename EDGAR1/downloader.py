from edgar_downloader import EDGARDownloader
from pathlib import Path

# Initialize the downloader
downloader = EDGARDownloader()

# Input: company ticker and target directory
ticker = "DBRG"
target_dir = Path("GDS")

# # Get CIK from ticker
#cik = downloader.get_cik_by_ticker(ticker)
cik =  '0001526125'
# Process both 10-K and 10-Q
for form in ["10-K", "10-Q","20-F"]:
    filings = downloader.get_company_recent_filings(cik, form_type=form)
    for filing in filings:
        downloader.download_filings(cik, target_dir, filing)