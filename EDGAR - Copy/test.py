# from utils.sec_database import SECDatabase

# db = SECDatabase()
# cik = '0001434740'
# db.print_filings_by_cik(cik)

from edgar_downloader import EDGARDownloader
from pathlib import Path
downloader = EDGARDownloader()
link = 'https://www.sec.gov/Archives/edgar/data/1458631/000147793222006489/0001477932-22-006489.txt'
print(downloader.required_urls(link))