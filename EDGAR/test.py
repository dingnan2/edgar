from sec_edgar_downloader import Downloader
import requests
import sys
import time
from pathlib import Path
# Initialize the downloader. Replace with your company name and email
url = "https://www.sec.gov/Archives/edgar/data/940944/0000940944-25-000003-index.htm"
headers = {
    "User-Agent": "YourName YourEmail@example.com",  # SEC requires this
    "Accept-Encoding": "gzip, deflate"
}

# Create and prepare your data directory
dest_dir = Path("data")
dest_dir.mkdir(parents=True, exist_ok=True)

# Define destination file path—the actual file to write
dest_file = dest_dir / "0001615774-15-000008-index.htm"

resp = requests.get(url, headers=headers)
time.sleep(0.2)  # respectful pause to stay within SEC rate limits

if resp.status_code == 200:
    # Write bytes to the file
    dest_file.write_bytes(resp.content)
    print(f"✅ Downloaded and saved to '{dest_file}'")
else:
    print(f"⚠️ Failed (HTTP {resp.status_code}) to download {url}")