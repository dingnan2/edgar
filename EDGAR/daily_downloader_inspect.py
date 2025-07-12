import requests
import re
import time
from datetime import datetime
from pathlib import Path
from utils.sec_database import SECDatabase
from edgar_downloader import EDGARDownloader
import logging
from bs4 import BeautifulSoup
from utils.txt_processing import clean_mojibake, decode_entities
import pdfkit
import shutil
from utils.config import BASE_DATA_DIR
from utils.rate_limit import SafeRateLimiter
import sys

class SECDailyIndexDownloader:
    def __init__(self, base_dir=BASE_DATA_DIR, rate_limit=0.16):
        self.base_url = "https://www.sec.gov/Archives/edgar/daily-index"
        self.target_forms = ['10-K', '10-Q', '10-K/A', '10-Q/A']
        self.base_dir = Path(base_dir)
        # Initialize database and downloader
        self.database = SECDatabase()
        self.downloader = EDGARDownloader(base_dir=base_dir, rate_limit=rate_limit)
        self.rate_limit = rate_limit
        self.rate_limiter = SafeRateLimiter()

        self.headers = {
            'User-Agent': 'MIT  data@mit.edu',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': 'application/json, text/html, */*'
        }
        
        self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def setup_logging(self):
        log_dir = self.base_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log filename with timestamp
        log_filename = f"daily_index_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file_path = log_dir / log_filename
        
        # Configure logging with both file and console handlers
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file_path, encoding='utf-8'),  # File handler
                logging.StreamHandler(sys.stdout)  # Console handler
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized. Log file: {log_file_path}")
    
    def safe_request(self, url: str, **kwargs) -> requests.Response:
        '''Only stop for errors'''
        self.rate_limiter.acquire()

        if 'timeout' not in kwargs:
            kwargs['timeout'] = 30
        
        file_name = url.split('/')[-1] if '/' in url else url

        try:
            response = self.session.get(url, **kwargs)
            code = response.status_code
            if code == 429:
                self.logger.error(f"\n[STOP] RATE LIMITED (429) - STOPPING PROGRAM\n URL:{url}")
                raise SystemExit("[429 ERROR] Program stopped due to rate limit")
            
            elif code == 403:
                self.logger.error(f"\n[STOP] ACCESS FORBIDDEN (403) - STOPPING PROGRAM\n URL: {url}")
                raise SystemExit("[403 ERROR] Program stopped due to access forbidden")

            elif code == 404:
                self.logger.debug(f"[404 ERROR]: File {file_name} not found")
            
            elif code >= 500:
                self.logger.warning(f"[HTTP {code} ERROR] for {file_name}]")
                return response
            
            elif code >= 400:
                self.logger.warning(f"[HTTP {code} ERROR] for {file_name}")
                return response

            return response
        
        except KeyboardInterrupt:
            self.logger.warning("\n[STOP] Interrupted by User (Ctrl+C)")
            raise SystemExit("[KEYBOARD INTERRUPT] Program stopped by user")
        
        except requests.exceptions.Timeout:
            self.logger.warning(f"\n[TIMEOUT ERROR] skipping {file_name}")
            response = requests.Response()
            response.status_code = 408
            return response
        
        except requests.exceptions.ConnectionError:
            self.logger.error("\n[CONNECTION ERROR] stopping program\n URL: {url}")
            raise SystemExit("[CONNECTION ERROR] Program stopped due to connection error")
        
        except SystemExit:
            raise

        except Exception as e:
            self.logger.error("\n[UNEXPECTED ERROR] stopping program\n URL: {url} \n Error: {e}")
            raise SystemExit(f"[UNEXPECTED ERROR] Program stopped due to {e}")

    def get_idx_link(self, year, quarter):
        """Download and parse daily index for a quarter"""
        url = f"{self.base_url}/{year}/QTR{quarter}/"
        try:
            response = self.safe_request(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            form_pattern = re.compile(r'^form\.\d+\.idx$')
        
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if form_pattern.match(href):
                    links.append(url + href)
                    
            
            return links
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download {year} Q{quarter}: {e}")
            return []

    def parse_daily_index_line(self, line):
        """Parse a line from daily index file"""
        try:
            pattern = r"^(?P<form_type>\S+)\s+" \
                    r"(?P<company_name>.+?)\s{2,}(?P<cik>\d+)\s+" \
                    r"(?P<date_filed>\d{8})\s+" \
                    r"(?P<file_name>\S+\.txt)$"

            match = re.match(pattern, line.strip())
            if not match:
                self.logger.info(f"x  Could not parse line: {line}")
                return None
            
            diction = match.groupdict()

            form_type = diction['form_type']
            company_name = diction['company_name']
            cik = diction['file_name'].split('/')[-2].zfill(10)  # Pad CIK to 10 digits
            date_filed = diction['date_filed']
            file_name = diction['file_name'].strip()
            if 'edgar' in file_name:
                url = f"https://www.sec.gov/Archives/{file_name}"
            else:
                url = f"https://www.sec.gov/Archives/edgar/{file_name}"
             
            if form_type in self.target_forms:
                return {
                    'company_name': company_name,
                    'form_type': form_type,
                    'cik': cik,
                    'date_filed': date_filed,
                    'file_name': url,
                    'accession_number': diction['file_name'].strip().split('/')[-1].split('.')[0]
                }
        except Exception:
            return None
    
    def pure_parse_daily_single_index(self,idx_url):
        try:
            response = self.safe_request(idx_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            lines = response.text.splitlines()
            
            filings = []
            for i, line in enumerate(lines):
                if set(line) == {'-'}:
                    data_start = i + 1
                    break
            
            for line in lines[data_start:]:
                filing = self.parse_daily_index_line(line)
                if filing and filing['form_type'] in self.target_forms:
                   filings.append(filing)      
            
            return filings

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to parse index {idx_url}: {e}")
            return []

    def pure_parse_daily_index(self, all_idx_url):
        stats = {
            'downloaded': 0,
            'skipped': 0,
            'failed': 0,
            'errors': 0
        }
        for idx_url in all_idx_url:
            self.logger.info(f"Processing index file: {idx_url.split('/')[-1]}")
            filings = self.pure_parse_daily_single_index(idx_url)
            self.logger.info(f"{idx_url.split('/')[-1].split(".")[1]} has {len(filings) }")
            
            for filing in filings:
                self.logger.info(f"Download {len(filing)} forms")
                url = filing['file_name']
                response = self.safe_request(url, timeout = 15)
                if response.status_code == 200:
                    self.logger.info(f"{filing['company_name']} has {filing['form_type']} filed in {filing['date_filed']}")
                    if self.database.is_filing_downloaded(filing['cik'], filing['accession_number']):
                        self.logger.info("[IN DB], skipped")
                        stats['skipped'] +=1
                        continue

                    result, target_dir, target_dir2 = self.downloader.download_filing(filing)
                    
                    if result and result.get('downloaded_files'):
                        stats['downloaded'] += 1
                        self.logger.info(f"Downloaded: {filing['company_name']} {filing['form_type']}")
                        if target_dir and target_dir2:
                            self.inspect_download(filing['cik'], filing['accession_number'], target_dir, target_dir2)
                    else:
                        stats['failed'] += 1
                        self.logger.warning(f"Failed: {filing['company_name']} {filing['form_type']} - no files downloaded")

        return stats

    def parse_daily_single_index(self, idx_url):
        """Parse daily index file and return list of filings"""
        try:
            response = self.safe_request(idx_url, timeout=30)
            response.raise_for_status()
            lines = response.text.splitlines()
            
            data_start = 0
            for i, line in enumerate(lines):
                if set(line) == {'-'}:
                    data_start = i + 1
                    break
            stats = {
                'total_found': 0,
                'downloaded': 0,
                'skipped': 0,
                'failed': 0,
                'errors': 0
            }

            for line in lines[data_start:]:
                filing = self.parse_daily_index_line(line)
                if not filing or filing['form_type'].replace(' ', '') not in self.target_forms:
                    continue
                cik = filing['cik']
                accession_number = filing['accession_number']

                if self.database.is_filing_downloaded(cik, accession_number):
                    self.database.delete_filing_record(cik, accession_number)
                
                record = self.downloader.get_company_recent_filings(cik, accession_number)
                if record and isinstance(record, dict):
                    if filing['form_type'] == record['form_type']:
                        filing['report_date'] = record.get('report_date', filing['date_filed'])
                        filing['primary_document'] = record.get('primary_document', 'UNKNOWN')
                        filing['filing_date'] = record.get('filing_date', filing['date_filed'])
                    
                        try:
                            result, filing_data = self.downloader.download_filing(filing)
                            if result and result.get('downloaded_files'):
                                stats['downloaded'] += 1
                                self.logger.info(f"Downloaded: {filing['company_name']} {filing['form_type']}")
                                self.inspect_download(cik, accession_number)
                            else:
                                stats['failed'] += 1
                                self.logger.warning(f"Failed: {filing['company_name']} {filing['form_type']} - no files downloaded")
                                
                        except Exception as e:
                            stats['errors'] += 1
                            self.logger.error(f"Error downloading {filing['company_name']} {filing['form_type']}: {e}")
                else:
                    # No API record - skip for now or handle differently
                    stats['skipped'] += 1
                    self.logger.info(f"Skipped (no API record): {filing['company_name']} {filing['form_type']}")
            
            return stats

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to parse index {idx_url}: {e}")
            return []
        
    def parse_daily_index(self, all_idx_url):
        aggregated_stats = {
            'total_found': 0,
            'downloaded': 0,
            'skipped': 0,
            'failed': 0,
            'errors': 0
        }
        for idx_url in all_idx_url:
            self.logger.info(f"Processing index file: {idx_url.split('/')[-1]}")

            file_stats = self.parse_daily_single_index(idx_url)
            for key in aggregated_stats:
                aggregated_stats[key] += file_stats[key]
            
            # Log progress for this file
            if file_stats['total_found'] > 0:
                self.logger.info(f"  File results: {file_stats['total_found']} found, "
                            f"{file_stats['downloaded']} downloaded, "
                            f"{file_stats['skipped']} skipped, "
                            f"{file_stats['failed']} failed, "
                            f"{file_stats['errors']} errors")
        
        return aggregated_stats
    
    def download_year_range(self, start_year, end_year):
        """Download all filings for a year range"""
        overall_stats = {
            'total_found': 0,
            'downloaded': 0,
            'skipped': 0,
            'failed': 0,
            'errors': 0
        }
        
        self.logger.info(f"Starting download for years {start_year}-{end_year}")
        
        for year in range(start_year, end_year + 1):
            self.logger.info(f"Processing year {year}")
            
            year_stats = {
                'total_found': 0,
                'downloaded': 0,
                'skipped': 0,
                'failed': 0,
                'errors': 0
            }
            
            for quarter in range(1, 5):
                self.logger.info(f"  Processing QTR{quarter}")
                
                # Skip early 1994 quarters
                if year == 1994 and quarter in [1, 2]:
                    continue
                if year == 2025 and quarter == 4:
                    continue
                # Get index links
                idx_links = self.get_idx_link(year, quarter)
                if not idx_links:
                    self.logger.warning(f"No index files found for {year} Q{quarter}")
                    continue
                    
                self.logger.info(f"  Found {len(idx_links)} index files")
                
                # Process all index files for this quarter
                
                quarter_stats = self.pure_parse_daily_index(idx_links)
                
                    # Log quarter results
                if quarter_stats['total_found'] > 0:
                    self.logger.info(f"  QTR{quarter} results: {quarter_stats['total_found']} found, "
                                    f"{quarter_stats['downloaded']} downloaded, "
                                    f"{quarter_stats['skipped']} skipped, "
                                    f"{quarter_stats['failed']} failed, "
                                    f"{quarter_stats['errors']} errors")
                else:
                    self.logger.warning(f"No filings found in index files for {year} Q{quarter}")
                    
                    # Aggregate to year stats
                for key in year_stats:
                    year_stats[key] += quarter_stats[key]
            
            # Log year results
            self.logger.info(f"Year {year} completed: {year_stats['total_found']} found, "
                            f"{year_stats['downloaded']} downloaded, "
                            f"{year_stats['skipped']} skipped, "
                            f"{year_stats['failed']} failed, "
                            f"{year_stats['errors']} errors")
            
            # Aggregate to overall stats
            for key in overall_stats:
                overall_stats[key] += year_stats[key]
        
        self.logger.info(f"Completed processing for years {start_year}-{end_year}")
        
        # Final summary
        self.logger.info("="*60)
        self.logger.info("DAILY INDEX DOWNLOAD COMPLETE")
        self.logger.info(f"Total filings found: {overall_stats['total_found']}")
        self.logger.info(f"Downloaded: {overall_stats['downloaded']}")
        self.logger.info(f"Skipped (in DB): {overall_stats['skipped']}")
        self.logger.info(f"Failed: {overall_stats['failed']}")
        self.logger.info(f"Errors: {overall_stats['errors']}")
        self.logger.info("="*60)
        
        return overall_stats

    def replace_image_paths(self, file_path, cik, accession_number):
        try:
            html = file_path.read_text(encoding='utf-8', errors='replace')
            html = clean_mojibake(html)
            soup = BeautifulSoup(html, 'html.parser')
            
            updated = False
            base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number.replace("-","")}"
            
            for img in soup.find_all("img"):
                src = img.get("src", "").strip()
                if ".jpg" in src.lower():
                    if not src.startswith(base_url):
                        filename = Path(src).name
                        img["src"] = f"{base_url}/{filename}"
                        updated = True
                        self.logger.info(f" Updated image src to: {img['src']}")
                    else:
                        self.logger.info(f" Image already has correct src: {src}")
                
            
            cleaned_html = decode_entities(soup)
            cleaned_html = clean_mojibake(cleaned_html)
            file_path.write_text(cleaned_html, encoding='utf-8')
            self.logger.info(f" Saved updated HTML: {file_path.name}")
            # Step 5: Warn if still problematic
            if any(bad in cleaned_html for bad in ["â€œ", "â€", "Ã", "Â"]):
                self.logger.warning(f"Mojibake artifacts may remain in {file_path.name}")
            else:
                self.logger.info(f"Cleaned and saved HTML: {file_path.name}")
        
        except Exception as e:
            self.logger.info(f"Error processing HTML {file_path.name}: {e}")

    def inspect_download(self, cik, accession_number, target_dir, target_dir2):  
        fiscal_year, fiscal_period, form_type, ticker = self.database.get_fiscal_info(cik, accession_number)
        if form_type in ['10-K', '10-Q', '10-K/A', '10-Q/A']:
            form_type = form_type.replace('/A', '_A')
        
        src_dir = target_dir
        target_dir = target_dir2
        config = pdfkit.configuration(wkhtmltopdf='C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe')
        pdf_options = {
            'encoding': 'UTF-8',
            'no-images': None,  # Disable image loading
            'disable-javascript': None,
            'disable-external-links': None,
            'load-error-handling': 'ignore',
            'load-media-error-handling': 'ignore',
            'quiet': None
        }
        
        for file_path in src_dir.iterdir():
            try:
                if file_path.suffix.lower() == '.html':
                    self.replace_image_paths(file_path, cik, accession_number)
                    filename = file_path.stem
                    output_pdf = target_dir / f"{filename}.pdf"
                    pdfkit.from_file(str(file_path), str(output_pdf), configuration=config)
                    self.logger.info(f" Converted HTML to PDF: {output_pdf}")
                
                elif file_path.suffix.lower() == '.txt':
                    shutil.copy(file_path, target_dir / file_path.name)
                    self.logger.info(f" Copied TXT file: {file_path.name} to {target_dir}")
        
                
                elif file_path.suffix.lower() == '.xlsx':
                    shutil.copy(file_path, target_dir / file_path.name)
                    self.logger.info(f" Copied XLSX file: {file_path.name} to {target_dir}")
                    
            except Exception as e:
                self.logger.error(f"x  Error processing file {file_path.name}: {e}")

    
    
if __name__ == "__main__":
    downloader = SECDailyIndexDownloader()
    downloader.download_year_range(2025, 2025)
    