from utils.config import BASE_DATA_DIR
from utils.sec_database import SECDatabase
from utils.rate_limit import SafeRateLimiter
import requests
import logging
from datetime import datetime
import sys
from pathlib import Path
import re
from typing import Dict, List
from utils.txt_processing import to_process_normal_html, to_process_xbrl, extract_documents_html, extract_documents_xbrl
from bs4 import BeautifulSoup
import time 
from utils.index_parser import extract_sec_filing_data
import pandas as pd

class EDGARDownloader:
    def __init__(self, base_dir = BASE_DATA_DIR, tart_dir = Path('sec-data'), rate_limit = 0.15):
        """ Initialize SEC EDGAR Downloader with SQLite Download detection"""
        self.base_dir = Path(base_dir)
        self.rate_limit = rate_limit
        self.rate_limiter = SafeRateLimiter()
        self.database = SECDatabase()
        self.target_dir = tart_dir
        self.headers = {
            'User-Agent': 'sdadsCEWIT  reseadsadsarch@sbu.edu',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': 'application/json, text/html, */*'
        }
        self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.company_tickers = {}
        self.load_company_mapping()
        
    def setup_logging(self):
        '''Setup logging configuration'''
        log_dir = self.base_dir/ "logs"
        log_dir.mkdir(parents=True,exist_ok=True)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[
                                logging.FileHandler(log_dir/f"edgar_download_{datetime.now().strftime('%Y%m%d')}.log"),
                                logging.StreamHandler(sys.stdout)
                            ])
        self.logger = logging.getLogger(__name__)

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

    def load_company_mapping(self, mapping_path='cik_ticker.csv'):
        
        self.logger.info(f"Loading company mappings from {mapping_path}")
        try:
            df = pd.read_csv(mapping_path, dtype=str)

            count = 0
            for _, row in df.iterrows():
                cik = row['cik'].zfill(10)
                ticker = str(row.get('ticker', '')).upper()


                self.company_tickers[cik] = ticker
                count += 1

            self.logger.info(f"Loaded {count} companies from {mapping_path}")

        except FileNotFoundError:
            self.logger.error(f"Company CSV file not found: {mapping_path}")
        except Exception as e:
            self.logger.error(f"Failed to load company mappings: {e}")
        
    def create_folder_path(self, cik: str, year: str, form_type: str, company_name:str) -> Path:
        '''Create folder structure cik/year/form_type'''
        form_type = re.sub(r'\s+', ' ', form_type).upper()
        if form_type in ['10-K', '10-K/A']:
            form_type = '10-K_A' if '/A' in form_type else '10-K'
        elif form_type in ['10-Q', '10-Q/A']:
            form_type = '10-Q_A' if '/A' in form_type else '10-Q'
        else:
            form_type = form_type.replace('/', '_').replace('\\', '_')

        path = [self.base_dir, cik]
        target_dir = Path(*path)
       

        path2 = [self.target_dir, cik]
        target_dir2 = Path(*path2)
        
        return target_dir, target_dir2

    def create_filename(self, fiscal_info: Dict, report_date, filing_date, cik:str, company_name: str) -> Dict:
        form = fiscal_info.get('form_type')
        clean_form = form.replace('/', '_').replace(' ', '') if form else 'UNKNOWN'
        company_name = company_name.replace("/", "_").replace('\\','_').strip()
        if form in ['10-Q', '10-Q/A']:
            # For 10-Q, use fiscal year and quarter in filename
            if fiscal_info.get('fiscal_year') and fiscal_info.get('fiscal_period'):
                clean_date = f"{fiscal_info['fiscal_year']}_{clean_form}_{fiscal_info['fiscal_period']}"
                clean_date = clean_date.replace('/', '_')
            else:
                filename_date = report_date if report_date and report_date != 'None' and report_date is not None else filing_date
                clean_date = filename_date.replace('-', '')
                clean_date = clean_date.replace('/', '_')
        elif form in ['10-K', '10-K/A']:
            # For 10-K, use fiscal year in filename
            if fiscal_info.get('fiscal_year'):
                clean_date = fiscal_info.get('fiscal_year').replace('-', '')
                clean_date = clean_date.replace('/', '_')
                clean_date = f"{clean_date}_{clean_form}"
            else:
                filename_date = report_date if report_date and report_date != 'None' and report_date is not None else filing_date
                filename_date = filename_date.replace('-', '')
                clean_fiscal_year = self.estimate_10K_fiscal_year(filename_date)
                clean_date = clean_fiscal_year.replace('/', '_')
        else:
            # Fallback to date-based filename
            filename_date = report_date if report_date and report_date != 'None' and report_date is not None else filing_date
            clean_date = filename_date.replace('-', '')
            clean_date = clean_date.replace('/', '_')
        
        return f"{clean_date}_{company_name}"

    def required_urls(self, filename: str) -> Dict[str, str]:
        cik = filename.split('/')[-2].zfill(10)
        accession_number = filename.split('/')[-1].split(".")[0]
        urls = {'financial_report_xlsx': f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number.replace('-', '')}/Financial_Report.xlsx"}
        index_url = filename.replace('.txt', '-index.htm')

        try:
            response = self.safe_request(index_url, timeout=30)
            if response.status_code != 200:
                return None
            response.raise_for_status()
            content = response.text
            result = extract_sec_filing_data(content)
            doc_files = result.get("documentFormatFiles", {})
            data_files = result.get("dataFiles", {})
            
            # Always ensure we have complete_txt URL
            complete_txt_found = False
            
            if doc_files:
                # Check for complete submission text file first
                if "Complete submission text file" in doc_files:
                    urls['complete_txt'] = f"https://www.sec.gov{doc_files['Complete submission text file']['doc']}"
                    complete_txt_found = True
                
                # Process document format files
                for key, value in doc_files.items():
                    if key == "Complete submission text file":
                        if not complete_txt_found:
                            urls['complete_txt'] = f"https://www.sec.gov{value['doc']}"
                            complete_txt_found = True
                    elif key.isdigit() and value['type'].isdigit():
                        url_key = value['doc'].split('/')[-1].split(".")[0]
                        urls[url_key] = f"https://www.sec.gov{value['doc']}"
                    elif value['doc'].endswith(('.htm', '.html')):
                        # Use the document type as the key, fallback to description
                        url_key = value.get('type', key)
                        if "nbsp" not in url_key:
                            urls[url_key] = f"https://www.sec.gov{value['doc']}"
                        else:
                            urls[key] = f"https://www.sec.gov{value['doc']}"
            
            # Process data files (XBRL, etc.)
            if data_files:
                for key, value in data_files.items():
                    url_key = value.get('type', key)
                    if "nbsp" not in url_key:
                        if key == "EXTRACTED XBRL INSTANCE DOCUMENT":
                            urls['xbrl_instance'] = f"https://www.sec.gov{value['doc']}"
                        elif key.isdigit() and value['type'].isdigit():
                            url_keys = value['doc'].split('/')[-1].split(".")[0]
                            urls[url_keys] = f"https://www.sec.gov{value['doc']}"
                        else:
                            urls[f"xbrl_{url_key}"] = f"https://www.sec.gov{value['doc']}"
                    else:
                        urls[f"xbrl_{key}"] = f"https://www.sec.gov{value['doc']}"
            
            # Fallback: if no complete_txt found, construct it from the original filename
            if not complete_txt_found:
                urls['complete_txt'] = filename
                
        except Exception as e:
            self.logger.error(f"Error parsing index {index_url}: {e}")
            # Fallback to just the original file
            urls['complete_txt'] = filename

        return urls

    def get_tikcer_by_cik(self, cik:str):
        cik = cik.strip()
        if cik in self.company_tickers and self.company_tickers[cik]:
            return self.company_tickers[cik]
        return 'UNKNOWN'
    
    def get_cik_by_ticker(self, ticker: str):
        if ticker in self.company_tickers.values():
            for key, value in self.company_tickers.items():
                if value == ticker:
                    return key

    def get_company_recent_filings(self, cik: str, form_type: str) -> Dict:
        """Get recent filings for a company using SEC submissions API """
        form_types = ['10-K', '10-Q', '10-K/A', '10-Q/A']
        
        try:
            # self.logger.info(f"Getting filings for CIK {cik} accession_number {accession_number}")
            
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            response = self.safe_request(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            filings = data['filings']['recent']
            
            # Process filings
            recent_filings = []
            for i, form in enumerate(filings['form']):
                ac = filings['accessionNumber'][i]
                form_type = filings['form'][i]
                
                self.logger.info(f"{data.get('name', 'Unknown')}  {form_type} filing is in the record")
                recent_filings.append( {
                        'cik': cik,
                        'form_type': form_type,
                        'filing_date': filings['filingDate'][i],
                        'accession_number': filings['accessionNumber'][i],
                        'report_date': filings.get('reportDate', [None] * len(filings['form']))[i],
                        'company_name': data.get('name', 'Unknown'),
                        'primary_document': filings['primaryDocument'][i]
                    })
            
            return recent_filings
        except Exception as e:
            self.logger.error(f"Error getting filings for CIK {cik}: {e}")
            return []
    
    def estimate_10K_fiscal_year(self, date_filed: str) -> str:
        filling_date = datetime.strptime(date_filed, "%Y%m%d")
        if filling_date.month <= 5:
            return filling_date.year - 1
        else:
            return filling_date.year
    
    def determine_QTR_from_date(self, date_str: str) -> str:
        try:
            if not date_str or date_str == 'None' or date_str is None:
                return "Q1"
            
            date_str = date_str.replace('\n', '').replace('\t', '').strip()
            try:
                date_object = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                try:
                    date_object = datetime.strptime(date_str, '%b %d, %Y')
                except ValueError:
                    self.logger.error(f"[TIME PARSING ERROR] Could not parse date: {date_str}")
                    return "Q1" 
            
            month = date_object.month
            day = date_object.day
            if month == 3 and day >= 28:  # Late March = Q1 end
                return "Q1"
            elif month == 6 and day >= 28:  # Late June = Q2 end
                return "Q2"
            elif month == 9 and day >= 28:  # Late September = Q3 end
                return "Q3"
            elif month == 12 and day >= 28:  # Late December = Q4 end
                return "Q4"
            else:
                if month <= 4:
                    return "Q1"  # Jan-Mar 
                elif month <= 7:
                    return "Q2"  # Apr-Jun  
                elif month <= 10:
                    return "Q3"  # Jul-Sep
                else:
                    return "Q4"  # Oct-Dec
                    
        except Exception as e:
            self.logger.error(f"[DATE PARSING ERROR] Failed to parse date {date_str}: {e}")
            return "Q1" 

    def extract_fiscal_info_from_txt(self, complete_txt_url: str) -> str:
        fiscal_info = {
            "period_end_date": None,
            "fiscal_year_end": None,
            "fiscal_year": None,
            "fiscal_period": None,
            "form_type": None,
            "company_name": None
        }
        try:
            response = self.safe_request(complete_txt_url, timeout = 30)
            response.raise_for_status()
            content = response.text

            txt_period_of_report = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d{8})', content, re.IGNORECASE)
            txt_fiscal_period = re.search(r'>Document Fiscal Period Focus</a></td>\s*<td class="text">\s*(Q[1-4])', content, re.IGNORECASE)
            txt_fiscal_year = re.search(r'>Document Fiscal Year Focus</a></td>\s*<td class="text">\s*(\d{4})', content, re.IGNORECASE)
            txt_submission_type = re.search(r'CONFORMED SUBMISSION TYPE:\s*(\d+-[A-Za-z]+(?:/[A-Za-z])?)', content, re.IGNORECASE)
            txt_company_name = re.search(r'COMPANY CONFORMED NAME:\s*(.+)', content, re.IGNORECASE)
            txt_date_filed = re.search(r'FILED AS OF DATE:\s*(\d{8})', content, re.IGNORECASE)
            if txt_submission_type:
                fiscal_info['form_type'] = txt_submission_type.group(1).upper()
            
            if txt_fiscal_year:
                fiscal_info['fiscal_year'] = txt_fiscal_year.group(1)
            
            if txt_period_of_report:
                raw_date = txt_period_of_report.group(1)
                period_end_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
                fiscal_info['period_end_date'] = period_end_date
                fiscal_info['period_end_year'] = f"{raw_date[:4]}"

                if not fiscal_info['fiscal_year']:
                    fiscal_info['fiscal_year'] = raw_date[:4]
                
                fiscal_info['fiscal_year_end'] = f"{raw_date[4:6]}-{raw_date[6:]}"
            
            if txt_date_filed:
                raw_date = txt_date_filed.group(1)
                fiscal_info['date_filed_year'] = f"{raw_date[:4]}"
                fiscal_info['date_filed'] = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            if txt_company_name:
                fiscal_info['company_name'] = txt_company_name.group(1).strip()

            if fiscal_info['form_type'] in ['10-Q', '10-Q/A']:
                if txt_fiscal_period:
                    fiscal_info['fiscal_period'] = txt_fiscal_period.group(1)
                elif fiscal_info['period_end_date']:
                    fiscal_info['fiscal_period'] = self.determine_QTR_from_date(fiscal_info['period_end_date'])
            elif fiscal_info['form_type'] in ['10-K', '10-K/A']:
                fiscal_info['fiscal_period'] = 'FY'


        except Exception as e:
            self.logger.debug(f"[TXT EXTRACTION ERROR] failed to extract fiscal info from txt due to {e}")
        
        return fiscal_info

    def download_filing(self, filling: Dict) -> Dict:
        '''Download required files'''
        cik = filling['cik']
        company_name = filling['company_name']
        form_type = filling['form_type']
        file_name = filling['file_name']
        accession_number = filling['accession_number']
        if self.database.is_filing_downloaded(cik, accession_number):
            return {
                'filing_record': filling,
                'downloaded_files': [],
                'failed_files': [],
                'skipped': True
            }
        
        ticker = self.get_tikcer_by_cik(cik)

        urls = self.required_urls(file_name)
        if not urls:
            return None
        fiscal_info = self.extract_fiscal_info_from_txt(urls['complete_txt'])
        company_name = fiscal_info['company_name'] if fiscal_info['company_name'] else company_name

        fiscal_year = fiscal_info['fiscal_year']
        fiscal_period = fiscal_info['fiscal_period']

        file_name = self.create_filename(fiscal_info, fiscal_info['period_end_year'], fiscal_info['date_filed_year'],cik,company_name)
        target_dir, target_dir2 = self.create_folder_path(cik, fiscal_year, form_type, company_name)
        
        results = {
            'filing_record': filling,
            'downloaded_files': [],
            'failed_files': [],
            'skipped': False
        }
        has_xbrl_format = 0
        if any(k in urls for k in ['10-K', '10-Q', '10-K/A', '10-Q/A']):
            try:
                for key, url in urls.items():
                    if key == form_type:
                        html_path = target_dir2/f"{file_name}_main.{url.split('.')[-1]}"
                    elif 'xbrl' in key:
                        html_path = target_dir/f"{file_name}_{key.replace("/", "_")}.{url.split('.')[-1]}"
                        has_xbrl_format = 1
                    elif 'EX' in key.upper():
                        html_path = target_dir2/f"{file_name}_{key.replace("/", "_")}.{url.split('.')[-1]}"
                    else:
                        continue

                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_resp = self.safe_request(url, timeout=30)
                    if html_resp.status_code == 200:
                        html_content = html_resp.text
                        updated_html = re.sub(
                            r'<img[^>]*src=["\']([^"\']*\.(jpg|png|gif))["\']',
                            lambda m: m.group(0).replace(m.group(1), f"{Path(url).parent}/{Path(m.group(1)).name}"),
                            html_content,
                            flags=re.IGNORECASE
                        )
                        
                        with open(html_path, 'wb') as f:
                            f.write(updated_html.encode('utf-8'))
                        results['downloaded_files'].append(str(html_path))
                        self.logger.info(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)} Saved {key} to {html_path}]")
                    
            except Exception as e:
                self.logger.error(f"[UNEXPECTED ERROR] DUE to {e}")
                results['failed_files'].append('html')
        
        else:
            try:
                txt_path = target_dir2/f"{file_name}_full_submission.txt"
                txt_resp = self.safe_request(urls['complete_txt'], timeout=30)
                txt_path.parent.mkdir(parents=True, exist_ok=True)
                if txt_resp.status_code == 200:
                    with open(txt_path, 'wb') as f:
                        f.write(txt_resp.content)
                    results['downloaded_files'].append(str(txt_path))
                    self.logger.info(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)} Saved full submission txt to {txt_path}]")
                    
            except Exception as e:
                self.logger.warning(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)}] Failed to download .txt: {e}")
                results['failed_files'].append('txt')


        # Download .xlsx file
        
        try:
            xlsx_path = target_dir/f"{file_name}_financial_report.xlsx"
            xlsx_resp = self.safe_request(urls['financial_report_xlsx'], timeout=30)
            
            xlsx_path.parent.mkdir(parents=True, exist_ok=True)
            if xlsx_resp.status_code == 200:
                with open(xlsx_path, 'wb') as f:
                    f.write(xlsx_resp.content)
                results['downloaded_files'].append(str(xlsx_path))
                self.logger.info(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)} Saved financial report xlsx to {xlsx_path}]")
                    
        except Exception as e:
            self.logger.warning(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)}] Failed to download .xlsx: {e}")
            results['failed_files'].append('xlsx')

        if results['downloaded_files']:
            filing_data = {
                'filing_id': f"{cik}_{accession_number}",
                'cik': cik,
                'accession_number': accession_number,
                'form_type': form_type,
                'company_name': company_name,
                'ticker': ticker,
                'fiscal_year': fiscal_info.get('fiscal_year'),
                'fiscal_period': fiscal_info.get('fiscal_period'),
                'filing_date': fiscal_info.get('date_filed', filling['date_filed']),
                'period_end_date': fiscal_info.get('period_end_date', filling['date_filed']),
                'file_path': str(target_dir2),
                'file_count': len(results['downloaded_files']),
                'total_size': sum(Path(f).stat().st_size for f in target_dir2.iterdir()),
                'download_status': 'completed',
                'has_xbrl_format': has_xbrl_format,
                'path': file_name
            }
            self.database.add_filing(filing_data)
        
        
        return results
    
    
    def download_filings(self, cik, store_dir, filling_record) -> Dict:
        '''Download required files'''
        
        ticker = self.get_tikcer_by_cik(cik)
       
        date_filed = filling_record['filing_date']
        report_date = filling_record['report_date']
        accession_number = filling_record['accession_number']
        company_name = filling_record['company_name']
        form_type = filling_record['form_type']
        urls = self.required_urls(f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}.txt")
        if not urls:
            return None
        fiscal_info = self.extract_fiscal_info_from_txt(urls['complete_txt'])
        company_name = fiscal_info['company_name'] if fiscal_info['company_name'] else company_name

        fiscal_year = fiscal_info['fiscal_year']
        fiscal_period = fiscal_info['fiscal_period']

        file_name = self.create_filename(fiscal_info, fiscal_info['period_end_year'], fiscal_info['date_filed_year'],cik,company_name)
        target_dir, target_dir2 = store_dir, store_dir
        
    
        if True:
            try:
                for key, url in urls.items():
                    if key == form_type:
                        html_path = target_dir2/fiscal_info['fiscal_year']/f"{file_name}_main.{url.split('.')[-1]}"
                    else:
                        continue

                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_resp = self.safe_request(url, timeout=30)
                    if html_resp.status_code == 200:
                        html_content = html_resp.text
                        updated_html = re.sub(
                            r'<img[^>]*src=["\']([^"\']*\.(jpg|png|gif))["\']',
                            lambda m: m.group(0).replace(m.group(1), f"{Path(url).parent}/{Path(m.group(1)).name}"),
                            html_content,
                            flags=re.IGNORECASE
                        )
                        
                        with open(html_path, 'wb') as f:
                            f.write(updated_html.encode('utf-8'))
                        
                        self.logger.info(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)} Saved {key} to {html_path}]")
                    
            except Exception as e:
                self.logger.error(f"[UNEXPECTED ERROR] DUE to {e}")
            
                    
            except Exception as e:
                self.logger.warning(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)}] Failed to download .txt: {e}")


        # Download .xlsx file
        
        try:
            xlsx_path = target_dir/fiscal_info['fiscal_year']/f"{file_name}_financial_report.xlsx"
            xlsx_resp = self.safe_request(urls['financial_report_xlsx'], timeout=30)
            
            xlsx_path.parent.mkdir(parents=True, exist_ok=True)
            if xlsx_resp.status_code == 200:
                with open(xlsx_path, 'wb') as f:
                    f.write(xlsx_resp.content)
                
                self.logger.info(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)} Saved financial report xlsx to {xlsx_path}]")
                    
        except Exception as e:
            self.logger.warning(f"[{ticker if ticker != 'UNKNOWN' else (company_name if company_name != 'UNKNOWN' else cik)}] Failed to download .xlsx: {e}")
           


if __name__ == "__main__":
    downloader = EDGARDownloader()
    cik = downloader.get_cik_by_ticker('APLD')
    filings = downloader.get_company_recent_filings('0000001961', '10-K')
    print(filings)
    downloader.download_filings(cik, 'test_data', filings)

