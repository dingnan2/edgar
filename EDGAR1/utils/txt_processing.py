import re
from bs4 import BeautifulSoup
import os
from utils.sec_database import SECDatabase
from pathlib import Path
from utils.config import BASE_DATA_DIR

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import json
from datetime import datetime
import unicodedata


def setup_logging():
    """Setup comprehensive logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('edgar_processing.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


db = SECDatabase()

def clean_mojibake(text):
    replacements = [
        ("Â", " "),
        ("â€¢", "•"),
        ("â€“", "–"),
        ("â€”", "—"),
        ("â€œ", "“"),
        ("â€�", "”"),
        ("â€˜", "‘"),
        ("â€™", "’"),
        ("â€¦", "…"),
        ("Ã©", "é"),
        ("Ã", "à"),  # very rare, but seen in some reports
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    return text

def decode_entities(soup):
    html = soup.prettify(formatter="html")
    html = html.replace("&#x2611;", "☑").replace("&#x2610;", "☐")  # checkbox symbols
    html = html.replace("&bull;", "•").replace("&#8226;", "•")
    html = html.replace("&ldquo;", "“").replace("&rdquo;", "”")
    html = html.replace("&lsquo;", "‘").replace("&rsquo;", "’")
    return html


def get_accession_number(cik, year, quarter):
    if quarter in ['10-K', 'FY', 'Q4']:
        # Look for 10-K or full year filings
        query = """
            SELECT accession_number, form_type, fiscal_period, filing_date
            FROM filings
            WHERE cik = ?
              AND fiscal_year = ?
              AND (form_type = '10-K' OR UPPER(fiscal_period) IN ('FY', 'Q4'))
            ORDER BY filing_date DESC
            LIMIT 1
        """
        params = (cik, str(year))
    elif quarter in ['Q1', 'Q2', 'Q3']:
        # Look for quarterly filings (10-Q)
        query = """
            SELECT accession_number, form_type, fiscal_period, filing_date
            FROM filings
            WHERE cik = ?
              AND fiscal_year = ?
              AND UPPER(fiscal_period) = ?
              AND form_type = '10-Q'
            ORDER BY filing_date DESC
            LIMIT 1
        """
        params = (cik, str(year), quarter)
        
    else:
        # Generic search by fiscal_period
        query = """
            SELECT accession_number, form_type, fiscal_period, filing_date
            FROM filings
            WHERE cik = ?
              AND fiscal_year = ?
              AND UPPER(fiscal_period) = ?
            ORDER BY filing_date DESC
            LIMIT 1
        """
        params = (cik, str(year), quarter)
    
    with db.get_optimized_connection() as conn:
        cursor = conn.execute(query, params)
        result = cursor.fetchone()
        if result:
            accession_number = result[0]
            form_type = result[1]
            fiscal_period = result[2]
            filing_date = result[3]
            accession_clean = accession_number.replace('-', '')
            return accession_clean

def to_process_normal_html(content: str) -> bool:
    content_upper = content.upper()
    # Must contain HTML structure
    has_html = ("<HTML>" in content_upper and 
                "<HEAD>" in content_upper and 
                "<BODY>" in content_upper)
    
    # Must NOT be XBRL
    is_xbrl = "<DESCRIPTION>XBRL TAXONOMY EXTENSION SCHEMA" in content_upper
    
    # Must have SEC document structure
    has_sec_structure = ("<SEC-DOCUMENT>" in content_upper and 
                        "<DOCUMENT>" in content_upper and
                        "ACCESSION NUMBER:" in content_upper)
    
    return has_html and not is_xbrl and has_sec_structure

def to_process_xbrl(content: str) -> bool:
    """ Check if the content is an XBRL taxonomy extension schema """
    return "<DESCRIPTION>XBRL TAXONOMY EXTENSION SCHEMA" in content.upper()

def extract_documents_xbrl(file_path: Path):
    """
    SAFE VERSION: Only deletes original file if HTML files were successfully created
    """
    cik = file_path.parts[-4]  # CIK is the 4th last part
    year = file_path.parts[-3]
    form_type = file_path.parts[-2]
    
    print(f"Processing: CIK={cik}, Year={year}, Form={form_type} for XBRL")
    prefix = file_path.name.replace("_complete.txt", "")
    period = prefix.split('_')[-1].upper() 
    if period not in ['Q1', 'Q2', 'Q3', 'Q4']:
        if period.isdigit() and 1 <= int(period) <= 4:
            period = '10-K'
        else:
            period = 'FY'

    print(f"cik: {cik}, year: {year}, form type: {form_type}, period: {period}")

    try:
        content = file_path.read_text(encoding='utf-8', errors='replace')
    except Exception as e:
        print(f"   Failed to read file {file_path.name}: {e}")
        return
    
    if not to_process_xbrl(content):
        print(f"   Skipping {file_path.name} - not an XBRL taxonomy extension schema")
        return False

    documents = re.findall(r'<DOCUMENT>(.*?)</DOCUMENT>', content, re.DOTALL | re.IGNORECASE)
    if not documents:
        print(f"  No <DOCUMENT> tags found in {file_path.name}")
        return
    
    print(f" Found {len(documents)} document(s) in {file_path.name}")
    
    saved_files = []  # Track successfully saved files
    
    for doc_idx, doc in enumerate(documents, 1):
        seq_match = re.search(r'<SEQUENCE>\s*(\d+)', doc, re.IGNORECASE)
        if not seq_match:
            print(f"    Doc {doc_idx}: No SEQUENCE tag found")
            continue
            
        seq_num = int(seq_match.group(1))
       
        # Check for XSD files (stop processing)
        filename_match = re.search(r'<FILENAME>\s*(.*?)\s*$', doc, re.IGNORECASE | re.MULTILINE)
        type_name = re.search(r'<TYPE>\s*(.*?)\s*$', doc, re.IGNORECASE | re.MULTILINE)
        if filename_match:
            filename = filename_match.group(1).lower()
            type_name = type_name.group(1).lower()
            
            if not (filename.endswith('.htm') or filename.endswith('.html')) or type_name.lower() == 'xml' :
                print(f" Skipping file does not end with .htm or .html: {filename} — stopping processing")
                continue
            if filename.lower().endswith('.xsd') or filename.lower().endswith('.xml') or filename.lower().endswith('.xbrl') or filename.lower().endswith('.jpg') or filename.lower().endswith('.png') or filename.lower().endswith('.gif') :
                print(f"     Skipping Invalid file: {filename} in sequence {seq_num}")
                continue
            

        # Look for HTML content
        html_match = re.search(r'(<html.*?</html>)', doc, re.DOTALL | re.IGNORECASE)
        if not html_match:
            print(f"     No HTML content found in sequence {seq_num}")
            continue

        print(f"    Processing HTML content in sequence {seq_num}")

        try:
            html_content = clean_mojibake(html_match.group(1))
            soup = BeautifulSoup(html_content, 'html.parser')

            if not soup.head:
                head = soup.new_tag("head")
                soup.html.insert(0, head)
            soup.head.insert(0, soup.new_tag("meta", charset="utf-8"))

            # Determine output filename
            if seq_num == 1:
                accession_number = get_accession_number(cik, year, period)
                if accession_number:
                    print(f"   🔗 Found accession: {accession_number}")
                    # Update image sources
                    img_count = 0
                    for img in soup.find_all("img"):
                        src = img.get("src", "").strip()
                        if ".jpg" in src.lower():
                            filename = Path(src).name
                            img["src"] = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{filename}"
                            img_count += 1
                    if img_count > 0:
                        print(f"Updated {img_count} image source(s)")
                else:
                    print(f" No accession number found for CIK={cik}, Year={year}, Period={period}")
                    
                output_name = f"{prefix}.html"
            else:
                type_match = re.search(r'<TYPE>\s*([\w\-\.]+)', doc, re.IGNORECASE)
                output_name = f"{prefix}_{type_match.group(1)}.html" if type_match else f"{prefix}_sequence_{seq_num}.html"

            # Save the file
            output_path = file_path.parent / output_name
            output_path.write_text(decode_entities(soup), encoding='utf-8')
            saved_files.append(output_path)
            print(f"   [✓] Saved: {output_path.name}")
            
        except Exception as e:
            print(f"      Error processing sequence {seq_num}: {e}")
            continue

    # Summary and cleanup
    if saved_files:
        print(f" Successfully saved {len(saved_files)} file(s) from {file_path.name}")
        try:
            file_path.unlink()  # Only delete if we saved something
            print(f"[✗] Deleted original: {file_path.name}")
        except Exception as e:
            print(f"  Could not delete original file: {e}")
    else:
        print(f"  No HTML files generated from {file_path.name} - keeping original file")
        print(f" Reasons: No HTML content found, or all sequences were outside range 1-8")

def extract_documents_html(file_path: Path):
    """
    Extract HTML documents from EDGAR filing .txt that are not XBRL.
    Similar to extract_documents_xbrl but skips XBRL taxonomy documents.
    """
    cik = file_path.parts[-4]  # CIK is assumed to be in 3rd index
    year = file_path.parts[-3]
    form_type = file_path.parts[-2]
    print(f"🔍 Processing: CIK={cik}, Year={year}, Form={form_type} for normal html")
    
    print(f"Processing HTML: CIK={cik}, Year={year}, Form={form_type}")
    prefix = file_path.name.replace("_complete.txt", "")
    period = prefix.split('_')[-1].upper()
    if period not in ['Q1', 'Q2', 'Q3', 'Q4']:
        period = '10-K' if period.isdigit() and 1 <= int(period) <= 4 else 'FY'

    try:
        content = file_path.read_text(encoding='utf-8', errors='replace')
    except Exception as e:
        print(f"   Failed to read file {file_path.name}: {e}")
        return

    if not to_process_normal_html(content):
        print(f" Skipping {file_path.name} - not a normal HTML document")
        return False

    documents = re.findall(r'<DOCUMENT>(.*?)</DOCUMENT>', content, re.DOTALL | re.IGNORECASE)
    if not documents:
        print(f" No <DOCUMENT> tags found in {file_path.name}")
        return

    print(f" Found {len(documents)} document(s) in {file_path.name}")
    saved_files = []

    for doc_idx, doc in enumerate(documents, 1):
        seq_match = re.search(r'<SEQUENCE>\s*(\d+)', doc, re.IGNORECASE)
        if not seq_match:
            print(f"    Doc {doc_idx}: No SEQUENCE tag found")
            continue
            
        seq_num = int(seq_match.group(1))
        filename_match = re.search(r'<FILENAME>\s*(.*?)\s*$', doc, re.IGNORECASE | re.MULTILINE)
        type_name = re.search(r'<TYPE>\s*(.*?)\s*$', doc, re.IGNORECASE | re.MULTILINE)
        if filename_match:
            filename = filename_match.group(1).lower()
            type_name = type_name.group(1).lower()
            
            if not (filename.endswith('.htm') or filename.endswith('.html')) or type_name.lower() == 'xml' :
                print(f" Skipping file does not end with .htm or .html: {filename} — stopping processing")
                continue
            if filename.lower().endswith('.xsd') or filename.lower().endswith('.xml') or filename.lower().endswith('.xbrl') or filename.lower().endswith('.jpg') or filename.lower().endswith('.png') or filename.lower().endswith('.gif') :
                print(f"     Skipping Invalid file: {filename} in sequence {seq_num}")
                continue
            

        # Look for HTML content
        html_match = re.search(r'(<html.*?</html>)', doc, re.DOTALL | re.IGNORECASE)
        if not html_match:
            print(f"     No HTML content found in sequence {seq_num}")
            continue

        print(f"    Processing HTML content in sequence {seq_num}")

        try:
            html_content = clean_mojibake(html_match.group(1))
            soup = BeautifulSoup(html_content, 'html.parser')

            if not soup.head:
                head = soup.new_tag("head")
                soup.html.insert(0, head)
            soup.head.insert(0, soup.new_tag("meta", charset="utf-8"))

            if seq_num == 1:
                accession_number = get_accession_number(cik, year, period)
                if accession_number:
                    for img in soup.find_all("img"):
                        src = img.get("src", "").strip()
                        if ".jpg" in src.lower():
                            filename = Path(src).name
                            img["src"] = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{filename}"

                output_name = f"{prefix}.html"
            else:
                type_match = re.search(r'<TYPE>\s*([\w\-\.]+)', doc, re.IGNORECASE)
                output_name = f"{prefix}_{type_match.group(1)}.html" if type_match else f"{prefix}_sequence_{seq_num}.html"

            output_path = file_path.parent / output_name
            output_path.write_text(decode_entities(soup), encoding='utf-8')
            saved_files.append(output_path)
            print(f" Saved: {output_path.name}")

        except Exception as e:
            print(f"   Error processing sequence {seq_num}: {e}")
            continue

    if saved_files:
        print(f" Successfully saved {len(saved_files)} file(s) from {file_path.name}")
        try:
            file_path.unlink()
            print(f"[✗] Deleted original: {file_path.name}")
            return True
        except Exception as e:
            print(f"Could not delete original file: {e}")
    else:
        print(f"No valid HTML files generated from {file_path.name}")
        return False


def batch_process_edgar():
    """
    Improved batch processing with progress tracking and statistics
    """
    import time
    from collections import defaultdict

    start_time = time.time()
    all_txt = list(Path("sec-data").glob("**/*_complete.txt"))
    total_files = len(all_txt)
    ff = all_txt[0] if total_files > 0 else None
    print(f"file_path parts: {ff.parts}")
    print(f"Processing file: {ff.name} ")
    print(f"🔍 Found {total_files:,} files to process...")
    
    if total_files == 0:
        print("   No files found!")
        return
    
    # Statistics tracking
    stats = {
        'processed': 0,
        'failed': 0,
        'errors': defaultdict(int),
        'companies': set()
    }
    
    for i, file_path in enumerate(all_txt, 1):
        try:
            # Extract company info for tracking
            cik = file_path.parts[2] 
            stats['companies'].add(cik)
            
            # Progress indicator
            progress = (i / total_files) * 100
            print(f"\n[{i:,}/{total_files:,}] ({progress:.1f}%) Processing: {file_path.name}")
            
            content = file_path.read_text(encoding='utf-8', errors='replace')
            if to_process_xbrl(content):
                extract_documents_xbrl(file_path)
            elif to_process_normal_html(content):
                extract_documents_html(file_path)

            stats['processed'] += 1
            
            # Show ETA every 50 files
            if i % 50 == 0:
                elapsed = time.time() - start_time
                eta = (elapsed / i) * (total_files - i)
                print(f" ETA: {eta/60:.1f} minutes remaining")
            
        except Exception as e:
            error_type = type(e).__name__
            stats['errors'][error_type] += 1
            stats['failed'] += 1
            print(f"   Failed on {file_path.name}: {error_type} - {str(e)}")
    
    # Final summary
    elapsed_total = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"🎉 PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f" Successfully processed: {stats['processed']:,}")
    print(f"   Failed: {stats['failed']:,}")
    print(f"🏢 Companies processed: {len(stats['companies'])}")
    print(f"⏱️  Total time: {elapsed_total/60:.1f} minutes")
    print(f"🚀 Average speed: {stats['processed']/(elapsed_total/60):.1f} files/min")
    
    # Show error breakdown if any
    if stats['errors']:
        print(f"\n   Error breakdown:")
        for error_type, count in stats['errors'].items():
            print(f"   {error_type}: {count}")
    
    # Show success rate
    success_rate = (stats['processed'] / total_files) * 100
    print(f"📊 Success rate: {success_rate:.1f}%")