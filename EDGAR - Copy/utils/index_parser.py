import re
import json
import requests

def strip_html_tags(text):
    return re.sub(r'<[^>]+>', '', text)

def extract_sec_filing_data(html_content):
    """Extract document format files and data files from SEC filing HTML
    
    Returns:
        dict: Contains 'documentFormatFiles' and 'dataFiles', both using description as key
    """
    
    # 1. Document Format Files (description as key, .htm and .txt links)
    doc_format_files = {}
    doc_section = html_content[html_content.find('<p>Document Format Files</p>'):html_content.find('<p>Data Files</p>')]
    
    rows = re.findall(r'<tr[^>]*>.*?</tr>', doc_section, re.DOTALL)
    count = 0
    for row in rows:
        match = re.search(
            r'<td[^>]*>.*?</td>\s*'                  # Seq #
            r'<td[^>]*>(.*?)</td>\s*'                # Description
            r'<td[^>]*>.*?href="([^"]+)"[^>]*>.*?</a>.*?</td>\s*'  # Link (may include iXBRL)
            r'<td[^>]*>(.*?)</td>',                  # Type
            row, re.DOTALL
        )
        if match:
            description = strip_html_tags(match.group(1).strip())
            file_type = strip_html_tags(match.group(3).strip())
            raw_href = match.group(2).strip()
            if description == "" and type != "":
                description = file_type
            elif description == "" and type  == "":
                description = str(count)
                file_type = str(count)
                count += 1
            elif description in doc_format_files.keys():
                description = str(count)
                count += 1
            
            # Normalize iXBRL link
            if raw_href.startswith("/ix?doc="):
                href_match = re.search(r'doc=([^&]+)', raw_href)
                href = href_match.group(1) if href_match else raw_href
            else:
                href = raw_href

            if href.endswith(('htm', 'html', 'txt')):

                doc_format_files[description] = {'type': file_type, 'doc': href}
    
    # 2. Data Files (description as key, all files)
    data_files = {}
    data_section = html_content[html_content.find('<p>Data Files</p>'):html_content.find('<!-- END DOCUMENT DIV -->')]
    rows2 = re.findall(r'<tr[^>]*>.*?</tr>', data_section, re.DOTALL)

    for row in rows2:
        match = re.search(
            r'<td[^>]*>.*?</td>\s*'                  # Seq #
            r'<td[^>]*>(.*?)</td>\s*'                # Description
            r'<td[^>]*>.*?href="([^"]+)"[^>]*>.*?</a>.*?</td>\s*'  # Link (may include iXBRL)
            r'<td[^>]*>(.*?)</td>',                  # Type
            row, re.DOTALL
        )
        if match:
            description = strip_html_tags(match.group(1).strip())
            file_type = strip_html_tags(match.group(3).strip())
            raw_href = match.group(2).strip()
            if description == "" and type != "":
                description = file_type
            elif description == "" and type  == "":
                description = str(count)
                file_type = str(count)
                count += 1
            elif description in doc_format_files.keys():
                description = str(count)
                count += 1
            # Normalize iXBRL link
            if raw_href.startswith("/ix?doc="):
                href_match = re.search(r'doc=([^&]+)', raw_href)
                href = href_match.group(1) if href_match else raw_href
            else:
                href = raw_href

            

            data_files[description] = {'type': file_type, 'doc': href}

    return {'documentFormatFiles': doc_format_files, 'dataFiles': data_files}

# Usage example:
if __name__ == "__main__":
    url = "https://www.sec.gov/Archives/edgar/data/1458631/000147793222006489/0001477932-22-006489-index.htm"

    headers = {
        "User-Agent": "YourName YourEmail@example.com",  # SEC requires this
        "Accept-Encoding": "gzip, deflate"
    }
    
    print(f"Fetching data from: {url}")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    
    # resp.text is already the HTML content - no need to open as file
    html_content = resp.text
    
    result = extract_sec_filing_data(html_content)
    print(json.dumps(result, indent=2))
    
 