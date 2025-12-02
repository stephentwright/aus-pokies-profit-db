import hashlib
from datetime import datetime
from pathlib import Path
import pandas as pd
import importlib.util
import re

# Configuration
DATA_DIR = Path(__file__).parent / "raw-data"
METADATA_FILE = Path(__file__).parent / "raw-data" / "download_metadata.csv"
OUTPUT_MD = Path(__file__).parent / "VALIDATE_DOWNLOADS.md"

# Load URLS from 00_scrape-data-from-web.py (safe file import so it works even if directory name has a hyphen)
src_path = Path(__file__).parent / "00_scrape-data-from-web.py"
spec = importlib.util.spec_from_file_location("scraper_00", str(src_path))
scraper_00 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper_00)
URLS = getattr(scraper_00, "URLS", [])

# Build a filename -> url map for convenience
URL_MAP = {u.split('/')[-1]: u for u in URLS}

def calculate_checksum(filepath, algorithm='sha256'):
    h = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def parse_period_text(text):
    """
    Try to extract a start and end date from a free-text period string.
    Returns (start_str, end_str) in "YYYYMMDD" form (e.g. "20231201") or (None, None).
    """
    if not text or pd.isna(text):
        return None, None
    s = re.sub(r'\s+', ' ', str(text)).strip()
    sep_pattern = r'\s+(?:to|-|–|—|through|until)\s+'
    # day month year  to day month year
    m = re.search(
        r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})' + sep_pattern + r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})',
        s, flags=re.IGNORECASE
    )
    if m:
        try:
            start = pd.to_datetime(m.group(1), dayfirst=True)
            end = pd.to_datetime(m.group(2), dayfirst=True)
            return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
        except Exception:
            return None, None
    # numeric dates like 01/12/2023 to 29/02/2024
    m = re.search(
        r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})' + sep_pattern + r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
        s
    )
    if m:
        try:
            start = pd.to_datetime(m.group(1), dayfirst=True)
            end = pd.to_datetime(m.group(2), dayfirst=True)
            return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
        except Exception:
            return None, None
    # month year to month year (e.g. December 2023 to February 2024)
    m = re.search(
        r'([A-Za-z]+\s+\d{4})' + sep_pattern + r'([A-Za-z]+\s+\d{4})',
        s, flags=re.IGNORECASE
    )
    if m:
        try:
            start = pd.to_datetime(m.group(1))
            end = pd.to_datetime(m.group(2))
            return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
        except Exception:
            return None, None
    # single date anywhere
    m = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', s, flags=re.IGNORECASE)
    if m:
        try:
            dt = pd.to_datetime(m.group(1), dayfirst=True)
            return dt.strftime("%Y%m%d"), None
        except Exception:
            return None, None
    return None, None

def find_header_row_and_period(filepath):
    """
    Read a file and find the header row containing 'LGA' or 'Local Government Area'.
    Extract a single reporting period text from rows before the header.

    Returns: (header_row_number, period_string, start_period, end_period) or (None, None, None, None)
    """
    try:
        df_raw = pd.read_excel(filepath, sheet_name=0, header=None)
        header_row = None
        header_re = re.compile(r'\b(TAX|PREMISES COUNT)\b', flags=re.IGNORECASE)

        for idx, row in df_raw.iterrows():
            cells = row.astype(str).fillna('').tolist()
            row_str = ' '.join(cells).strip()
            if header_re.search(row_str):
                header_row = idx
                break

        if header_row is None:
            return None, None, None, None

        # Look back a few rows for period text
        period = None
        for idx in range(max(0, header_row - 8), header_row):
            row_text = ' '.join(df_raw.iloc[idx].astype(str).fillna('').tolist()).strip()
            if row_text and row_text.lower() != 'nan':
                if any(k in row_text.upper() for k in ['PERIOD', 'YEAR', 'MONTH', 'DATE', 'TO']) or re.search(r'\d{4}', row_text):
                    period = row_text
                    break

        start_period, end_period = parse_period_text(period)
        return header_row, period, start_period, end_period
    except Exception:
        return None, None, None, None

def extract_header_info(filepath, header_row):
    """
    Extract header names and column count from the specified header row.
    
    Returns: (column_count, header_string) where header_string is pipe-delimited
             e.g. "LGA|TAX|PREMISES COUNT|..."
             or (None, None) if extraction fails
    """
    try:
        if header_row is None:
            return None, None
        
        df_raw = pd.read_excel(filepath, sheet_name=0, header=None)
        header_cells = df_raw.iloc[header_row].astype(str).fillna('').tolist()
        
        # Filter out empty cells and 'nan' strings
        header_names = [cell.strip() for cell in header_cells if cell.strip() and cell.strip().lower() != 'nan']
        
        column_count = len(header_names)
        header_string = '|'.join(header_names)
        
        return column_count, header_string
    except Exception:
        return None, None

def build_metadata():
    records = []
    if not DATA_DIR.exists():
        print(f"No data directory at {DATA_DIR}")
        return pd.DataFrame(columns=['type', 'filename', 'url', 'last_download', 'checksum', 'file_size', 'header_row', 'column_count', 'header_names', 'start_period', 'end_period', 'status'])
    
    for p in sorted(DATA_DIR.iterdir()):
        if not p.is_file():
            continue
        fname = p.name
        try:
            # Extract entity type from filename
            if 'clubs' in fname.lower():
                entity_type = 'club'
            elif 'hotels' in fname.lower():
                entity_type = 'hotel'
            else:
                entity_type = 'unknown'
            
            checksum = calculate_checksum(p)
            file_size = p.stat().st_size
            last_download = datetime.fromtimestamp(p.stat().st_mtime).isoformat()
            url = URL_MAP.get(fname, None)
            
            # Extract header row and parsed start/end
            header_row, _, start_period, end_period = find_header_row_and_period(p)
            
            # Extract header info (column count and names)
            column_count, header_names = extract_header_info(p, header_row)
            
            records.append({
                'type': entity_type,
                'filename': fname,
                'url': url,
                'last_download': last_download,
                'checksum': checksum,
                'file_size': file_size,
                'header_row': header_row,
                'column_count': column_count,
                'header_names': header_names,
                'start_period': start_period,
                'end_period': end_period,
                'status': 'success'
            })
        except Exception as e:
            # Extract entity type from filename even on error
            if 'clubs' in fname.lower():
                entity_type = 'club'
            elif 'hotels' in fname.lower():
                entity_type = 'hotel'
            else:
                entity_type = 'unknown'
            
            records.append({
                'type': entity_type,
                'filename': fname,
                'url': URL_MAP.get(fname, None),
                'last_download': datetime.now().isoformat(),
                'checksum': None,
                'file_size': None,
                'header_row': None,
                'column_count': None,
                'header_names': None,
                'start_period': None,
                'end_period': None,
                'status': f'failed: {e}'
            })
    
    df = pd.DataFrame(records, columns=['type', 'filename', 'url', 'last_download', 'checksum', 'file_size', 'header_row', 'column_count', 'header_names', 'start_period', 'end_period', 'status'])
    df.to_csv(METADATA_FILE, index=False)
    print(f"Metadata written to {METADATA_FILE}")
    return df

def generate_metadata_markdown(df=None):
    if df is None:
        if not METADATA_FILE.exists():
            print("No metadata CSV found; run build_metadata first.")
            return
        df = pd.read_csv(METADATA_FILE)
    df_success = df[df['status'] == 'success'].copy()
    if df_success.empty:
        print("No successful downloads to report.")
        return
    df_success['entity_type'] = df_success['filename'].str.extract(r'(clubs|hotels)', expand=False).str.capitalize()
    df_success['period'] = df_success['filename'] \
        .str.replace(r'(clubs|hotels)-gaming-machine-', '', regex=True) \
        .str.replace(r'\.xlsx?$', '', regex=True) \
        .str.replace('-', ' ', regex=False) \
        .str.title()
    df_success = df_success.sort_values(['entity_type', 'last_download'], ascending=[True, False])

    md = "# Download Metadata Summary\n\n"
    for entity in ['Clubs', 'Hotels']:
        ent_df = df_success[df_success['entity_type'] == entity]
        if ent_df.empty:
            continue
        md += f"## {entity}\n\n"
        md += "| Reporting Period | URL | Downloaded | Checksum |\n"
        md += "|---|---|---|---|\n"
        for _, row in ent_df.iterrows():
            url_link = f"[Link]({row['url']})" if pd.notna(row['url']) else ""
            checksum_preview = f"`{str(row['checksum'])[:16]}...`" if pd.notna(row['checksum']) else ""
            md += f"| {row['period']} | {url_link} | {row['last_download']} | {checksum_preview} |\n"
        md += "\n"

    OUTPUT_MD.write_text(md)
    print(f"Markdown summary saved to {OUTPUT_MD}")
    return md

def main():
    print("Building metadata from files in:", DATA_DIR)
    df = build_metadata()
    generate_metadata_markdown(df)

if __name__ == "__main__":
    main()