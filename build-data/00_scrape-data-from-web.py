import os
import hashlib
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# Configuration
DATA_DIR = Path(__file__).parent / "raw-data"
DATA_DIR.mkdir(exist_ok=True)
METADATA_FILE = Path(__file__).parent / "raw-data/download_metadata.csv"

# URLs to download (update with your actual URLs)
URLS = [
    # Updated Reports for Clubs
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-quarterly-report-by-lga-feb-2025.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-08/clubs-gaming-machine-quarterly-report-by-lga-may-2025.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-quarterly-report-by-lga-feb-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-quarterly-report-by-lga-may-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-quarterly-report-by-lga-aug-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-quarterly-report-by-lga-nov-2024.xlsx",
    # Updated Reports for Hotels
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-quarterly-report-by-lga-mar-2025.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-08/hotels-gaming-machine-quarterly-report-by-lga-jun-2025_0.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-quarterly-report-by-lga-mar-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-quarterly-report-by-lga-jun-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-quarterly-report-by-lga-sep-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-quarterly-report-by-lga-dec-2024.xlsx",
    # Older Reports for Clubs
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-jun-2023-30-nov-2023.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-dec-2022-31-may-2023.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-jun-2022-30-nov-2022.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-dec-2021-31-may-2022.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-jun-2021-to-30-nov-2021.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-lga-report-1-dec-2020-to-31-may-2021.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-lga-report_1-june-2020-to-30-november-2020.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-lga-report_1-dec-2019-to-31-may-2020.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-by-lga-report-1-june-2019-to-30-november-2019.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-dec-2018-to-31-may-2019.XLSX",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-jun-2018-to-30-nov-2018.XLSX",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-december-2017-to-31-may-2018.XLSX",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-june-2017-to-30-november-2017.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-december-2016-to-31-may-2017.xlsx",
    # Older Reports for Hotels
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jul-2023-31-dec-2023.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jan-2023-30-jun-2023.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jul-2022-31-dec-2022.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jan-2022-30-jun-2022.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jul-2021-to-31-dec-2021.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-lga-report-1-jan-2021-to-30-jun-2021.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-july-2020-to-31-december-2020.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-lga-report_1-jan-2020-to-30-jun-2020.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-report-1-july-2019-to-31-december-2019.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jan-2019-to-30-jun-2019.XLSX",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-jul-2018-to-31-dec-2018.XLSX",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/new_hotels-gaming-machine-bi-annual-report-by-lga-jun-2018-1.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-july-2017-to-31-december-2017.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-december-2016-to-31-may-2017.xlsx"

]
# NOTES FOR URLS:
# Note 01: A software error linked to a legacy reporting tool resulted in 12 hotels (located in 11 separate local government areas) 
# not being published in our latest hotel gaming data reports. The software error only affected the reporting of total profit and 
# duty figures in the 11 local government areas for the 1 April 2018 to 30 June 2018 reporting period. No other reporting periods 
# were impacted. There were no discrepancies in gaming machine numbers.

def calculate_checksum(filepath, algorithm='sha256'):
    """Calculate file checksum."""
    hash_func = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def download_file(url):
    """Download file and return filepath and checksum."""
    try:
        filename = url.split('/')[-1]
        filepath = DATA_DIR / filename
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        checksum = calculate_checksum(filepath)
        file_size = filepath.stat().st_size
        
        return {
            'filename': filename,
            'url': url,
            'last_download': datetime.now().isoformat(),
            'checksum': checksum,
            'file_size': file_size,
            'status': 'success'
        }
    except Exception as e:
        return {
            'filename': url.split('/')[-1],
            'url': url,
            'last_download': datetime.now().isoformat(),
            'checksum': None,
            'file_size': None,
            'status': f'failed: {str(e)}'
        }

def load_metadata():
    """Load existing metadata or create new dataframe."""
    if METADATA_FILE.exists():
        return pd.read_csv(METADATA_FILE)
    return pd.DataFrame(columns=['filename', 'url', 'last_download', 'checksum', 'file_size', 'status'])

def update_metadata(new_record):
    """Update metadata CSV with new download record."""
    df = load_metadata()
    
    # Remove old entry if exists
    df = df[df['url'] != new_record['url']]
    
    # Append new record
    df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
    df.to_csv(METADATA_FILE, index=False)
    
    return df

def generate_metadata_markdown():
    """Generate markdown table summarizing downloads by entity type and period."""
    df = load_metadata()
    
    # Filter only successful downloads
    df = df[df['status'] == 'success'].copy()
    
    if df.empty:
        print("No successful downloads to report.")
        return
    
    # Extract entity type (clubs/hotels) and period from filename
    df['entity_type'] = df['filename'].str.extract(r'(clubs|hotels)', expand=False).str.capitalize()
    df['period'] = df['filename'].str.replace(r'(clubs|hotels)-gaming-machine-', '', regex=True).str.replace(r'\.xlsx?$', '', regex=True).str.replace('-', ' ').str.title()
    
    # Sort by entity type, then by date
    df = df.sort_values(['entity_type', 'last_download'], ascending=[True, False])
    
    # Generate markdown
    markdown = "# Download Metadata Summary\n\n"
    
    for entity in ['Clubs', 'Hotels']:
        entity_df = df[df['entity_type'] == entity]
        
        if entity_df.empty:
            continue
        
        markdown += f"## {entity}\n\n"
        markdown += "| Reporting Period | URL | Downloaded | Checksum |\n"
        markdown += "|---|---|---|---|\n"
        
        for _, row in entity_df.iterrows():
            url_link = f"[Link]({row['url']})"
            markdown += f"| {row['period']} | {url_link} | {row['last_download']} | `{row['checksum'][:16]}...` |\n"
        
        markdown += "\n"
    
    # Save to file
    output_file = Path(__file__).parent / "VALIDATE_DOWNLOADS.md"
    with open(output_file, 'w') as f:
        f.write(markdown)
    
    print(f"\nMarkdown summary saved to {output_file}")
    return markdown

def main():
    """Main download script."""
    print(f"Starting download at {datetime.now().isoformat()}")
    print(f"Metadata file: {METADATA_FILE}")
    print(f"Data directory: {DATA_DIR}")
    
    if not URLS:
        print("No URLs configured. Update URLS list in script.")
        return
    
    for url in URLS:
        print(f"\nDownloading: {url}")
        record = download_file(url)
        update_metadata(record)
        print(f"Status: {record['status']}")
    
    print(f"\nDownload complete. Metadata saved to {METADATA_FILE}")
    
    generate_metadata_markdown()

if __name__ == "__main__":
    main()