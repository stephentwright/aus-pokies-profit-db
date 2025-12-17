import os
import requests
from datetime import datetime
from pathlib import Path

# Configuration
DATA_DIR = Path(__file__).parent / "raw-data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

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
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/clubs-gaming-machine-report-by-lga-1-june-2021-to-30-nov-2021.xlsx",
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
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-07/hotels-gaming-machine-report-by-lga-1-january-2017-to-30-june-2017.xlsx"
]

def download_file(url):
    """Download file and return a simple status record (no metadata writing)."""
    filename = url.split('/')[-1]
    filepath = DATA_DIR / filename
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            f.write(response.content)
        file_size = filepath.stat().st_size
        last_download = datetime.now().isoformat()
        return {'filename': filename, 'url': url, 'file_path': str(filepath), 'file_size': file_size, 'last_download': last_download, 'status': 'success'}
    except Exception as e:
        return {'filename': filename, 'url': url, 'file_path': str(filepath), 'file_size': None, 'last_download': datetime.now().isoformat(), 'status': f'failed: {e}'}

def main():
    print(f"Starting downloads at {datetime.now().isoformat()}")
    print(f"Data directory: {DATA_DIR}")
    if not URLS:
        print("No URLs configured. Update URLS list in script.")
        return
    for url in URLS:
        print(f"\nDownloading: {url}")
        result = download_file(url)
        print(f"Result: {result['status']} - {result['filename']}")
    print("\nDownloads complete. Run 01_build_metadata.py to build metadata and markdown.")

if __name__ == "__main__":
    main()