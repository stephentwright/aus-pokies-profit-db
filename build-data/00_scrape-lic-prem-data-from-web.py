import os
import requests
from datetime import datetime
from pathlib import Path

# Configuration
DATA_DIR = Path(__file__).parent / "raw-data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# URLs to download (update with your actual URLs)
URLS = [
    # grab the licence premises twice yearly reports
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-08/premises-list-jul-2025.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-Jan-2025%20L%26G.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-Jul-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-Jan-2024.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-Jul-2023.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-jan-2023.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-Jul-2022.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-Jan-2022.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-july-2021.xlsx",
    "https://www.nsw.gov.au/sites/default/files/noindex/2025-01/premises-list-as-at-january-2021.xlsx"
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