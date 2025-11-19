from src.data.downloader import HyperliquidDownloader
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def main():
    downloader = HyperliquidDownloader()
    
    # Try a recent date that is likely to exist
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 1)
    
    print("Testing Real S3 Download...")
    downloader.download_l2_data("SOL", start_date, end_date)

if __name__ == "__main__":
    main()
