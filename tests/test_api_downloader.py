"""
Test script for the Hyperliquid REST API downloader.
"""

from src.api_downloader.cli import download_data

def main():
    print("Testing Hyperliquid REST API Downloader...")
    print("Downloading SOL data for 2024-01-01, hour 0...")
    
    try:
        download_data(
            symbols=["SOL"],
            start_date="2024-01-01",
            end_date="2024-01-01",
            hours="0",
            output_dir="data/raw/api",
            log_level="INFO"
        )
        print("\nTest completed!")
    except Exception as e:
        print(f"\nTest failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
