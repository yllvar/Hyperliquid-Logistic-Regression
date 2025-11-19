import boto3
from botocore import UNSIGNED
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_direct_download():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = "hyperliquid-archive"
    
    # Try to find a valid key. The user suggested:
    # market_data/2024/12/24/13/l2Book/SOL.lz4
    # But 2024/12/24 is in the future relative to some datasets or maybe valid.
    # Let's try a known past date.
    # The user mentioned: market_data/20240101/00/l2Book/SOL.lz4 in previous attempts.
    # But the user's new prompt says: market_data/YYYY/MM/DD/HH/l2Book/{COIN}.lz4
    # Let's try both formats.
    
    # Format 1: market_data/YYYYMMDD/HH/l2Book/COIN.lz4 (Old assumption)
    # Format 2: market_data/YYYY/MM/DD/HH/l2Book/COIN.lz4 (User suggestion)
    
    # Let's try a very recent date or a standard one.
    # 2023-11-01 is a safe bet for historical data.
    
    keys_to_try = [
        "market_data/20231101/00/l2Book/SOL.lz4",
        "market_data/2023/11/01/00/l2Book/SOL.lz4",
        "20231101/market_data/00/l2Book/SOL.lz4",
        "market_data/2023-11-01/00/l2Book/SOL.lz4",
    ]
    
    for key in keys_to_try:
        logger.info(f"Testing HEAD object for: {key}")
        try:
            s3.head_object(Bucket=bucket, Key=key)
            logger.info(f"SUCCESS: Found {key}")
            
            # Try downloading small chunk
            # s3.download_file(bucket, key, "test_download.lz4")
            # logger.info("Download successful")
            return
        except Exception as e:
            logger.info(f"Failed {key}: {e}")

if __name__ == "__main__":
    test_direct_download()
