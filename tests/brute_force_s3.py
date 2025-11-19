import boto3
from botocore import UNSIGNED
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def brute_force_paths():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = "hyperliquid-archive"
    
    # We know AccessDenied means key doesn't exist or is blocked.
    # But usually for public buckets, if key exists, it works.
    # If key doesn't exist, it might return 404 or 403 depending on bucket policy.
    # Hyperliquid might return 403 for non-existent keys to prevent enumeration.
    
    # Let's try a very specific known path if possible.
    # Or try a different asset like BTC.
    
    paths = [
        "market_data/20231101/00/l2Book/BTC.lz4",
        "market_data/2023/11/01/00/l2Book/BTC.lz4",
        "20231101/market_data/00/l2Book/BTC.lz4",
        "market_data/l2Book/20231101/00/BTC.lz4",
    ]
    
    for p in paths:
        logger.info(f"Trying {p}")
        try:
            s3.head_object(Bucket=bucket, Key=p)
            logger.info(f"FOUND: {p}")
            return
        except Exception as e:
            logger.info(f"Failed: {e}")

if __name__ == "__main__":
    brute_force_paths()
