import os
import lz4.frame
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
from tqdm import tqdm
from src.utils.s3_utils import get_s3_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HyperliquidDownloader:
    ARCHIVE_BUCKET = "hyperliquid-archive"
    
    def __init__(self, raw_data_dir: str = "data/raw"):
        self.s3 = get_s3_client(unsigned=True)
        self.raw_data_dir = Path(raw_data_dir)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def download_l2_data(self, coin: str, start_date: datetime, end_date: datetime):
        """
        Downloads L2 orderbook data for a specific coin and date range.
        Path format: market_data/{YYYY-MM-DD}/{hour}/l2/{coin}.lz4
        """
        logger.info(f"Starting L2 download for {coin} from {start_date.date()} to {end_date.date()}")
        
        current_date = start_date
        while current_date <= end_date:
            # New format: market_data/YYYY-MM-DD/HH/l2/{COIN}.lz4
            date_str = current_date.strftime("%Y-%m-%d")
            date_compact = current_date.strftime("%Y%m%d") # For local storage
            
            # Iterate through 24 hours
            for hour in range(24):
                hour_str = f"{hour:02d}"
                key = f"market_data/{date_str}/{hour_str}/l2/{coin}.lz4"
                
                output_dir = self.raw_data_dir / "l2Book" / date_compact / hour_str
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / f"{coin}.json"
                
                if output_file.exists():
                    logger.debug(f"Skipping existing file: {output_file}")
                    continue

                try:
                    logger.debug(f"Downloading {key}...")
                    # Direct GetObject, no listing
                    response = self.s3.get_object(Bucket=self.ARCHIVE_BUCKET, Key=key)
                    compressed_data = response['Body'].read()
                    
                    decompressed_data = lz4.frame.decompress(compressed_data)
                    text_data = decompressed_data.decode('utf-8')
                    
                    with open(output_file, 'w') as f:
                        f.write(text_data)
                    
                    logger.info(f"Downloaded {key}")
                        
                except self.s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logger.warning(f"File not found: {key}")
                    elif e.response['Error']['Code'] == "403":
                        logger.warning(f"Access Denied (Key might be wrong): {key}")
                    else:
                        logger.error(f"Error downloading {key}: {e}")
                except Exception as e:
                    logger.error(f"Error downloading {key}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Finished L2 download for {coin}")

    def download_trades(self, coin: str, start_date: datetime, end_date: datetime):
        """
        Downloads trade fills. 
        Note: Trade structure in S3 might differ. 
        Commonly found in similar paths or separate bucket.
        For this MVP, we will assume a similar structure or skip if unknown.
        
        Based on user prompt: s3://hl-mainnet-node-data/node_fills
        This bucket might require different handling.
        """
        # TODO: Implement trade download once exact path structure is confirmed.
        # For now, we focus on L2 as primary data source.
        logger.warning("Trade download not yet fully implemented due to path uncertainty.")
