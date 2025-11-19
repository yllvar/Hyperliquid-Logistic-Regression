import polars as pl
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, raw_dir: str = "data/raw", processed_dir: str = "data/processed"):
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def process_l2_day(self, coin: str, date: datetime):
        """
        Reads all hourly JSON files for a coin on a given date,
        concatenates them, and saves as a single Parquet file.
        """
        date_str = date.strftime("%Y%m%d")
        day_dir = self.raw_dir / "l2Book" / date_str
        
        if not day_dir.exists():
            logger.warning(f"No data found for {coin} on {date_str}")
            return

        # Collect all JSON files for the day
        json_files = sorted(list(day_dir.glob(f"**/{coin}.json")))
        
        if not json_files:
            logger.warning(f"No JSON files found for {coin} on {date_str}")
            return

        dfs = []
        for json_file in json_files:
            try:
                # L2 data is often a list of snapshots.
                # We need to handle potential schema variations.
                # Using polars lazy reading if possible, or eager for simplicity first.
                # Assuming standard JSON structure.
                
                # If the file contains multiple JSON objects (one per line), use read_ndjson
                # If it's a single JSON array, use read_json
                # We'll try read_json first.
                
                # Note: Hyperliquid L2 updates might be complex. 
                # Let's assume it's a list of book states.
                df = pl.read_json(json_file)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading {json_file}: {e}")

        if not dfs:
            return

        full_df = pl.concat(dfs)
        
        # Standardization
        # Ensure timestamp is datetime
        if "time" in full_df.columns:
            full_df = full_df.with_columns(
                pl.col("time").cast(pl.Int64).cast(pl.Datetime("ms")).alias("timestamp")
            )
        elif "timestamp" in full_df.columns:
             full_df = full_df.with_columns(
                pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms"))
            )
            
        # Save to Parquet
        output_dir = self.processed_dir / "l2Book" / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{coin}.parquet"
        
        full_df.write_parquet(output_file)
        logger.info(f"Saved processed data to {output_file}")
