import polars as pl
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureCalculator:
    def __init__(self, processed_dir: str = "data/processed", features_dir: str = "data/features"):
        self.processed_dir = Path(processed_dir)
        self.features_dir = Path(features_dir)
        self.features_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self, coin: str, date_str: str) -> pl.DataFrame:
        """Loads processed Parquet data for a specific coin and date."""
        file_path = self.processed_dir / "l2Book" / date_str / f"{coin}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        return pl.read_parquet(file_path)

    def compute_features(self, df: pl.DataFrame, inference: bool = False) -> pl.DataFrame:
        """
        Computes features from L2 orderbook data.
        Assumes 'levels' column structure: [[bids], [asks]]
        where bids/asks are lists of structs {px: str, sz: str, n: int}
        """
        logger.info("Computing features...")
        
        # 1. Extract Best Bid/Ask and Sizes
        # levels[0] is bids, levels[1] is asks.
        # We assume they are sorted (Bids: High->Low, Asks: Low->High) usually.
        # But let's verify or just take the first element if we assume standard L2 snapshot ordering.
        # Hyperliquid snapshots usually order Bids DESC, Asks ASC.
        
        # Helper expression to get first element of the list (best price)
        # We need to cast String to Float
        
        df = df.with_columns([
            pl.col("levels").list.get(0).alias("bids"),
            pl.col("levels").list.get(1).alias("asks")
        ])

        # Extract Best Bid/Ask Price and Size
        # bids[0] -> {px, sz, n}
        df = df.with_columns([
            pl.col("bids").list.get(0).struct.field("px").cast(pl.Float64).alias("bid_px"),
            pl.col("bids").list.get(0).struct.field("sz").cast(pl.Float64).alias("bid_sz"),
            pl.col("asks").list.get(0).struct.field("px").cast(pl.Float64).alias("ask_px"),
            pl.col("asks").list.get(0).struct.field("sz").cast(pl.Float64).alias("ask_sz"),
        ])
        
        # 2. Basic Price Features
        df = df.with_columns([
            ((pl.col("bid_px") + pl.col("ask_px")) / 2).alias("mid_price"),
            (pl.col("ask_px") - pl.col("bid_px")).alias("spread"),
            ((pl.col("bid_sz") - pl.col("ask_sz")) / (pl.col("bid_sz") + pl.col("ask_sz"))).alias("imbalance_1")
        ])
        
        # 3. Weighted Mid Price
        df = df.with_columns([
            (
                (pl.col("bid_px") * pl.col("ask_sz") + pl.col("ask_px") * pl.col("bid_sz")) / 
                (pl.col("bid_sz") + pl.col("ask_sz"))
            ).alias("wmp")
        ])

        # 4. Time-based Features (Rolling)
        # Sort by time just in case
        df = df.sort("timestamp")
        
        # Returns
        df = df.with_columns([
            pl.col("mid_price").pct_change().alias("returns")
        ])
        
        # Rolling Volatility (e.g., 5-minute window)
        # Assuming 1-minute snapshots for synthetic data, but real data might be irregular.
        # We'll use row-based rolling for simplicity or time_based if needed.
        # Let's use a simple rolling_std over last 5 rows for now.
        if inference:
             # For inference, we might not have enough history in the batch.
             # We assume the caller handles history or we accept null/0.
             df = df.with_columns([
                pl.col("returns").rolling_std(window_size=5, min_periods=1).fill_null(0).alias("volatility_5m")
            ])
        else:
            df = df.with_columns([
                pl.col("returns").rolling_std(window_size=5).alias("volatility_5m")
            ])
        
        # 5. Target Generation
        if not inference:
            # y = 1 if price moves +0.1% within next 5 minutes
            # We need to look forward.
            # shift(-5) gets the price 5 steps ahead.
            
            future_return = (pl.col("mid_price").shift(-5) - pl.col("mid_price")) / pl.col("mid_price")
            
            df = df.with_columns([
                future_return.alias("future_return_5m")
            ])
            
            df = df.with_columns([
                (pl.col("future_return_5m") > 0.001).cast(pl.Int32).alias("target")
            ])
            
            # Drop rows with nulls (due to rolling/shifting)
            df = df.drop_nulls()
        else:
            # For inference, we don't need target, but we might need to fill nulls for features
            df = df.fill_null(0)
        
        # Cleanup intermediate columns
        select_cols = [
            "timestamp", "coin", 
            "bid_px", "ask_px", "bid_sz", "ask_sz",
            "mid_price", "spread", "imbalance_1", "wmp",
            "volatility_5m"
        ]
        
        if not inference:
            select_cols.append("target")
            
        df = df.select(select_cols)
        
        return df

    def save_features(self, df: pl.DataFrame, coin: str, date_str: str):
        output_dir = self.features_dir / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{coin}_features.parquet"
        df.write_parquet(output_file)
        logger.info(f"Saved features to {output_file}")

