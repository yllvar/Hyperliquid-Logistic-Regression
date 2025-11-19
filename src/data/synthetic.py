import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class SyntheticDataGenerator:
    def __init__(self, raw_dir: str = "data/raw"):
        self.raw_dir = Path(raw_dir)
    
    def generate_l2_data(self, coin: str, date: datetime, num_hours: int = 1):
        """Generates synthetic L2 orderbook data."""
        date_str = date.strftime("%Y%m%d")
        
        for hour in range(num_hours):
            hour_str = f"{hour:02d}"
            output_dir = self.raw_dir / "l2Book" / date_str / hour_str
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{coin}.json"
            
            logger.info(f"Generating synthetic data for {coin} at {date_str} {hour_str}:00")
            
            # Generate 60 snapshots (1 per minute)
            snapshots = []
            base_price = 100.0 if coin == "SOL" else 50000.0
            
            for i in range(60):
                timestamp = int((date + timedelta(hours=hour, minutes=i)).timestamp() * 1000)
                
                # Random walk price
                base_price *= (1 + random.uniform(-0.001, 0.001))
                
                # Generate bids and asks
                bids = []
                asks = []
                for level in range(10):
                    bid_px = base_price * (1 - (level + 1) * 0.0005)
                    ask_px = base_price * (1 + (level + 1) * 0.0005)
                    qty = random.uniform(1, 100)
                    bids.append({"px": f"{bid_px:.2f}", "sz": f"{qty:.2f}", "n": 1})
                    asks.append({"px": f"{ask_px:.2f}", "sz": f"{qty:.2f}", "n": 1})
                
                snapshot = {
                    "coin": coin,
                    "time": timestamp,
                    "levels": [bids, asks]
                }
                snapshots.append(snapshot)
            
            with open(output_file, 'w') as f:
                json.dump(snapshots, f)
                
        logger.info(f"Synthetic data generation complete for {coin}")
