import time
import logging
import pandas as pd
import polars as pl
import joblib
from pathlib import Path
from src.live.connector import HyperliquidConnector
from src.features.calculator import FeatureCalculator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LiveEngine:
    def __init__(self, coin: str = "SOL", models_dir: str = "models"):
        self.coin = coin
        self.connector = HyperliquidConnector(coin)
        self.feature_calc = FeatureCalculator() # We'll reuse methods if possible
        self.models_dir = Path(models_dir)
        self.model = None
        self.scaler = None
        
        self.load_model()

    def load_model(self):
        model_files = sorted(list(self.models_dir.glob("lr_model_*.pkl")))
        scaler_files = sorted(list(self.models_dir.glob("scaler_*.pkl")))
        
        if not model_files or not scaler_files:
            raise FileNotFoundError("No model or scaler found.")
            
        self.model = joblib.load(model_files[-1])
        self.scaler = joblib.load(scaler_files[-1])
        logger.info(f"Loaded model: {model_files[-1].name}")

    def process_snapshot(self, book_data):
        # Debug logging
        # logger.info(f"Book Data Keys: {book_data.keys()}")
        levels = book_data.get("levels")
        if not levels or len(levels) < 2:
            logger.warning("Invalid levels data")
            return None
            
        # Convert single snapshot to DataFrame format expected by FeatureCalculator
        # FeatureCalculator expects Polars DataFrame with 'levels'
        
        # Construct Polars DataFrame
        # book_data['levels'] is [[bids], [asks]]
        # We need to wrap it in a list to make it a single row
        
        row = {
            "coin": self.coin,
            "time": book_data.get("time"),
            "levels": levels, # Do not wrap in list for Polars
            "timestamp": book_data.get("time") # Placeholder, will be cast
        }
        
        df = pl.DataFrame([row])
        
        # Cast timestamp
        df = df.with_columns(
            pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms"))
        )
        
        # logger.info(f"Schema: {df.schema}")
        # logger.info(f"Row: {df.head(1)}")
        
        # Compute features
        # Note: FeatureCalculator.compute_features expects a batch and does rolling windows.
        # For live inference, we have two options:
        # 1. Maintain a buffer of recent snapshots to compute rolling features.
        # 2. Use simplified features for MVP (no rolling).
        
        # The current FeatureCalculator uses:
        # - mid_price, spread, imbalance_1, wmp (Instantaneous)
        # - volatility_5m (Rolling)
        # - target (Future - not needed for inference)
        
        # We need to handle volatility. If we don't have history, it will be null.
        # For MVP, let's assume 0 volatility or handle it.
        # Ideally, we keep a buffer.
        
        features_df = self.feature_calc.compute_features(df, inference=True)
        
        # Handle missing rolling features
        features_df = features_df.fill_null(0)
        
        return features_df

    def run(self):
        logger.info("Starting Live Engine...")
        self.connector.start()
        
        try:
            while True:
                book = self.connector.get_latest_book()
                if book:
                    # Process
                    features_df = self.process_snapshot(book)
                    
                    # Prepare for model
                    # Convert to pandas
                    pdf = features_df.to_pandas()
                    
                    # Select features
                    drop_cols = ["timestamp", "coin", "target"]
                    feature_cols = [c for c in pdf.columns if c not in drop_cols]
                    
                    # Ensure columns match model expectation (might need to check scaler)
                    # For now assume they match if code hasn't changed.
                    
                    X = pdf[feature_cols]
                    X_scaled = self.scaler.transform(X)
                    
                    # Predict
                    prob = self.model.predict_proba(X_scaled)[0, 1]
                    
                    # Signal
                    signal = "HOLD"
                    if prob > 0.6:
                        signal = "BUY"
                    elif prob < 0.4:
                        signal = "SELL"
                        
                    logger.info(f"Price: {pdf['mid_price'].iloc[0]:.2f} | Prob: {prob:.4f} | Signal: {signal}")
                    
                else:
                    logger.info("Waiting for data...")
                
                time.sleep(1) # 1Hz update
                
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self.connector.stop()

if __name__ == "__main__":
    engine = LiveEngine()
    engine.run()
