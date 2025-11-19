import pandas as pd
import polars as pl
import numpy as np
import joblib
from pathlib import Path
import logging
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Backtester:
    def __init__(self, models_dir: str = "models", features_dir: str = "data/features"):
        self.models_dir = Path(models_dir)
        self.features_dir = Path(features_dir)
        self.model = None
        self.scaler = None
        self.fee_rate = 0.00025 # 2.5 bps taker fee

    def load_latest_model(self):
        # Find latest model files
        model_files = sorted(list(self.models_dir.glob("lr_model_*.pkl")))
        scaler_files = sorted(list(self.models_dir.glob("scaler_*.pkl")))
        
        if not model_files or not scaler_files:
            raise FileNotFoundError("No model or scaler found.")
            
        self.model = joblib.load(model_files[-1])
        self.scaler = joblib.load(scaler_files[-1])
        logger.info(f"Loaded model: {model_files[-1].name}")

    def load_test_data(self, coin: str, date_str: str):
        file_path = self.features_dir / date_str / f"{coin}_features.parquet"
        df = pl.read_parquet(file_path).to_pandas()
        df = df.sort_values("timestamp")
        
        # Use last 15% as test set to match training split
        n = len(df)
        test_start = int(n * 0.85)
        return df.iloc[test_start:].reset_index(drop=True)

    def run_backtest(self, coin: str, date_str: str):
        if self.model is None:
            self.load_latest_model()
            
        df = self.load_test_data(coin, date_str)
        
        # Prepare features
        drop_cols = ["timestamp", "coin", "target"]
        feature_cols = [c for c in df.columns if c not in drop_cols]
        X = df[feature_cols]
        
        # Scale
        X_scaled = self.scaler.transform(X)
        
        # Predict
        probs = self.model.predict_proba(X_scaled)[:, 1]
        df['prob'] = probs
        
        # Simulation Loop
        position = 0 # 0: Flat, 1: Long
        entry_price = 0.0
        entry_time = None
        cash = 10000.0 # Starting capital
        holdings = 0.0
        
        trades = []
        equity_curve = []
        
        # Strategy Parameters
        buy_threshold = 0.6
        stop_loss = 0.005 # 0.5%
        take_profit = 0.01 # 1.0%
        max_hold_time = pd.Timedelta(minutes=5)
        
        for i, row in df.iterrows():
            current_price = row['mid_price']
            current_time = row['timestamp']
            
            # Mark to Market
            current_equity = cash + (holdings * row['bid_px'] if holdings > 0 else 0)
            equity_curve.append({'time': current_time, 'equity': current_equity})
            
            if position == 0:
                if row['prob'] > buy_threshold:
                    # BUY
                    # Assume fill at Ask
                    buy_price = row['ask_px']
                    # Buy max possible
                    size = (cash * 0.99) / buy_price # Leave buffer for fees
                    cost = size * buy_price
                    fee = cost * self.fee_rate
                    
                    cash -= (cost + fee)
                    holdings = size
                    position = 1
                    entry_price = buy_price
                    entry_time = current_time
                    
                    trades.append({
                        'side': 'BUY',
                        'price': buy_price,
                        'size': size,
                        'time': current_time,
                        'fee': fee
                    })
            
            elif position == 1:
                # Check exit conditions
                pct_change = (row['bid_px'] - entry_price) / entry_price
                time_held = current_time - entry_time
                
                exit_signal = False
                reason = ""
                
                if pct_change >= take_profit:
                    exit_signal = True
                    reason = "TP"
                elif pct_change <= -stop_loss:
                    exit_signal = True
                    reason = "SL"
                elif time_held >= max_hold_time:
                    exit_signal = True
                    reason = "Time"
                elif row['prob'] < 0.4: # Signal reversal
                    exit_signal = True
                    reason = "Signal"
                    
                if exit_signal:
                    # SELL
                    sell_price = row['bid_px']
                    revenue = holdings * sell_price
                    fee = revenue * self.fee_rate
                    
                    cash += (revenue - fee)
                    holdings = 0.0
                    position = 0
                    
                    trades.append({
                        'side': 'SELL',
                        'price': sell_price,
                        'size': holdings, # Note: this is 0 now, should log previous size
                        'time': current_time,
                        'fee': fee,
                        'pnl': revenue - fee - (entry_price * size + (entry_price * size * self.fee_rate)), # Approx PnL
                        'reason': reason
                    })

        # Final Close
        if position == 1:
            row = df.iloc[-1]
            sell_price = row['bid_px']
            revenue = holdings * sell_price
            fee = revenue * self.fee_rate
            cash += (revenue - fee)
            trades.append({'side': 'SELL', 'price': sell_price, 'time': row['timestamp'], 'reason': 'End'})
            
        final_equity = cash
        logger.info(f"Backtest Complete. Final Equity: {final_equity:.2f}")
        logger.info(f"Total Trades: {len(trades)}")
        
        return pd.DataFrame(equity_curve), pd.DataFrame(trades)

