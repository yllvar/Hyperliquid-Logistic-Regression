from src.backtest.engine import Backtester
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

def main():
    backtester = Backtester()
    
    coin = "SOL"
    date_str = "20240101"
    
    print(f"Running backtest for {coin}...")
    
    try:
        equity, trades = backtester.run_backtest(coin, date_str)
        
        print("\n--- Backtest Results ---")
        print(f"Final Equity: {equity.iloc[-1]['equity']:.2f}")
        print(f"Total Trades: {len(trades)}")
        if not trades.empty:
            print("\nTrade Log:")
            print(trades[['time', 'side', 'price', 'reason', 'pnl'] if 'pnl' in trades.columns else ['time', 'side', 'price', 'reason']])
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
