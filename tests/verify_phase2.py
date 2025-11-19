from src.features.calculator import FeatureCalculator
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def main():
    calculator = FeatureCalculator()
    
    coin = "SOL"
    date_str = "20240101"
    
    print(f"Processing features for {coin} on {date_str}...")
    
    try:
        df = calculator.load_data(coin, date_str)
        features_df = calculator.compute_features(df)
        
        print("Features computed:")
        print(features_df.head())
        print(features_df.schema)
        
        calculator.save_features(features_df, coin, date_str)
        print("Success!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
