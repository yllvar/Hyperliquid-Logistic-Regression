from src.data.synthetic import SyntheticDataGenerator
from src.data.processor import DataProcessor
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def main():
    # 1. Generate Synthetic Data (since S3 is inaccessible without creds)
    generator = SyntheticDataGenerator()
    
    # Pick a specific hour to keep it fast
    start_date = datetime(2024, 1, 1)
    
    print("Generating Synthetic Data...")
    generator.generate_l2_data("SOL", start_date, num_hours=2)
    
    # 2. Test Processor
    processor = DataProcessor()
    print("Testing Processor...")
    processor.process_l2_day("SOL", start_date)
    
    print("Verification Complete!")

if __name__ == "__main__":
    main()
