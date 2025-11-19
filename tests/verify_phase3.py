from src.models.trainer import ModelTrainer
import logging

logging.basicConfig(level=logging.INFO)

def main():
    trainer = ModelTrainer()
    
    coin = "SOL"
    date_str = "20240101"
    
    print(f"Training model for {coin}...")
    
    try:
        df = trainer.load_dataset(coin, date_str)
        trainer.train(df)
        print("Training Complete!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
