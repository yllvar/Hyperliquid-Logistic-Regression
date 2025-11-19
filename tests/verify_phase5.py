from src.live.engine import LiveEngine
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)

def main():
    print("Initializing Live Engine...")
    try:
        engine = LiveEngine(coin="SOL")
        
        # Run in a separate thread so we can stop it
        t = threading.Thread(target=engine.run)
        t.daemon = True
        t.start()
        
        print("Running for 10 seconds...")
        time.sleep(10)
        
        print("Stopping...")
        engine.connector.stop()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
