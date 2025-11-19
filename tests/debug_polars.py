import polars as pl
import logging

logging.basicConfig(level=logging.INFO)

def test_polars_logic():
    # Simulate the data structure from WebSocket
    # levels = [[bids], [asks]]
    # bids = [{"px": "100.0", "sz": "1.0", "n": 1}, ...]
    
    bids = [{"px": "100.0", "sz": "1.0", "n": 1}]
    asks = [{"px": "101.0", "sz": "1.0", "n": 1}]
    levels = [bids, asks]
    
    row = {
        "coin": "SOL",
        "time": 1234567890,
        "levels": levels, # Do not wrap in list for Polars when passing row dict
        "timestamp": 1234567890
    }
    
    df = pl.DataFrame([row])
    
    # Cast timestamp
    df = df.with_columns(
        pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms"))
    )
    
    print("Schema:", df.schema)
    print("Head:", df)
    
    # Logic from FeatureCalculator
    print("Step 1: Extract bids/asks")
    try:
        df = df.with_columns([
            pl.col("levels").list.get(0).alias("bids"),
            pl.col("levels").list.get(1).alias("asks")
        ])
        print("Step 1 Success")
        print(df.select(["bids", "asks"]))
    except Exception as e:
        print(f"Step 1 Failed: {e}")
        return

    print("Step 2: Extract prices")
    try:
        df = df.with_columns([
            pl.col("bids").list.get(0).struct.field("px").cast(pl.Float64).alias("bid_px"),
            pl.col("bids").list.get(0).struct.field("sz").cast(pl.Float64).alias("bid_sz"),
            pl.col("asks").list.get(0).struct.field("px").cast(pl.Float64).alias("ask_px"),
            pl.col("asks").list.get(0).struct.field("sz").cast(pl.Float64).alias("ask_sz"),
        ])
        print("Step 2 Success")
        print(df.select(["bid_px", "ask_px"]))
    except Exception as e:
        print(f"Step 2 Failed: {e}")
        return

if __name__ == "__main__":
    test_polars_logic()
