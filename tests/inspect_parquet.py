import polars as pl

def inspect_parquet():
    file_path = "data/processed/l2Book/20240101/SOL.parquet"
    try:
        df = pl.read_parquet(file_path)
        print("Schema:")
        print(df.schema)
        print("\nFirst 5 rows:")
        print(df.head(5))
        
        # Check the structure of 'levels'
        print("\nLevels column sample:")
        print(df.select("levels").head(1).to_dict())
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_parquet()
