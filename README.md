# Crypto Order Book Analysis & Trading System

A modular, data-driven quantitative trading system using Logistic Regression on Hyperliquid L2 order book data.

## Project Structure

```
.
├── data/               # Data storage
│   ├── raw/            # Raw JSON/LZ4 data
│   │   ├── api/        # REST API downloads
│   │   └── synthetic/  # Synthetic data for testing
│   ├── processed/      # Cleaned Parquet files
│   └── features/       # Feature datasets
├── models/             # Trained models and scalers
├── src/
│   ├── api_downloader/ # NEW: REST API data downloader
│   ├── data/           # Legacy S3 downloader (deprecated)
│   ├── features/       # Feature engineering
│   ├── models/         # Model training & validation
│   ├── backtest/       # Backtesting engine
│   └── live/           # Live WebSocket inference
└── tests/              # Verification scripts
```

## Quick Start

### 1. Setup
```bash
pip install -r requirements.txt
```

### 2. Download Real Data (NEW - REST API)

**Use the new REST API downloader to fetch real Hyperliquid data:**

```bash
# Download SOL data for Jan 1-3, 2024, all hours
python -m src.api_downloader.cli --symbols SOL \
    --start-date 2024-01-01 \
    --end-date 2024-01-03 \
    --hours all \
    --out data/raw/api

# Download multiple symbols for specific hours
python -m src.api_downloader.cli --symbols BTC ETH SOL \
    --start-date 2024-01-01 \
    --end-date 2024-01-01 \
    --hours 0-5,12,18-23

# Or use the test script
python tests/test_api_downloader.py
```

**Features:**
- ✅ No AWS credentials required
- ✅ Automatic retry with exponential backoff
- ✅ LZ4 decompression
- ✅ Rate limit handling
- ✅ Clean file organization

### 3. Data Pipeline (Synthetic - for testing)

For testing without real data:
```bash
# Generate synthetic data and process it
python tests/verify_phase1.py

# Compute features
python tests/verify_phase2.py
```

### 4. Model Training (Phase 3)
Train the Logistic Regression model on the processed data.
```bash
python tests/verify_phase3.py
```

### 5. Backtesting (Phase 4)
Test the model's performance on historical data.
```bash
python tests/verify_phase4.py
```

### 6. Live Trading (Phase 5)
Connect to Hyperliquid's mainnet WebSocket for real-time signals.
```bash
python src/live/engine.py
```

## REST API Downloader

### Module Structure

- **`downloader.py`**: Core API client with retry logic and error handling
- **`storage.py`**: File management and directory organization
- **`utils.py`**: Date/time utilities and logging setup
- **`cli.py`**: Command-line interface
- **`__init__.py`**: Package exports

### API Endpoints

The downloader uses Hyperliquid's official REST API:

```
GET https://api.hyperliquid.xyz/historical/l2book
Parameters:
  - symbol: string (e.g., "SOL", "BTC", "ETH")
  - date: YYYYMMDD format
  - hour: 0-23
```

### Output Format

Downloaded data is saved as:
```
data/raw/api/
  ├── SOL/
  │   └── 20240101/
  │       ├── 00.json
  │       ├── 01.json
  │       └── ...
  ├── BTC/
  └── ETH/
```

## Configuration
- **Coin**: Defaults to "SOL". Change in scripts or pass arguments.
- **Model**: Logistic Regression with standard scaling.
- **Features**: Mid-price, Spread, Imbalance, Weighted Mid-Price, Volatility (5m).

## Migration Notes

**Old S3 Downloader (Deprecated):**
- Located in `src/data/downloader.py`
- Required AWS credentials (no longer accessible)
- Used S3 bucket `hyperliquid-archive` (access denied)

**New REST API Downloader (Recommended):**
- Located in `src/api_downloader/`
- No credentials required
- Uses official Hyperliquid REST API
- Better error handling and retry logic

## Next Steps
- **Data**: Use the new REST API downloader to fetch real historical data
- **Training**: Retrain models on real data instead of synthetic
- **Execution**: Implement order placement logic in `src/live/engine.py`
