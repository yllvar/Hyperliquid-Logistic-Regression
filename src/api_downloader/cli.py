"""
Command-line interface for Hyperliquid data downloader.
"""

import argparse
import logging
from typing import List

from .downloader import HyperliquidClient, HyperliquidAPIError
from .storage import save_snapshot
from .utils import to_date_range, to_hour_list, setup_logging

logger = logging.getLogger(__name__)


def download_data(
    symbols: List[str],
    start_date: str,
    end_date: str,
    hours: str,
    output_dir: str,
    log_level: str
) -> None:
    """
    Download historical L2 data for specified symbols, dates, and hours.
    
    Args:
        symbols: List of trading symbols
        start_date: Start date (YYYY-MM-DD or YYYYMMDD)
        end_date: End date (YYYY-MM-DD or YYYYMMDD)
        hours: Hour specification (e.g., "0-5,12" or "all")
        output_dir: Output directory for downloaded data
        log_level: Logging level
    """
    setup_logging(log_level)
    
    # Parse inputs
    dates = to_date_range(start_date, end_date)
    hour_list = to_hour_list(hours)
    
    logger.info(f"Symbols: {', '.join(symbols)}")
    logger.info(f"Dates: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    logger.info(f"Hours: {hour_list}")
    logger.info(f"Output: {output_dir}")
    
    # Statistics
    total_tasks = len(symbols) * len(dates) * len(hour_list)
    completed = 0
    failed = 0
    skipped = 0
    
    logger.info(f"Total tasks: {total_tasks}")
    
    # Download data
    with HyperliquidClient() as client:
        for symbol in symbols:
            for date in dates:
                for hour in hour_list:
                    try:
                        # Fetch data
                        data = client.get_l2_snapshot(symbol, date, hour)
                        
                        if data is None:
                            # Data not available
                            skipped += 1
                            logger.debug(
                                f"Skipped: {symbol} {date} {hour:02d} "
                                f"(not available)"
                            )
                        else:
                            # Save to disk
                            save_snapshot(symbol, date, hour, data, output_dir)
                            completed += 1
                            
                    except HyperliquidAPIError as e:
                        logger.error(
                            f"Failed: {symbol} {date} {hour:02d} - {e}"
                        )
                        failed += 1
                        
                    except Exception as e:
                        logger.error(
                            f"Unexpected error for {symbol} {date} {hour:02d}: {e}"
                        )
                        failed += 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("Download Summary:")
    logger.info(f"  Total tasks:  {total_tasks}")
    logger.info(f"  Completed:    {completed}")
    logger.info(f"  Skipped:      {skipped}")
    logger.info(f"  Failed:       {failed}")
    logger.info("=" * 60)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download historical L2 orderbook data from Hyperliquid",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download SOL data for Jan 1-3, 2024, all hours
  hl-download --symbols SOL --start-date 2024-01-01 --end-date 2024-01-03 --hours all

  # Download BTC and ETH for specific hours
  hl-download --symbols BTC ETH --start-date 2024-01-01 --end-date 2024-01-01 --hours 0-5,12,18-23

  # Custom output directory
  hl-download --symbols SOL --start-date 2024-01-01 --end-date 2024-01-01 --hours all --out ./my_data
        """
    )
    
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Trading symbols to download (e.g., BTC ETH SOL)"
    )
    
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD or YYYYMMDD format"
    )
    
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD or YYYYMMDD format"
    )
    
    parser.add_argument(
        "--hours",
        default="all",
        help='Hours to download (e.g., "0-5,12" or "all"). Default: all'
    )
    
    parser.add_argument(
        "--out",
        default="data/raw/api",
        help="Output directory. Default: data/raw/api"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level. Default: INFO"
    )
    
    args = parser.parse_args()
    
    try:
        download_data(
            symbols=args.symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            hours=args.hours,
            output_dir=args.out,
            log_level=args.log_level
        )
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
