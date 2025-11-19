"""
Utility functions for date/time handling and logging.
"""

import logging
import sys
from datetime import datetime, timedelta
from typing import List, Tuple


def to_date_range(start_date: str, end_date: str) -> List[str]:
    """
    Generate a list of dates in YYYYMMDD format between start and end (inclusive).
    
    Args:
        start_date: Start date in YYYY-MM-DD or YYYYMMDD format
        end_date: End date in YYYY-MM-DD or YYYYMMDD format
        
    Returns:
        List of date strings in YYYYMMDD format
        
    Example:
        >>> to_date_range("2024-01-01", "2024-01-03")
        ['20240101', '20240102', '20240103']
    """
    # Parse dates
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    
    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")
    
    # Generate range
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    return dates


def to_hour_list(hours: str) -> List[int]:
    """
    Parse hour specification into a list of integers.
    
    Args:
        hours: Hour specification, e.g., "0-5,12,18-20" or "all"
        
    Returns:
        List of hour integers (0-23)
        
    Example:
        >>> to_hour_list("0-2,12")
        [0, 1, 2, 12]
        >>> to_hour_list("all")
        [0, 1, 2, ..., 23]
    """
    if hours.lower() == "all":
        return list(range(24))
    
    result = []
    for part in hours.split(","):
        part = part.strip()
        if "-" in part:
            # Range
            start, end = part.split("-")
            start, end = int(start), int(end)
            if not (0 <= start <= 23 and 0 <= end <= 23):
                raise ValueError(f"Hours must be 0-23, got {start}-{end}")
            result.extend(range(start, end + 1))
        else:
            # Single hour
            hour = int(part)
            if not (0 <= hour <= 23):
                raise ValueError(f"Hour must be 0-23, got {hour}")
            result.append(hour)
    
    return sorted(set(result))


def _parse_date(date_str: str) -> datetime:
    """
    Parse date string in YYYY-MM-DD or YYYYMMDD format.
    
    Args:
        date_str: Date string
        
    Returns:
        datetime object
    """
    # Try YYYY-MM-DD
    if "-" in date_str:
        return datetime.strptime(date_str, "%Y-%m-%d")
    # Try YYYYMMDD
    else:
        return datetime.strptime(date_str, "%Y%m%d")


def setup_logging(level: str = "INFO") -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
