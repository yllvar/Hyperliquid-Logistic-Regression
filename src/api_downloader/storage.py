"""
Storage utilities for saving downloaded data to disk.
"""

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        Path object for the directory
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_snapshot(
    symbol: str,
    date: str,
    hour: int,
    data: bytes,
    output_dir: Union[str, Path] = "data/raw/api"
) -> Path:
    """
    Save L2 snapshot data to disk.
    
    File structure: {output_dir}/{symbol}/{date}/{hour:02d}.json
    
    Args:
        symbol: Trading symbol (e.g., "SOL")
        date: Date in YYYYMMDD format
        hour: Hour of day (0-23)
        data: Decompressed JSON data as bytes
        output_dir: Base output directory
        
    Returns:
        Path to the saved file
    """
    output_dir = Path(output_dir)
    
    # Create directory structure: symbol/date/
    file_dir = output_dir / symbol / date
    ensure_directory(file_dir)
    
    # Save file: hour.json
    file_path = file_dir / f"{hour:02d}.json"
    
    try:
        with open(file_path, 'wb') as f:
            f.write(data)
        
        logger.info(f"Saved: {file_path} ({len(data)} bytes)")
        return file_path
        
    except Exception as e:
        logger.error(f"Failed to save {file_path}: {e}")
        raise
