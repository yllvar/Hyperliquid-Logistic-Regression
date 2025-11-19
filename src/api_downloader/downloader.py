"""
Hyperliquid REST API Client with retry logic and error handling.
"""

import time
import random
import logging
import requests
import lz4.frame
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class HyperliquidAPIError(Exception):
    """Custom exception for Hyperliquid API errors."""
    pass


class HyperliquidClient:
    """
    Client for fetching historical data from Hyperliquid REST API.
    
    Supports:
    - L2 orderbook snapshots
    - Automatic retry with exponential backoff
    - LZ4 decompression
    """
    
    BASE_URL = "https://api.hyperliquid.xyz"
    MAX_RETRIES = 10
    INITIAL_BACKOFF = 1.0
    MAX_BACKOFF = 60.0
    
    def __init__(self, timeout: int = 30):
        """
        Initialize the Hyperliquid API client.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "HyperliquidDownloader/1.0",
            "Accept": "application/json",
        })
    
    def get_l2_snapshot(
        self, 
        symbol: str, 
        date: str, 
        hour: int
    ) -> Optional[bytes]:
        """
        Fetch L2 orderbook snapshot for a specific symbol, date, and hour.
        
        Args:
            symbol: Trading symbol (e.g., "SOL", "BTC")
            date: Date in YYYYMMDD format
            hour: Hour of day (0-23)
            
        Returns:
            Decompressed JSON data as bytes, or None if not available
            
        Raises:
            HyperliquidAPIError: On unrecoverable API errors
        """
        endpoint = f"{self.BASE_URL}/historical/l2book"
        params = {
            "symbol": symbol,
            "date": date,
            "hour": hour,
        }
        
        for attempt in range(self.MAX_RETRIES):
            try:
                logger.debug(
                    f"Fetching L2 snapshot: {symbol} {date} {hour:02d} "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                
                response = self.session.get(
                    endpoint,
                    params=params,
                    timeout=self.timeout,
                )
                
                # Handle different status codes
                if response.status_code == 200:
                    # Decompress LZ4 data
                    try:
                        decompressed = lz4.frame.decompress(response.content)
                        logger.info(
                            f"Successfully fetched {symbol} {date} {hour:02d}"
                        )
                        return decompressed
                    except Exception as e:
                        logger.error(f"LZ4 decompression failed: {e}")
                        raise HyperliquidAPIError(f"Decompression error: {e}")
                
                elif response.status_code == 404:
                    logger.warning(
                        f"Data not found: {symbol} {date} {hour:02d}"
                    )
                    return None
                
                elif response.status_code == 429:
                    # Rate limit - retry with backoff
                    backoff = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Rate limited. Retrying in {backoff:.2f}s..."
                    )
                    time.sleep(backoff)
                    continue
                
                elif response.status_code >= 500:
                    # Server error - retry
                    backoff = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Server error {response.status_code}. "
                        f"Retrying in {backoff:.2f}s..."
                    )
                    time.sleep(backoff)
                    continue
                
                else:
                    # Other error
                    raise HyperliquidAPIError(
                        f"API error {response.status_code}: {response.text}"
                    )
                    
            except requests.exceptions.Timeout:
                backoff = self._calculate_backoff(attempt)
                logger.warning(f"Request timeout. Retrying in {backoff:.2f}s...")
                time.sleep(backoff)
                continue
                
            except requests.exceptions.ConnectionError as e:
                backoff = self._calculate_backoff(attempt)
                logger.warning(
                    f"Connection error: {e}. Retrying in {backoff:.2f}s..."
                )
                time.sleep(backoff)
                continue
                
            except HyperliquidAPIError:
                # Don't retry on API errors
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise HyperliquidAPIError(f"Unexpected error: {e}")
        
        # Max retries exceeded
        raise HyperliquidAPIError(
            f"Max retries ({self.MAX_RETRIES}) exceeded for "
            f"{symbol} {date} {hour:02d}"
        )
    
    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff with jitter.
        
        Args:
            attempt: Current attempt number (0-indexed)
            
        Returns:
            Backoff time in seconds
        """
        backoff = min(
            self.INITIAL_BACKOFF * (2 ** attempt),
            self.MAX_BACKOFF
        )
        # Add jitter (±25%)
        jitter = backoff * 0.25 * (2 * random.random() - 1)
        return backoff + jitter
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
