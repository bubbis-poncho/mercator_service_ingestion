from dataclasses import dataclass
from typing import Optional

@dataclass
class ScraperConfig:
    """Configuration for web scraper."""
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    timeout: int = 30
    requests_per_minute: int = 10
    random_delay: bool = True
    proxy: Optional[str] = None
    max_retries: int = 3
    backoff_factor: float = 2.0
    retry_on_status: tuple = (500, 502, 503, 504)
    verify_ssl: bool = True


# backend/ingestion-service/src/connectors/web_scraper/helpers/retry_helper.py

import logging
import time
import functools
import requests
from typing import Callable, Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')

def retry_with_backoff(max_retries: int = 3, backoff_factor: float = 2.0) -> Callable:
    """
    Retry decorator with exponential backoff for request failures.
    
    Args:
        max_retries: Maximum number of retries
        backoff_factor: Multiplicative factor for backoff
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, 
                        requests.exceptions.HTTPError, 
                        requests.exceptions.ConnectionError, 
                        requests.exceptions.Timeout) as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded: {str(e)}")
                        raise
                    
                    # Calculate backoff time with exponential factor and jitter
                    backoff_time = backoff_factor ** retries
                    logger.warning(f"Request failed: {str(e)}. Retrying in {backoff_time:.2f} seconds...")
                    time.sleep(backoff_time)
                except Exception as e:
                    # Don't retry on other exceptions
                    logger.error(f"Non-retryable error: {str(e)}")
                    raise
        return wrapper
    return decorator