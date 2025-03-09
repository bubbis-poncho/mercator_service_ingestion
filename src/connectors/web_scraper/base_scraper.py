import logging
import time
from typing import Dict, List, Optional, Any
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime

from .config import ScraperConfig
from .helpers.retry_helper import retry_with_backoff
from .helpers.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

class WebScraper:
    """Base web scraper class with common functionality for all scrapers."""
    
    def __init__(self, config: ScraperConfig):
        """
        Initialize base web scraper with common configuration.
        
        Args:
            config: Scraper configuration
        """
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml'
        })
        self.rate_limiter = RateLimiter(
            requests_per_minute=self.config.requests_per_minute
        )
    
    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        Args:
            url: URL to request
            params: Optional query parameters
            
        Returns:
            HTTP response
            
        Raises:
            requests.exceptions.RequestException: On request failure after retries
        """
        self.rate_limiter.wait()
        
        # Add random delay to avoid detection
        if self.config.random_delay:
            time.sleep(random.uniform(0.5, 2.0))
            
        response = self.session.get(url, params=params, timeout=self.config.timeout)
        response.raise_for_status()
        return response
    
    def _parse_html(self, html_content: str) -> BeautifulSoup:
        """
        Parse HTML content using BeautifulSoup.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            BeautifulSoup object
        """
        return BeautifulSoup(html_content, 'html.parser')
    
    def _extract_table_data(self, table_element: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Generic method to extract data from an HTML table.
        
        Args:
            table_element: BeautifulSoup table element
            
        Returns:
            List of dictionaries containing table data
        """
        records = []
        
        if not table_element:
            logger.warning("No table element provided")
            return records
            
        # Extract headers
        headers = []
        header_row = table_element.find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
        
        # Process data rows
        data_rows = table_element.find_all('tr')[1:]  # Skip header row
        for row in data_rows:
            cells = row.find_all('td')
            if len(cells) == len(headers):
                record = {}
                for i, cell in enumerate(cells):
                    # Extract text and strip whitespace
                    value = cell.get_text(strip=True)
                    # Map to correct header
                    record[headers[i]] = value
                records.append(record)
        
        return records