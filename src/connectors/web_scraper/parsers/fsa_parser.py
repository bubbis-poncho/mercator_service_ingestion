import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from ..base_scraper import WebScraper
from ..config import ScraperConfig

logger = logging.getLogger(__name__)

class FSAScraper(WebScraper):
    """Swedish FSA (Finansinspektionen) scraper for insider trading data."""
    
    BASE_URL = "https://fi.se/en/our-registers/insynshandel/"
    SEARCH_URL = "https://fi.se/en/our-registers/insynshandel/"
    
    def __init__(self, config: ScraperConfig):
        """
        Initialize FSA scraper with configuration.
        
        Args:
            config: Scraper configuration
        """
        super().__init__(config)
        
    def search_insider_trading(self, 
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None,
                              emittent: Optional[str] = None,
                              person: Optional[str] = None) -> pd.DataFrame:
        """
        Search for insider trading records with optional filters.
        
        Args:
            start_date: Optional start date for transaction date filter
            end_date: Optional end date for transaction date filter
            emittent: Optional company/emittent name filter
            person: Optional person name filter
            
        Returns:
            DataFrame containing insider trading records
        """
        # Initialize parameters for search
        params = {}
        
        # Add date parameters if provided
        if start_date:
            params['TransaktionsdatumFrom'] = start_date.strftime('%Y-%m-%d')
        if end_date:
            params['TransaktionsdatumTo'] = end_date.strftime('%Y-%m-%d')
        
        # Add other filters if provided
        if emittent:
            params['Emittent'] = emittent
        if person:
            params['Person'] = person
            
        # Make initial search request
        logger.info(f"Searching insider trading with params: {params}")
        response = self._make_request(self.SEARCH_URL, params=params)
        
        # Extract data from all pages
        all_records = []
        current_page = 1
        max_pages = 100  # Safety limit
        
        while current_page <= max_pages:
            soup = self._parse_html(response.text)
            
            # Extract records from current page
            page_records = self._extract_records_from_page(soup)
            if not page_records:
                logger.warning(f"No records found on page {current_page}")
                break
                
            all_records.extend(page_records)
            logger.info(f"Extracted {len(page_records)} records from page {current_page}")
            
            # Check if there's a next page
            next_page_link = self._find_next_page_link(soup, current_page)
            if not next_page_link:
                logger.info(f"No more pages available after page {current_page}")
                break
                
            # Navigate to next page
            current_page += 1
            response = self._make_request(urljoin(self.BASE_URL, next_page_link))
            
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        logger.info(f"Total records extracted: {len(df)}")
        return df
    
    def _extract_records_from_page(self, soup: Any) -> List[Dict[str, Any]]:
        """
        Extract insider trading records from the FSA page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of records as dictionaries
        """
        records = []
        table = soup.find('table')
        
        if not table:
            logger.warning("No table found on page")
            return records
            
        # Extract headers
        headers = []
        header_row = table.find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
        
        # Process data rows
        data_rows = table.find_all('tr')[1:]  # Skip header row
        for row in data_rows:
            cells = row.find_all('td')
            if len(cells) == len(headers):
                record = {}
                for i, cell in enumerate(cells):
                    # Extract text and strip whitespace
                    value = cell.get_text(strip=True)
                    # Map to correct header
                    record[headers[i]] = value
                    
                # Add link to details if available
                details_link = row.find('a', string="Anmälan")
                if details_link and 'href' in details_link.attrs:
                    record['details_link'] = details_link['href']
                    
                records.append(record)
        
        return records
    
    def _find_next_page_link(self, soup: Any, current_page: int) -> Optional[str]:
        """
        Find link to next page if it exists.
        
        Args:
            soup: BeautifulSoup object of the page
            current_page: Current page number
            
        Returns:
            URL of next page or None if no next page
        """
        # Look for pagination controls
        pagination = soup.find('ul', class_='pagination')
        if not pagination:
            return None
            
        # Look for a link with text of current_page + 1
        next_page_num = current_page + 1
        next_link = pagination.find('a', string=str(next_page_num))
        
        if next_link and 'href' in next_link.attrs:
            return next_link['href']
        
        # Alternative: look for 'Nästa' (Next in Swedish) link
        next_link = pagination.find('a', string='Nästa')
        if next_link and 'href' in next_link.attrs:
            return next_link['href']
            
        return None
    
    def get_transaction_details(self, details_link: str) -> Dict[str, Any]:
        """
        Fetch and parse detailed information for a specific transaction.
        
        Args:
            details_link: Link to transaction details page
            
        Returns:
            Dictionary with detailed transaction information
        """
        full_url = urljoin(self.BASE_URL, details_link)
        response = self._make_request(full_url)
        soup = self._parse_html(response.text)
        
        # Extract detailed information - structure will depend on the page layout
        details = {}
        
        # This is a placeholder - actual implementation would depend on the page structure
        detail_sections = soup.find_all('div', class_='detail-section')
        for section in detail_sections:
            section_title = section.find('h3').get_text(strip=True)
            section_data = {}
            
            rows = section.find_all('div', class_='row')
            for row in rows:
                label = row.find('div', class_='label').get_text(strip=True)
                value = row.find('div', class_='value').get_text(strip=True)
                section_data[label] = value
                
            details[section_title] = section_data
            
        return details