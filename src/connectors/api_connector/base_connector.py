import logging
import time
from typing import Dict, List, Optional, Any, Union, Callable
import json
import random
import requests
from datetime import datetime, timedelta
from ..web_scraper.helpers.retry_helper import retry_with_backoff
from ..web_scraper.helpers.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

class APIConnector:
    """Base API connector class with common functionality for all API connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize base API connector with common configuration.
        
        Args:
            config: API connector configuration containing:
                - base_url: Base URL for the API
                - headers: Default headers for requests
                - auth_type: Authentication type (none, basic, bearer, api_key)
                - auth_config: Authentication configuration
                - rate_limit: Rate limiting configuration
        """
        self.config = config
        self.base_url = config.get("base_url", "")
        self.headers = config.get("headers", {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        # Initialize session
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Setup authentication
        self.auth_type = config.get("auth_type", "none")
        self.auth_config = config.get("auth_config", {})
        self._setup_authentication()
        
        # Setup rate limiting
        rate_limit_config = config.get("rate_limit", {})
        self.rate_limiter = RateLimiter(
            requests_per_minute=rate_limit_config.get("requests_per_minute", 60)
        )
        
        # Setup pagination
        self.pagination_strategy = config.get("pagination_strategy", "none")
        
    def _setup_authentication(self) -> None:
        """Configure authentication based on the specified type."""
        if self.auth_type == "basic":
            username = self.auth_config.get("username", "")
            password = self.auth_config.get("password", "")
            self.session.auth = (username, password)
            
        elif self.auth_type == "bearer":
            token = self.auth_config.get("token", "")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            
        elif self.auth_type == "api_key":
            api_key = self.auth_config.get("api_key", "")
            key_name = self.auth_config.get("key_name", "api_key")
            key_in = self.auth_config.get("key_in", "header")
            
            if key_in == "header":
                self.session.headers.update({key_name: api_key})
            elif key_in == "query":
                # Will be added to request parameters
                self.auth_param = {key_name: api_key}
            
    def refresh_token(self) -> bool:
        """
        Refresh the authentication token.
        
        Returns:
            True if token refreshed successfully, False otherwise
        """
        if self.auth_type != "bearer" or "refresh_config" not in self.auth_config:
            return False
            
        refresh_config = self.auth_config.get("refresh_config", {})
        refresh_url = refresh_config.get("url", "")
        refresh_method = refresh_config.get("method", "POST").upper()
        refresh_data = refresh_config.get("data", {})
        
        try:
            response = requests.request(
                method=refresh_method,
                url=refresh_url,
                json=refresh_data,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            result = response.json()
            new_token = result.get(refresh_config.get("token_path", "access_token"))
            
            if new_token:
                # Update session headers with new token
                self.session.headers.update({"Authorization": f"Bearer {new_token}"})
                self.auth_config["token"] = new_token
                logger.info("Successfully refreshed authentication token")
                return True
            else:
                logger.error("Token path not found in refresh response")
                return False
                
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return False
    
    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def _make_request(self, 
                     method: str, 
                     endpoint: str, 
                     params: Optional[Dict] = None, 
                     data: Optional[Dict] = None,
                     json_data: Optional[Dict] = None) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: API endpoint (will be joined with base_url)
            params: Optional query parameters
            data: Optional form data
            json_data: Optional JSON data
            
        Returns:
            HTTP response
            
        Raises:
            requests.exceptions.RequestException: On request failure after retries
        """
        self.rate_limiter.wait()
        
        # Add random delay to avoid detection
        if self.config.get("random_delay", False):
            time.sleep(random.uniform(0.1, 1.0))
        
        # Construct URL
        url = endpoint if endpoint.startswith(('http://', 'https://')) else f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        # Merge auth params if using query authentication
        if self.auth_type == "api_key" and hasattr(self, "auth_param"):
            params = {**params, **self.auth_param} if params else self.auth_param
            
        logger.debug(f"Making {method} request to {url}")
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            json=json_data,
            timeout=self.config.get("timeout", 30)
        )
        
        # Check for rate limiting response
        if response.status_code == 429 or (response.status_code == 403 and "rate limit" in response.text.lower()):
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    sleep_time = int(retry_after)
                    logger.warning(f"Rate limited. Waiting for {sleep_time} seconds before retrying")
                    time.sleep(sleep_time)
                except ValueError:
                    # If Retry-After is a date
                    retry_date = datetime.strptime(retry_after, "%a, %d %b %Y %H:%M:%S %Z")
                    sleep_time = (retry_date - datetime.now()).total_seconds()
                    if sleep_time > 0:
                        logger.warning(f"Rate limited. Waiting until {retry_date} before retrying")
                        time.sleep(sleep_time)
            else:
                # If no Retry-After header, use exponential backoff
                logger.warning("Rate limited without Retry-After header. Using default backoff")
                time.sleep(5)
            
            # Try the request again
            return self._make_request(method, endpoint, params, data, json_data)
            
        response.raise_for_status()
        return response
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a GET request to the API.
        
        Args:
            endpoint: API endpoint
            params: Optional query parameters
            
        Returns:
            Response data as dictionary
        """
        response = self._make_request("GET", endpoint, params=params)
        return response.json()
    
    def post(self, endpoint: str, data: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a POST request to the API.
        
        Args:
            endpoint: API endpoint
            data: Optional form data
            json_data: Optional JSON data
            
        Returns:
            Response data as dictionary
        """
        response = self._make_request("POST", endpoint, data=data, json_data=json_data)
        return response.json()
    
    def put(self, endpoint: str, data: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a PUT request to the API.
        
        Args:
            endpoint: API endpoint
            data: Optional form data
            json_data: Optional JSON data
            
        Returns:
            Response data as dictionary
        """
        response = self._make_request("PUT", endpoint, data=data, json_data=json_data)
        return response.json()
    
    def delete(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a DELETE request to the API.
        
        Args:
            endpoint: API endpoint
            params: Optional query parameters
            
        Returns:
            Response data as dictionary
        """
        response = self._make_request("DELETE", endpoint, params=params)
        return response.json()
    
    def fetch_all_pages(self, 
                      endpoint: str, 
                      params: Optional[Dict] = None,
                      data_key: str = "data",
                      extract_function: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Fetch all pages of data using the configured pagination strategy.
        
        Args:
            endpoint: API endpoint
            params: Optional query parameters
            data_key: Key in response that contains data array
            extract_function: Optional function to extract data from response
            
        Returns:
            List of all items across all pages
        """
        all_items = []
        current_page = 1
        max_pages = self.config.get("max_pages", 100)  # Safety limit
        
        # Initialize params if None
        if params is None:
            params = {}
        
        # Add method-specific handling for each pagination strategy
        if self.pagination_strategy == "page":
            page_param = self.config.get("page_param", "page")
            
            while current_page <= max_pages:
                # Update params with current page
                params[page_param] = current_page
                
                response = self._make_request("GET", endpoint, params=params)
                response_data = response.json()
                
                # Extract data based on provided key or function
                if extract_function:
                    items = extract_function(response_data)
                elif data_key in response_data:
                    items = response_data[data_key]
                else:
                    # Assume response is an array directly
                    items = response_data if isinstance(response_data, list) else []
                
                if not items:
                    break
                    
                all_items.extend(items)
                logger.info(f"Fetched page {current_page} with {len(items)} items")
                
                # Check if there's a next page
                has_next = False
                if "meta" in response_data and "next" in response_data["meta"]:
                    has_next = bool(response_data["meta"]["next"])
                elif "has_more" in response_data:
                    has_next = response_data["has_more"]
                elif "next" in response_data:
                    has_next = bool(response_data["next"])
                elif len(items) > 0:
                    # If we have items, assume there could be more
                    has_next = True
                    
                if not has_next:
                    break
                    
                current_page += 1
                
        elif self.pagination_strategy == "offset":
            offset_param = self.config.get("offset_param", "offset")
            limit_param = self.config.get("limit_param", "limit")
            limit = self.config.get("limit", 100)
            
            # Set initial limit
            params[limit_param] = limit
            offset = 0
            
            while True:
                if current_page > max_pages:
                    break
                    
                # Update offset
                params[offset_param] = offset
                
                response = self._make_request("GET", endpoint, params=params)
                response_data = response.json()
                
                # Extract data
                if extract_function:
                    items = extract_function(response_data)
                elif data_key in response_data:
                    items = response_data[data_key]
                else:
                    items = response_data if isinstance(response_data, list) else []
                
                if not items:
                    break
                    
                all_items.extend(items)
                logger.info(f"Fetched offset {offset} with {len(items)} items")
                
                # If fewer items than limit, we've reached the end
                if len(items) < limit:
                    break
                    
                offset += limit
                current_page += 1
                
        elif self.pagination_strategy == "cursor":
            cursor_param = self.config.get("cursor_param", "cursor")
            cursor_path = self.config.get("cursor_path", "meta.next_cursor")
            cursor = None
            
            while True:
                if current_page > max_pages:
                    break
                    
                # Add cursor to params if not first page
                if cursor:
                    params[cursor_param] = cursor
                
                response = self._make_request("GET", endpoint, params=params)
                response_data = response.json()
                
                # Extract data
                if extract_function:
                    items = extract_function(response_data)
                elif data_key in response_data:
                    items = response_data[data_key]
                else:
                    items = response_data if isinstance(response_data, list) else []
                
                if not items:
                    break
                    
                all_items.extend(items)
                logger.info(f"Fetched page {current_page} with {len(items)} items")
                
                # Extract next cursor
                next_cursor = None
                cursor_parts = cursor_path.split('.')
                cursor_data = response_data
                
                try:
                    for part in cursor_parts:
                        cursor_data = cursor_data[part]
                    next_cursor = cursor_data
                except (KeyError, TypeError):
                    next_cursor = None
                
                if not next_cursor:
                    break
                    
                cursor = next_cursor
                current_page += 1
                
        elif self.pagination_strategy == "none" or not self.pagination_strategy:
            # No pagination, just fetch once
            response = self._make_request("GET", endpoint, params=params)
            response_data = response.json()
            
            # Extract data
            if extract_function:
                items = extract_function(response_data)
            elif data_key in response_data:
                items = response_data[data_key]
            else:
                items = response_data if isinstance(response_data, list) else []
                
            all_items.extend(items)
            logger.info(f"Fetched {len(items)} items (no pagination)")
            
        else:
            logger.error(f"Unsupported pagination strategy: {self.pagination_strategy}")
            
        return all_items
