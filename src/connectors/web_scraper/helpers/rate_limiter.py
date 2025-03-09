import time
import threading
from typing import Optional
from collections import deque
from datetime import datetime, timedelta

class RateLimiter:
    """
    Rate limiter to prevent too many requests in a short period.
    
    Uses a sliding window approach to track requests.
    """
    
    def __init__(self, requests_per_minute: int = 10):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum number of requests per minute
        """
        self.requests_per_minute = requests_per_minute
        self.window_size = 60  # 1 minute in seconds
        self.request_times = deque()
        self.lock = threading.Lock()
        
    def wait(self) -> None:
        """
        Wait if needed to comply with rate limits.
        """
        with self.lock:
            now = datetime.now()
            
            # Remove requests older than the window size
            cutoff_time = now - timedelta(seconds=self.window_size)
            while self.request_times and self.request_times[0] < cutoff_time:
                self.request_times.popleft()
            
            # If we've reached the limit, wait until we can make another request
            if len(self.request_times) >= self.requests_per_minute:
                # Calculate sleep time needed
                oldest_request = self.request_times[0]
                sleep_time = (oldest_request + timedelta(seconds=self.window_size) - now).total_seconds()
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            # Add current request to the queue
            self.request_times.append(datetime.now())