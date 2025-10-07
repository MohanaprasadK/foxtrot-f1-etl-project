import logging
import requests
import pandas as pd
from typing import Dict, List, Optional, Any
import time
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class F1APIClient:
    """
    Client for interacting with OpenF1 API
    """
    
    def __init__(self, base_url: str = "https://api.openf1.org/v1"):
        # Ensure base_url doesn't end with slash to avoid double slashes
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Set common headers
        self.session.headers.update({
            'User-Agent': 'F1-ETL-Pipeline/1.0',
            'Accept': 'application/json'
        })
    
    def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Fetch data from OpenF1 API
        
        Args:
            endpoint: API endpoint (e.g., 'car_data', 'drivers', 'laps')
            params: Query parameters for the API
            
        Returns:
            List of records as dictionaries
        """
        try:
            # Remove leading slash from endpoint to avoid double slashes
            endpoint = endpoint.lstrip('/')
            url = f"{self.base_url}/{endpoint}"
            
            logger.info(f"🌐 Fetching data from: {endpoint}")
            if params:
                logger.info(f"   Parameters: {params}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"✅ Successfully fetched {len(data)} records from {endpoint}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed for {endpoint}: {e}")
            raise
        except ValueError as e:
            logger.error(f"❌ JSON parsing failed for {endpoint}: {e}")
            raise
    
    def fetch_data_with_pagination(self, endpoint: str, 
                                 params: Optional[Dict] = None,
                                 page_size: int = 1000) -> List[Dict]:
        """
        Fetch data with pagination support (if API supports it)
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            page_size: Number of records per page
            
        Returns:
            Combined list of all records
        """
        # OpenF1 API doesn't officially support pagination, 
        # but we'll implement a placeholder for future use
        logger.info(f"📄 Pagination requested for {endpoint}, but OpenF1 doesn't support it yet")
        return self.fetch_data(endpoint, params)
    
    def test_connection(self) -> bool:
        """
        Test API connection with a simple endpoint
        
        Returns:
            bool: True if connection successful
        """
        try:
            # Use a lightweight endpoint for testing
            test_data = self.fetch_data("sessions", params={"limit": 1})
            logger.info("✅ API connection test successful")
            return True
        except Exception as e:
            logger.error(f"❌ API connection test failed: {e}")
            return False
    
    def build_endpoint_url(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """
        Build complete URL for an endpoint with parameters
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            Complete URL string
        """
        endpoint = endpoint.lstrip('/')
        url = f"{self.base_url}/{endpoint}"
        if params:
            query_string = urlencode(params)
            url = f"{url}?{query_string}"
        return url


# Utility functions for easy usage
def fetch_f1_data(endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
    """Convenience function to fetch F1 data"""
    client = F1APIClient()
    return client.fetch_data(endpoint, params)

def test_api_connection() -> bool:
    """Convenience function to test API connection"""
    client = F1APIClient()
    return client.test_connection()