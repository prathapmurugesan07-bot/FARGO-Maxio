import requests
import base64
import logging
import time
import pandas as pd
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MaxioClient:
    """
    Maxio API Client for fetching customer and billing data.
    Supports both Token-based and HTTP Basic Authentication.
    """

    def __init__(self, api_token: Optional[str] = None, username: Optional[str] = None, 
                 password: Optional[str] = None, base_url: Optional[str] = None):
        """
        Initialize Maxio API client.
        
        Args:
            api_token: Maxio API Token (preferred method)
            username: Maxio username (fallback for Basic Auth)
            password: Maxio password (fallback for Basic Auth)
            base_url: Maxio API base URL
        """
        # Get from env if not provided
        api_token = api_token or os.getenv('MAXIO_API_TOKEN')
        username = username or os.getenv('MAXIO_USERNAME')
        password = password or os.getenv('MAXIO_PASSWORD')
        base_url = base_url or os.getenv('MAXIO_URL')
        
        if not base_url:
            base_url = "https://e36.platform.maxio.com/fargosystems"
        
        self.base_url = base_url.rstrip('/') + '/api/v1.0'
        self.session = requests.Session()
        
        # Set up authentication - Token-based (preferred) or Basic Auth
        if api_token:
            logger.info("Using Token-based authentication")
            self.session.headers.update({
                "Authorization": f"Token {api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            })
        elif username and password:
            logger.info("Using Basic Authentication")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            self.session.headers.update({
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            })
        else:
            raise ValueError("Either MAXIO_API_TOKEN or (MAXIO_USERNAME and MAXIO_PASSWORD) must be provided")
        
        logger.info(f"Initialized Maxio client for {self.base_url}")

    def _request(self, method: str, endpoint: str, payload: Dict[str, Any] = None, 
                 params: Dict[str, Any] = None, retries: int = 3) -> Dict[str, Any]:
        """
        Make HTTP request to Maxio API with retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., 'customers' or '/customers')
            payload: Request body for POST requests
            params: Query parameters
            retries: Number of retry attempts
            
        Returns:
            Response JSON data
            
        Raises:
            Exception: If request fails after all retries
        """
        # Ensure endpoint starts with /
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint
            
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(retries):
            try:
                logger.info(f"[Attempt {attempt + 1}/{retries}] {method.upper()} {url}")
                
                if method.upper() == 'GET':
                    response = self.session.get(url, params=params, timeout=30)
                elif method.upper() == 'POST':
                    response = self.session.post(url, json=payload, params=params, timeout=30)
                else:
                    response = self.session.request(method, url, json=payload, params=params, timeout=30)
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited (429). Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                
                # Handle success
                if response.status_code in (200, 201):
                    logger.info(f"✓ Success [{response.status_code}]")
                    return response.json() if response.text else {}
                
                # Handle client errors
                if response.status_code == 401:
                    raise Exception(f"✗ Authentication failed [401]: Invalid credentials or token. Response: {response.text[:200]}")
                if response.status_code == 404:
                    raise Exception(f"✗ Endpoint not found [404]: {endpoint}. Response: {response.text[:200]}")
                if response.status_code >= 400:
                    error_msg = response.text[:500] if response.text else "No error message"
                    logger.error(f"✗ Request failed [{response.status_code}]: {error_msg}")
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    continue
                    
            except requests.exceptions.Timeout as e:
                logger.error(f"✗ Request timeout: {e}")
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
                raise Exception(f"Timeout after {retries} retries")
            except requests.exceptions.RequestException as e:
                logger.error(f"✗ Request exception: {e}")
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
                raise Exception(f"Request failed after {retries} retries: {e}")
        
        raise Exception(f"✗ Failed after {retries} retries for endpoint: {endpoint}")

    def paginate(self, endpoint: str, method: str = "GET", max_pages: int = None) -> List[Dict[str, Any]]:
        """
        Handle pagination for Maxio API endpoints.
        
        Maxio API uses standard pagination with:
        - count: total number of records
        - results: array of records for current page
        - next: URL for next page
        - previous: URL for previous page
        
        Args:
            endpoint: API endpoint to paginate (e.g., 'customers', 'invoices')
            method: HTTP method (GET or POST)
            max_pages: Maximum pages to fetch (None = all)
            
        Returns:
            List of all records from paginated endpoint
        """
        all_records = []
        page = 1
        
        while True:
            params = {"page": page}
            
            logger.info(f"📄 Fetching page {page} from {endpoint}...")
            response = self._request(method, endpoint, params=params)
            
            # Maxio API returns: {count, next, previous, results}
            if isinstance(response, dict) and "results" in response:
                records = response.get("results", [])
                count = response.get("count", 0)
            else:
                # Fallback for other response formats
                logger.warning(f"Unexpected response format. Keys: {list(response.keys()) if isinstance(response, dict) else 'N/A'}")
                records = []
            
            if not records:
                logger.info(f"✓ No more records on page {page}")
                break
            
            logger.info(f"  Found {len(records)} records on page {page}")
            all_records.extend(records)
            
            # Check if we've fetched all records
            if len(all_records) >= count:
                logger.info(f"✓ Fetched all {len(all_records)} records")
                break
            
            if max_pages and page >= max_pages:
                logger.info(f"✓ Reached max pages limit ({max_pages})")
                break
            
            page += 1
        
        logger.info(f"📦 Total records fetched: {len(all_records)}")
        return all_records

    def get_customers(self, fields: List[str] = None) -> pd.DataFrame:
        """
        Fetch all customers from Maxio API and return as DataFrame.
        
        Args:
            fields: Specific fields to extract (None = all)
            
        Returns:
            DataFrame with customer data
        """
        try:
            logger.info("🔍 Fetching customers from Maxio API...")
            customers = self.paginate("customers")
            
            if not customers:
                logger.warning("⚠️  No customers found")
                return pd.DataFrame()
            
            df = pd.json_normalize(customers)
            
            if fields:
                available_fields = [f for f in fields if f in df.columns]
                if available_fields:
                    df = df[available_fields]
            
            logger.info(f"✓ Successfully fetched {len(df)} customers")
            logger.info(f"  Columns: {list(df.columns)[:5]}..." if len(df.columns) > 5 else f"  Columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to fetch customers: {str(e)}")
            return pd.DataFrame()

    def get_subscriptions(self) -> pd.DataFrame:
        """
        Fetch all subscriptions from Maxio API and return as DataFrame.
        
        Returns:
            DataFrame with subscription data
        """
        try:
            logger.info("🔍 Fetching subscriptions from Maxio API...")
            subscriptions = self.paginate("subscriptions")
            
            if not subscriptions:
                logger.warning("⚠️  No subscriptions found")
                return pd.DataFrame()
            
            df = pd.json_normalize(subscriptions)
            logger.info(f"✓ Successfully fetched {len(df)} subscriptions")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to fetch subscriptions: {str(e)}")
            return pd.DataFrame()

    def get_invoices(self) -> pd.DataFrame:
        """
        Fetch all invoices from Maxio API and return as DataFrame.
        
        Returns:
            DataFrame with invoice data
        """
        try:
            logger.info("🔍 Fetching invoices from Maxio API...")
            invoices = self.paginate("invoices")
            
            if not invoices:
                logger.warning("⚠️  No invoices found")
                return pd.DataFrame()
            
            df = pd.json_normalize(invoices)
            logger.info(f"✓ Successfully fetched {len(df)} invoices")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to fetch invoices: {str(e)}")
            return pd.DataFrame()
