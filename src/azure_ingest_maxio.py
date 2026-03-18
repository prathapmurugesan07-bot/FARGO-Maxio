"""
Azure Ingestion for Maxio API Data
Fetches customer, invoice, and other data from Maxio API and uploads to Azure Blob Storage
"""

from maxio_client import MaxioClient
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
maxio_api_token = os.getenv("MAXIO_API_TOKEN")
maxio_url = os.getenv("MAXIO_URL")
maxio_username = os.getenv("MAXIO_USERNAME")
maxio_password = os.getenv("MAXIO_PASSWORD")

azure_account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
azure_account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

# Validate required environment variables
required_vars = {
    'MAXIO_API_TOKEN': maxio_api_token,
    'MAXIO_URL': maxio_url,
    'AZURE_STORAGE_ACCOUNT': azure_account_name,
    'AZURE_STORAGE_KEY': azure_account_key,
    'AZURE_CONTAINER_NAME': container_name,
}

missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

# Initialize Maxio client
print("\n" + "="*80)
print("INITIALIZING MAXIO API CLIENT")
print("="*80)
try:
    client = MaxioClient(
        api_token=maxio_api_token,
        username=maxio_username,
        password=maxio_password,
        base_url=maxio_url
    )
    print("✓ Maxio client initialized successfully")
except Exception as e:
    print(f"✗ Failed to initialize Maxio client: {e}")
    raise

# Initialize Azure Blob Service client
print("\n" + "="*80)
print("INITIALIZING AZURE BLOB STORAGE CLIENT")
print("="*80)
try:
    blob_service_client = BlobServiceClient(
        account_url=f"https://{azure_account_name}.blob.core.windows.net",
        credential=azure_account_key
    )
    print("✓ Azure Blob Storage client initialized successfully")
except Exception as e:
    print(f"✗ Failed to initialize Azure client: {e}")
    raise

# Timestamp for folder organization
timestamp = datetime.now().strftime("%Y/%m/%d")
run_time = datetime.now().strftime("%Y%m%d_%H%M%S")

def upload_to_azure(df, blob_name, label):
    """
    Upload DataFrame to Azure Blob Storage as CSV
    
    Args:
        df: pandas DataFrame to upload
        blob_name: Path in Azure (e.g., 'maxio/customers/customers.csv')
        label: Descriptive label for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None or df.empty:
        logger.warning(f"⚠️  {label}: No data to upload")
        return False
    
    try:
        csv_data = df.to_csv(index=False)
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        blob_client.upload_blob(csv_data, overwrite=True)
        logger.info(f"✅ {label}")
        logger.info(f"   Uploaded to: {blob_name}")
        logger.info(f"   Records: {len(df)}")
        logger.info(f"   Columns: {list(df.columns)[:5]}...")
        return True
    except Exception as e:
        logger.error(f"❌ {label} - Failed to upload: {str(e)}")
        return False

# ============================================================================
# FETCH AND UPLOAD CUSTOMERS
# ============================================================================
print("\n" + "="*80)
print("FETCHING CUSTOMERS FROM MAXIO")
print("="*80)
try:
    df_customers = client.get_customers()
    if df_customers is not None and not df_customers.empty:
        logger.info(f"✓ Successfully fetched {len(df_customers)} customers")
        logger.info(f"  Columns: {list(df_customers.columns)}")
        
        # Upload to Azure
        blob_name = f"maxio/{timestamp}/customers/customers_{run_time}.csv"
        upload_to_azure(df_customers, blob_name, "Customers Data Upload")
    else:
        logger.warning("⚠️  No customer data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch customers: {str(e)}")

# ============================================================================
# FETCH AND UPLOAD INVOICES
# ============================================================================
print("\n" + "="*80)
print("FETCHING INVOICES FROM MAXIO")
print("="*80)
try:
    df_invoices = client.get_invoices()
    if df_invoices is not None and not df_invoices.empty:
        logger.info(f"✓ Successfully fetched {len(df_invoices)} invoices")
        logger.info(f"  Columns: {list(df_invoices.columns)}")
        
        # Upload to Azure
        blob_name = f"maxio/{timestamp}/invoices/invoices_{run_time}.csv"
        upload_to_azure(df_invoices, blob_name, "Invoices Data Upload")
    else:
        logger.warning("⚠️  No invoice data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch invoices: {str(e)}")

# ============================================================================
# FETCH AND UPLOAD SUBSCRIPTIONS (Optional - endpoint may not exist)
# ============================================================================
print("\n" + "="*80)
print("FETCHING SUBSCRIPTIONS FROM MAXIO")
print("="*80)
try:
    df_subscriptions = client.get_subscriptions()
    if df_subscriptions is not None and not df_subscriptions.empty:
        logger.info(f"✓ Successfully fetched {len(df_subscriptions)} subscriptions")
        logger.info(f"  Columns: {list(df_subscriptions.columns)}")
        
        # Upload to Azure
        blob_name = f"maxio/{timestamp}/subscriptions/subscriptions_{run_time}.csv"
        upload_to_azure(df_subscriptions, blob_name, "Subscriptions Data Upload")
    else:
        logger.warning("⚠️  No subscription data returned (endpoint may not exist)")
except Exception as e:
    logger.warning(f"⚠️  Subscriptions endpoint not available: {str(e)}")
    logger.info("   (This is normal if the subscriptions endpoint is not enabled)")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("INGESTION COMPLETE")
print("="*80)
print(f"✓ Data uploaded with timestamp: {run_time}")
print(f"✓ Azure Container: {container_name}")
print(f"✓ Folder Structure: maxio/{timestamp}/")
print("\nFolder structure created:")
print("  📁 maxio/")
print(f"    📁 {timestamp}/")
print("      📁 customers/     - Customer data")
print("      📁 invoices/      - Invoice data")
print("      📁 subscriptions/ - Subscription data (if available)")
print("="*80 + "\n")
