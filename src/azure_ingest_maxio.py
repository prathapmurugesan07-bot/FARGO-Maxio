"""
Azure Ingestion for Maxio API Data - 7 Endpoints
Fetches customer, invoice, transactions, payments, revenue entries, reports, and expense data
from Maxio API and uploads to Azure Blob Storage with organized folder structure
"""

from io import BytesIO
import time

from maxio_client import MaxioClient
from azure.storage.blob import BlobServiceClient, ContentSettings
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

AZURE_MAX_BLOCK_SIZE = int(os.getenv("AZURE_MAX_BLOCK_SIZE_MB", "4")) * 1024 * 1024
AZURE_MAX_SINGLE_PUT_SIZE = int(os.getenv("AZURE_MAX_SINGLE_PUT_SIZE_MB", "4")) * 1024 * 1024
AZURE_UPLOAD_TIMEOUT = int(os.getenv("AZURE_UPLOAD_TIMEOUT_SEC", "900"))
AZURE_CONNECTION_TIMEOUT = int(os.getenv("AZURE_CONNECTION_TIMEOUT_SEC", "30"))
AZURE_READ_TIMEOUT = int(os.getenv("AZURE_READ_TIMEOUT_SEC", "300"))
AZURE_UPLOAD_MAX_CONCURRENCY = int(os.getenv("AZURE_UPLOAD_MAX_CONCURRENCY", "4"))
AZURE_UPLOAD_RETRIES = int(os.getenv("AZURE_UPLOAD_RETRIES", "3"))

# Validate required environment variables
required_vars = {
    'MAXIO_URL': maxio_url,
    'AZURE_STORAGE_ACCOUNT': azure_account_name,
    'AZURE_STORAGE_KEY': azure_account_key,
    'AZURE_CONTAINER_NAME': container_name,
}

missing_vars = [key for key, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

if not maxio_api_token and not (maxio_username and maxio_password):
    raise ValueError(
        "Set MAXIO_API_TOKEN or MAXIO_USERNAME/MAXIO_PASSWORD in the environment."
    )

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
        credential=azure_account_key,
        max_block_size=AZURE_MAX_BLOCK_SIZE,
        max_single_put_size=AZURE_MAX_SINGLE_PUT_SIZE,
        retry_total=AZURE_UPLOAD_RETRIES + 2,
        retry_connect=AZURE_UPLOAD_RETRIES,
        retry_read=AZURE_UPLOAD_RETRIES,
        retry_status=AZURE_UPLOAD_RETRIES,
        connection_timeout=AZURE_CONNECTION_TIMEOUT,
        read_timeout=AZURE_READ_TIMEOUT,
    )
    print("✓ Azure Blob Storage client initialized successfully")
except Exception as e:
    print(f"✗ Failed to initialize Azure client: {e}")
    raise

# Timestamp parts for folder organization
run_started_at = datetime.now()
run_year = run_started_at.strftime("%Y")
run_month = run_started_at.strftime("%m")
run_day = run_started_at.strftime("%d")
run_time = run_started_at.strftime("%Y%m%d_%H%M%S")


def build_blob_name(endpoint_key):
    """
    Build the Azure blob path for an endpoint using endpoint/date folders.

    Example:
        maxio/customers/2026/03/18/customers_20260318_153045.csv
    """
    return (
        f"maxio/{endpoint_key}/{run_year}/{run_month}/{run_day}/"
        f"{endpoint_key}_{run_time}.csv"
    )


def build_folder_prefix(endpoint_key):
    """Build the Azure folder prefix for an endpoint/date path."""
    return f"maxio/{endpoint_key}/{run_year}/{run_month}/{run_day}/"


def build_folder_placeholder_name(endpoint_key):
    """Build a placeholder blob path so Azure shows the folder structure."""
    return f"{build_folder_prefix(endpoint_key)}_folder_placeholder"


def ensure_folder_structure(endpoint_key):
    """
    Create a zero-byte placeholder blob so the endpoint/date folder exists in Azure
    even when the endpoint has no rows or the data upload later fails.
    """
    placeholder_blob = build_folder_placeholder_name(endpoint_key)

    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=placeholder_blob,
        )
        blob_client.upload_blob(b"", overwrite=True, timeout=AZURE_UPLOAD_TIMEOUT)
        logger.info(f"📁 Folder structure ready: {build_folder_prefix(endpoint_key)}")
        return placeholder_blob
    except Exception as e:
        logger.error(
            f"❌ Failed to create folder structure for {endpoint_key}: {str(e)}"
        )
        return None

def upload_to_azure(df, blob_name, label):
    """
    Upload DataFrame to Azure Blob Storage as CSV
    
    Args:
        df: pandas DataFrame to upload
        blob_name: Path in Azure
            (e.g., 'maxio/customers/2026/03/18/customers_20260318_153045.csv')
        label: Descriptive label for logging
        
    Returns:
        dict: Upload result details
    """
    if df is None or df.empty:
        logger.warning(f"⚠️  {label}: No data to upload")
        return {
            "status": "NO_DATA",
            "records": 0,
            "blob_name": None,
            "error": "",
        }
    
    try:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        csv_size_mb = len(csv_bytes) / (1024 * 1024)

        logger.info(f"   CSV size: {csv_size_mb:.2f} MB")

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )

        for attempt in range(1, AZURE_UPLOAD_RETRIES + 1):
            try:
                blob_client.upload_blob(
                    BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    overwrite=True,
                    timeout=AZURE_UPLOAD_TIMEOUT,
                    max_concurrency=AZURE_UPLOAD_MAX_CONCURRENCY,
                    content_settings=ContentSettings(
                        content_type="text/csv; charset=utf-8"
                    ),
                )
                logger.info(f"✅ {label}")
                logger.info(f"   Uploaded to: {blob_name}")
                logger.info(f"   Records: {len(df)}")
                logger.info(f"   Columns: {list(df.columns)[:5]}...")
                return {
                    "status": "UPLOADED",
                    "records": len(df),
                    "blob_name": blob_name,
                    "error": "",
                }
            except Exception as upload_error:
                if attempt < AZURE_UPLOAD_RETRIES:
                    wait_seconds = 2 ** (attempt - 1)
                    logger.warning(
                        f"⚠️  {label} upload attempt {attempt}/{AZURE_UPLOAD_RETRIES} failed: "
                        f"{str(upload_error)}"
                    )
                    logger.warning(f"   Retrying upload in {wait_seconds}s...")
                    time.sleep(wait_seconds)
                    continue
                raise upload_error
    except Exception as e:
        logger.error(f"❌ {label} - Failed to upload: {str(e)}")
        return {
            "status": "UPLOAD_FAILED",
            "records": len(df),
            "blob_name": blob_name,
            "error": str(e),
        }


def create_result(status="PENDING", records=0, blob_name=None, error=""):
    """Create a consistent endpoint result payload for summary reporting."""
    return {
        "status": status,
        "records": records,
        "blob_name": blob_name,
        "error": error,
    }


def fetch_and_upload(endpoint_key, fetch_method, label):
    """
    Fetch an endpoint from Maxio and upload it to Azure when rows are returned.

    Returns:
        dict: Endpoint result details for summary reporting
    """
    placeholder_blob = ensure_folder_structure(endpoint_key)
    if not placeholder_blob:
        return create_result(
            status="UPLOAD_FAILED",
            error="Failed to create Azure folder structure placeholder.",
        )

    try:
        df = fetch_method()
        if df is None or df.empty:
            logger.warning(f"⚠️  {label}: No data returned from Maxio")
            return create_result(status="NO_DATA")

        logger.info(f"✓ Successfully fetched {len(df)} {endpoint_key}")
        logger.info(f"  Columns: {list(df.columns)[:5]}...")

        blob_name = build_blob_name(endpoint_key)
        return upload_to_azure(df, blob_name, label)
    except Exception as e:
        logger.error(f"✗ Failed to fetch {endpoint_key}: {str(e)}")
        return create_result(status="FETCH_FAILED", error=str(e))

# ============================================================================
# FETCH AND UPLOAD ALL MAXIO ENDPOINTS
# ============================================================================

# Track endpoint results
upload_results = {
    'customers': create_result(),
    'transactions': create_result(),
    'invoices': create_result(),
    'payments': create_result(),
    'revenue_entries': create_result(),
    'reports': create_result(),
    'expenses': create_result()
}

# ============================================================================
# 1. FETCH AND UPLOAD CUSTOMERS
# ============================================================================
print("\n" + "="*80)
print("FETCHING CUSTOMERS FROM MAXIO")
print("="*80)
try:
    upload_results['customers'] = fetch_and_upload(
        "customers",
        client.get_customers,
        "Customers Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch customers: {str(e)}")

# ============================================================================
# 2. FETCH AND UPLOAD TRANSACTIONS
# ============================================================================
print("\n" + "="*80)
print("FETCHING TRANSACTIONS FROM MAXIO")
print("="*80)
try:
    upload_results['transactions'] = fetch_and_upload(
        "transactions",
        client.get_transactions,
        "Transactions Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch transactions: {str(e)}")

# ============================================================================
# 3. FETCH AND UPLOAD INVOICES
# ============================================================================
print("\n" + "="*80)
print("FETCHING INVOICES FROM MAXIO")
print("="*80)
try:
    upload_results['invoices'] = fetch_and_upload(
        "invoices",
        client.get_invoices,
        "Invoices Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch invoices: {str(e)}")

# ============================================================================
# 4. FETCH AND UPLOAD PAYMENTS (Account Receivable)
# ============================================================================
print("\n" + "="*80)
print("FETCHING PAYMENTS FROM MAXIO")
print("="*80)
try:
    upload_results['payments'] = fetch_and_upload(
        "payments",
        client.get_payments,
        "Payments Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch payments: {str(e)}")

# ============================================================================
# 5. FETCH AND UPLOAD REVENUE ENTRIES
# ============================================================================
print("\n" + "="*80)
print("FETCHING REVENUE ENTRIES FROM MAXIO")
print("="*80)
try:
    upload_results['revenue_entries'] = fetch_and_upload(
        "revenue_entries",
        client.get_revenue_entries,
        "Revenue Entries Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch revenue entries: {str(e)}")

# ============================================================================
# 6. FETCH AND UPLOAD REPORTS DEFINITIONS
# ============================================================================
print("\n" + "="*80)
print("FETCHING REPORT DEFINITIONS FROM MAXIO")
print("="*80)
try:
    upload_results['reports'] = fetch_and_upload(
        "reports",
        client.get_reports,
        "Reports Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch reports: {str(e)}")

# ============================================================================
# 7. FETCH AND UPLOAD EXPENSES
# ============================================================================
print("\n" + "="*80)
print("FETCHING EXPENSES FROM MAXIO")
print("="*80)
try:
    upload_results['expenses'] = fetch_and_upload(
        "expenses",
        client.get_expenses,
        "Expenses Data",
    )
except Exception as e:
    logger.error(f"✗ Failed to fetch expenses: {str(e)}")

# ============================================================================
# SUMMARY AND RESULTS
# ============================================================================
print("\n" + "="*80)
print("INGESTION COMPLETE - SUMMARY")
print("="*80)
print(f"✓ Started at: {run_started_at.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"✓ Data uploaded with timestamp: {run_time}")
print(f"✓ Azure Container: {container_name}")
print(f"\n📊 UPLOAD RESULTS:")
print("-" * 80)

uploaded_count = sum(1 for result in upload_results.values() if result["status"] == "UPLOADED")
no_data_count = sum(1 for result in upload_results.values() if result["status"] == "NO_DATA")
failed_count = sum(
    1 for result in upload_results.values()
    if result["status"] in {"FETCH_FAILED", "UPLOAD_FAILED"}
)
uploaded_records = sum(
    result["records"] for result in upload_results.values()
    if result["status"] == "UPLOADED"
)
failed_upload_records = sum(
    result["records"] for result in upload_results.values()
    if result["status"] == "UPLOAD_FAILED"
)
total_endpoints = len(upload_results)

status_icons = {
    "UPLOADED": "✅",
    "NO_DATA": "⚪",
    "FETCH_FAILED": "❌",
    "UPLOAD_FAILED": "❌",
    "PENDING": "⏳",
}

for endpoint, result in upload_results.items():
    status = result["status"]
    icon = status_icons.get(status, "❓")
    details = [status.replace("_", " ").title()]

    if result["records"]:
        details.append(f"{result['records']} records")

    if result["blob_name"]:
        details.append(result["blob_name"])

    if result["error"]:
        details.append(result["error"][:120])

    print(f"  {icon} {endpoint.replace('_', ' ').title()} | " + " | ".join(details))

print("-" * 80)
print(
    f"✨ Uploaded: {uploaded_count}/{total_endpoints} | "
    f"No data: {no_data_count} | Failed: {failed_count}"
)
print(f"📦 Total records ingested to Azure: {uploaded_records:,}")
if failed_upload_records:
    print(f"⚠️ Records fetched but not uploaded: {failed_upload_records:,}")
print(f"\n📁 FOLDER STRUCTURE CREATED IN AZURE:")
print("  📦 maxio/")
print(f"    ├─ 📁 customers/{run_year}/{run_month}/{run_day}/")
print(f"    ├─ 📁 transactions/{run_year}/{run_month}/{run_day}/")
print(f"    ├─ 📁 invoices/{run_year}/{run_month}/{run_day}/")
print(f"    ├─ 📁 payments/{run_year}/{run_month}/{run_day}/")
print(f"    ├─ 📁 revenue_entries/{run_year}/{run_month}/{run_day}/")
print(f"    ├─ 📁 reports/{run_year}/{run_month}/{run_day}/")
print(f"    └─ 📁 expenses/{run_year}/{run_month}/{run_day}/")
print("\n" + "="*80 + "\n")
