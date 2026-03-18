"""
Azure Ingestion for Maxio API Data - 7 Endpoints
Fetches customer, invoice, transactions, payments, revenue entries, reports, and expense data
from Maxio API and uploads to Azure Blob Storage with organized folder structure
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

def upload_to_azure(df, blob_name, label):
    """
    Upload DataFrame to Azure Blob Storage as CSV
    
    Args:
        df: pandas DataFrame to upload
        blob_name: Path in Azure
            (e.g., 'maxio/customers/2026/03/18/customers_20260318_153045.csv')
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
# FETCH AND UPLOAD ALL MAXIO ENDPOINTS
# ============================================================================

# Track upload results
upload_results = {
    'customers': False,
    'transactions': False,
    'invoices': False,
    'payments': False,
    'revenue_entries': False,
    'reports': False,
    'expenses': False
}

# ============================================================================
# 1. FETCH AND UPLOAD CUSTOMERS
# ============================================================================
print("\n" + "="*80)
print("FETCHING CUSTOMERS FROM MAXIO")
print("="*80)
try:
    df_customers = client.get_customers()
    if df_customers is not None and not df_customers.empty:
        logger.info(f"✓ Successfully fetched {len(df_customers)} customers")
        logger.info(f"  Columns: {list(df_customers.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("customers")
        upload_results['customers'] = upload_to_azure(df_customers, blob_name, "Customers Data")
    else:
        logger.warning("⚠️  No customer data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch customers: {str(e)}")

# ============================================================================
# 2. FETCH AND UPLOAD TRANSACTIONS
# ============================================================================
print("\n" + "="*80)
print("FETCHING TRANSACTIONS FROM MAXIO")
print("="*80)
try:
    df_transactions = client.get_transactions()
    if df_transactions is not None and not df_transactions.empty:
        logger.info(f"✓ Successfully fetched {len(df_transactions)} transactions")
        logger.info(f"  Columns: {list(df_transactions.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("transactions")
        upload_results['transactions'] = upload_to_azure(df_transactions, blob_name, "Transactions Data")
    else:
        logger.warning("⚠️  No transaction data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch transactions: {str(e)}")

# ============================================================================
# 3. FETCH AND UPLOAD INVOICES
# ============================================================================
print("\n" + "="*80)
print("FETCHING INVOICES FROM MAXIO")
print("="*80)
try:
    df_invoices = client.get_invoices()
    if df_invoices is not None and not df_invoices.empty:
        logger.info(f"✓ Successfully fetched {len(df_invoices)} invoices")
        logger.info(f"  Columns: {list(df_invoices.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("invoices")
        upload_results['invoices'] = upload_to_azure(df_invoices, blob_name, "Invoices Data")
    else:
        logger.warning("⚠️  No invoice data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch invoices: {str(e)}")

# ============================================================================
# 4. FETCH AND UPLOAD PAYMENTS (Account Receivable)
# ============================================================================
print("\n" + "="*80)
print("FETCHING PAYMENTS FROM MAXIO")
print("="*80)
try:
    df_payments = client.get_payments()
    if df_payments is not None and not df_payments.empty:
        logger.info(f"✓ Successfully fetched {len(df_payments)} payments")
        logger.info(f"  Columns: {list(df_payments.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("payments")
        upload_results['payments'] = upload_to_azure(df_payments, blob_name, "Payments Data")
    else:
        logger.warning("⚠️  No payment data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch payments: {str(e)}")

# ============================================================================
# 5. FETCH AND UPLOAD REVENUE ENTRIES
# ============================================================================
print("\n" + "="*80)
print("FETCHING REVENUE ENTRIES FROM MAXIO")
print("="*80)
try:
    df_revenue_entries = client.get_revenue_entries()
    if df_revenue_entries is not None and not df_revenue_entries.empty:
        logger.info(f"✓ Successfully fetched {len(df_revenue_entries)} revenue entries")
        logger.info(f"  Columns: {list(df_revenue_entries.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("revenue_entries")
        upload_results['revenue_entries'] = upload_to_azure(df_revenue_entries, blob_name, "Revenue Entries Data")
    else:
        logger.warning("⚠️  No revenue entry data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch revenue entries: {str(e)}")

# ============================================================================
# 6. FETCH AND UPLOAD REPORTS DEFINITIONS
# ============================================================================
print("\n" + "="*80)
print("FETCHING REPORT DEFINITIONS FROM MAXIO")
print("="*80)
try:
    df_reports = client.get_reports()
    if df_reports is not None and not df_reports.empty:
        logger.info(f"✓ Successfully fetched {len(df_reports)} report definitions")
        logger.info(f"  Columns: {list(df_reports.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("reports")
        upload_results['reports'] = upload_to_azure(df_reports, blob_name, "Reports Data")
    else:
        logger.warning("⚠️  No report definition data returned")
except Exception as e:
    logger.error(f"✗ Failed to fetch reports: {str(e)}")

# ============================================================================
# 7. FETCH AND UPLOAD EXPENSES
# ============================================================================
print("\n" + "="*80)
print("FETCHING EXPENSES FROM MAXIO")
print("="*80)
try:
    df_expenses = client.get_expenses()
    if df_expenses is not None and not df_expenses.empty:
        logger.info(f"✓ Successfully fetched {len(df_expenses)} expenses")
        logger.info(f"  Columns: {list(df_expenses.columns)[:5]}...")
        
        # Upload to Azure
        blob_name = build_blob_name("expenses")
        upload_results['expenses'] = upload_to_azure(df_expenses, blob_name, "Expenses Data")
    else:
        logger.warning("⚠️  No expense data returned (table may be empty)")
        upload_results['expenses'] = False
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

successful_uploads = sum(1 for v in upload_results.values() if v)
total_endpoints = len(upload_results)

for endpoint, success in upload_results.items():
    status = "✅" if success else "❌"
    print(f"  {status} {endpoint.replace('_', ' ').title()}")

print("-" * 80)
print(f"✨ {successful_uploads}/{total_endpoints} endpoints successfully ingested")
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
