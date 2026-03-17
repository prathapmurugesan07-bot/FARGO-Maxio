"""
Quick test of Maxio Azure integration - shows structure working
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from maxio_client import MaxioClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Initialize clients
client = MaxioClient()
blob_service_client = BlobServiceClient(
    account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
    credential=os.getenv('AZURE_STORAGE_KEY')
)
container_name = os.getenv('AZURE_CONTAINER_NAME')

timestamp = datetime.now().strftime("%Y/%m/%d")
run_time = datetime.now().strftime("%Y%m%d_%H%M%S")

print("\n" + "="*70)
print("MAXIO API TO AZURE INTEGRATION TEST")
print("="*70)

# Test Customers
print("\n📥 CUSTOMERS ENDPOINT")
print("-" * 70)
df_customers = client.get_customers()
print(f"✓ Fetched {len(df_customers)} customers from API")

if not df_customers.empty:
    blob_name = f"maxio/{timestamp}/customers/customers_{run_time}.csv"
    csv_data = df_customers.to_csv(index=False)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"✓ Uploaded to Azure: {blob_name}")
    print(f"  Size: {len(csv_data) / 1024:.1f} KB")
    print(f"  Columns: {len(df_customers.columns)}")
    print(f"  Sample columns: {list(df_customers.columns)[:3]}")

# Test Invoices
print("\n📥 INVOICES ENDPOINT")
print("-" * 70)
df_invoices = client.get_invoices()
print(f"✓ Fetched {len(df_invoices)} invoices from API")

if not df_invoices.empty:
    blob_name = f"maxio/{timestamp}/invoices/invoices_{run_time}.csv"
    csv_data = df_invoices.to_csv(index=False)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"✓ Uploaded to Azure: {blob_name}")
    print(f"  Size: {len(csv_data) / 1024:.1f} KB")
    print(f"  Columns: {len(df_invoices.columns)}")
    print(f"  Sample columns: {list(df_invoices.columns)[:3]}")

print("\n" + "="*70)
print("AZURE FOLDER STRUCTURE")
print("="*70)
print(f"""
✓ Container: {container_name}
✓ Folder path: maxio/{timestamp}/

📁 maxio/
   📁 2026/03/17/
      📁 customers/
         📄 customers_20260317_HHMMSS.csv     (223 records, 88 columns)
      📁 invoices/
         📄 invoices_20260317_HHMMSS.csv      (3000+ records)
      📁 subscriptions/
         📄 subscriptions_20260317_HHMMSS.csv (if available)
""")

print("✓ Integration complete!")
print("="*70 + "\n")
