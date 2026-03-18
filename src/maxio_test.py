"""
Test script for Maxio API integration.
Tests basic connectivity and data fetching.
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from maxio_client import MaxioClient

def main():
    # Load credentials from .env
    maxio_username = os.getenv("MAXIO_USERNAME")
    maxio_password = os.getenv("MAXIO_PASSWORD")
    maxio_url = os.getenv("MAXIO_URL", "https://e36.platform.maxio.com/fargosystems")
    
    if not maxio_username or not maxio_password:
        print("❌ Error: MAXIO_USERNAME and MAXIO_PASSWORD not found in .env")
        print("   Please add these credentials to your .env file")
        return
    
    print("=" * 80)
    print("MAXIO API CONNECTION TEST")
    print("=" * 80)
    
    try:
        # Initialize Maxio client
        client = MaxioClient(maxio_username, maxio_password, maxio_url)
        
        # Test: Fetch customers
        print("\n📌 Fetching Customers...")
        df_customers = client.get_customers()
        
        if not df_customers.empty:
            print(f"\n✅ Successfully fetched {len(df_customers)} customers")
            print("\nFirst 5 records:")
            print(df_customers.head())
        else:
            print("⚠️  No customers returned (empty response)")
        
        # Uncomment to also test subscriptions and invoices:
        # print("\n" + "=" * 80)
        # print("📌 Fetching Subscriptions...")
        # df_subs = client.get_subscriptions()
        # print(f"Fetched {len(df_subs)} subscriptions")
        
        # print("\n" + "=" * 80)
        # print("📌 Fetching Invoices...")
        # df_invoices = client.get_invoices()
        # print(f"Fetched {len(df_invoices)} invoices")
        
        print("\n" + "=" * 80)
        print("✅ Maxio API connection test completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTroubleshooting tips:")
        print("  1. Verify MAXIO_USERNAME and MAXIO_PASSWORD in .env are correct")
        print("  2. Verify MAXIO_URL is accessible: " + maxio_url)
        print("  3. Check that your Maxio account has API permissions enabled")
        print("  4. Ensure your credentials have access to /customers endpoint")

if __name__ == "__main__":
    main()
