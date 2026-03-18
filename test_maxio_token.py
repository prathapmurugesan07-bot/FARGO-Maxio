#!/usr/bin/env python3
"""
Test script for Maxio API Token-based authentication.
Verifies that the MaxioClient correctly uses MAXIO_API_TOKEN from environment.
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from maxio_client import MaxioClient

def test_client_initialization():
    """Test that MaxioClient initializes with token-based auth."""
    print("\n" + "="*60)
    print("TEST 1: Client Initialization")
    print("="*60)
    
    try:
        client = MaxioClient()
        print("✓ MaxioClient initialized successfully")
        print(f"  Base URL: {client.base_url}")
        print(f"  Auth Header: {client.session.headers.get('Authorization', 'NOT SET')[:50]}...")
        return True
    except Exception as e:
        print(f"✗ Failed to initialize client: {e}")
        return False

def test_get_customers():
    """Test fetching customers from Maxio API."""
    print("\n" + "="*60)
    print("TEST 2: Fetch Customers")
    print("="*60)
    
    try:
        client = MaxioClient()
        df = client.get_customers()
        
        if df.empty:
            print("⚠️  No customers found (might be empty data)")
            return True
        else:
            print(f"✓ Successfully fetched {len(df)} customers")
            print(f"  Columns: {list(df.columns)[:5]}...")
            print(f"  Sample data (first row):")
            for col in df.columns[:3]:
                print(f"    {col}: {df[col].iloc[0]}")
            return True
    except Exception as e:
        print(f"✗ Failed to fetch customers: {e}")
        return False

def test_get_subscriptions():
    """Test fetching subscriptions from Maxio API."""
    print("\n" + "="*60)
    print("TEST 3: Fetch Subscriptions")
    print("="*60)
    
    try:
        client = MaxioClient()
        df = client.get_subscriptions()
        
        if df.empty:
            print("⚠️  No subscriptions found (might be empty data)")
            return True
        else:
            print(f"✓ Successfully fetched {len(df)} subscriptions")
            print(f"  Columns: {list(df.columns)[:5]}...")
            return True
    except Exception as e:
        print(f"✗ Failed to fetch subscriptions: {e}")
        return False

def test_get_invoices():
    """Test fetching invoices from Maxio API."""
    print("\n" + "="*60)
    print("TEST 4: Fetch Invoices")
    print("="*60)
    
    try:
        client = MaxioClient()
        df = client.get_invoices()
        
        if df.empty:
            print("⚠️  No invoices found (might be empty data)")
            return True
        else:
            print(f"✓ Successfully fetched {len(df)} invoices")
            print(f"  Columns: {list(df.columns)[:5]}...")
            return True
    except Exception as e:
        print(f"✗ Failed to fetch invoices: {e}")
        return False

def check_environment():
    """Check that required environment variables are set."""
    print("\n" + "="*60)
    print("Environment Variables Check")
    print("="*60)
    
    required_vars = {
        'MAXIO_API_TOKEN': 'API Token',
        'MAXIO_URL': 'Base URL',
        'MAXIO_USERNAME': 'Username (fallback)',
        'MAXIO_PASSWORD': 'Password (fallback)',
    }
    
    all_set = True
    for var, desc in required_vars.items():
        value = os.getenv(var)
        if value:
            display_value = value if len(value) < 20 else value[:20] + "..."
            print(f"✓ {var}: {display_value}")
        else:
            print(f"✗ {var}: NOT SET (required for {desc})")
            all_set = False
    
    return all_set

def main():
    """Run all tests."""
    print("\n" + "█"*60)
    print("█ MAXIO API TOKEN-BASED AUTHENTICATION TEST SUITE")
    print("█"*60)
    
    # Check environment
    if not check_environment():
        print("\n✗ Missing required environment variables!")
        print("  Please ensure MAXIO_API_TOKEN is set in .env file")
        return False
    
    # Run tests
    tests = [
        ("Client Initialization", test_client_initialization),
        ("Get Customers", test_get_customers),
        ("Get Subscriptions", test_get_subscriptions),
        ("Get Invoices", test_get_invoices),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n✗ Unexpected error in {test_name}: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Token-based authentication is working.")
        return True
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
