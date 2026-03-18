#!/usr/bin/env python3
"""
Test all 7 Maxio API endpoints to verify accessibility and data.
Quick validation before running full ingestion.
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from maxio_client import MaxioClient

def test_all_endpoints():
    """
    Test each of the 7 endpoints.
    """
    print("\n" + "="*80)
    print("🚀 MAXIO API - 7 ENDPOINTS TEST")
    print("="*80)
    print("Testing all Maxio API endpoints for accessibility and data\n")
    
    # Define the 7 endpoints to test
    endpoints_to_test = [
        ('customers', 'Customers'),
        ('transactions', 'Transactions'),
        ('invoices', 'Invoices'),
        ('payments', 'Payments'),
        ('revenue_entries', 'Revenue Entries'),
        ('reports', 'Reports'),
        ('expenses', 'Expenses'),
    ]
    
    try:
        # Initialize client
        client = MaxioClient()
        print("✅ MaxioClient initialized successfully\n")
    except Exception as e:
        print(f"❌ Failed to initialize client: {e}")
        return
    
    results = {}
    
    # Test each endpoint
    for endpoint_key, endpoint_name in endpoints_to_test:
        try:
            print(f"📊 Testing {endpoint_name}...")
            print(f"   -" * 40)
            
            # Get the appropriate method
            method_name = f"get_{endpoint_key}"
            method = getattr(client, method_name)
            
            # Call the method
            df = method()
            
            if df is not None and not df.empty:
                record_count = len(df)
                column_count = len(df.columns)
                
                results[endpoint_key] = {
                    'status': 'SUCCESS',
                    'records': record_count,
                    'columns': column_count,
                    'sample_columns': list(df.columns)[:3]
                }
                
                print(f"   ✅ SUCCESS")
                print(f"   Records: {record_count}")
                print(f"   Columns: {column_count}")
                print(f"   Sample columns: {list(df.columns)[:3]}\n")
            else:
                results[endpoint_key] = {
                    'status': 'NO_DATA',
                    'records': 0
                }
                print(f"   ⚠️  No data found (endpoint may be empty)")
                print(f"   Records: 0\n")
                
        except Exception as e:
            results[endpoint_key] = {
                'status': 'ERROR',
                'error': str(e)
            }
            print(f"   ❌ ERROR: {str(e)[:100]}\n")
    
    # Print summary
    print("\n" + "="*80)
    print("📋 TEST SUMMARY")
    print("="*80)
    
    successful = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
    no_data = sum(1 for r in results.values() if r['status'] == 'NO_DATA')
    errors = sum(1 for r in results.values() if r['status'] == 'ERROR')
    total_records = sum(r.get('records', 0) for r in results.values())
    
    print(f"\nEndpoints Tested: {len(results)}")
    print(f"✅ Successful: {successful}")
    print(f"⚠️  No Data: {no_data}")
    print(f"❌ Errors: {errors}")
    print(f"📊 Total Records Accessible: {total_records:,}")
    
    print("\nDetailed Results:")
    print("-" * 80)
    
    for endpoint_key, result in results.items():
        endpoint_name = [name for key, name in endpoints_to_test if key == endpoint_key][0]
        status = result.get('status')
        
        if status == 'SUCCESS':
            records = result.get('records', 0)
            columns = result.get('columns', 0)
            print(f"✅ {endpoint_name:<20} | Records: {records:>8,} | Columns: {columns:>3}")
        elif status == 'NO_DATA':
            print(f"⚠️  {endpoint_name:<20} | No data found")
        else:
            error = result.get('error', 'Unknown error')[:50]
            print(f"❌ {endpoint_name:<20} | Error: {error}")
    
    print("\n" + "="*80)
    print("✨ Test Complete!")
    print("="*80 + "\n")
    
    if errors == 0:
        print("✅ Ready to run full ingestion: python src/azure_ingest_maxio.py\n")
    else:
        print("⚠️  Some endpoints failed. Check errors above before running ingestion.\n")


if __name__ == "__main__":
    test_all_endpoints()
