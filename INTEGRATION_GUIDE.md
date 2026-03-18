# Maxio Integration & Azure Ingestion Guide

This directory contains integrated tools for testing Maxio API endpoints and ingesting data into Azure Blob Storage with standardized formatting.

## 📁 Files Overview

### Core Files
- **`test_all_endpoints_maxio.py`** - Test suite for all Maxio API endpoints
- **`src/azure_ingest_maxio.py`** - Complete Azure ingestion pipeline
- **`Maxio_Ingestion.ipynb`** - Interactive Jupyter notebook

### Existing Files
- `src/maxio_client.py` - Maxio API client with pagination
- `requirements.txt` - Python dependencies
- `.env` - Environment variables (not in repo)

## 🚀 Quick Start

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables
# Create .env file with:
MAXIO_API_TOKEN=your_token_here
AZURE_STORAGE_ACCOUNT=your_account
AZURE_STORAGE_KEY=your_key
AZURE_CONTAINER_NAME=your_container
```

### Test Endpoints
```bash
python test_all_endpoints_maxio.py                    # Test all endpoints
python test_all_endpoints_maxio.py customers          # Test individual endpoint
```

### Run Full Ingestion
```bash
python src/azure_ingest_maxio.py
```

### Run Interactive Notebook
```bash
jupyter notebook Maxio_Ingestion.ipynb
```

## 📊 Endpoints Supported

| Endpoint | Records Type | Date Columns | Folder |
|----------|--------------|--------------|--------|
| Customers | Customer data | created_at, updated_at | `maxio/customers/YYYY/MM/DD` |
| Subscriptions | Subscription data | created_at, updated_at, activated_at, cancelled_at | `maxio/subscriptions/YYYY/MM/DD` |
| Invoices | Invoice data | created_at, updated_at, issued_at, due_at | `maxio/invoices/YYYY/MM/DD` |

## 📅 Date Formatting

All date columns are automatically converted to **ISO 8601 format** with time:
```
YYYY-MM-DD HH:MM:SS

Example: 2024-03-18 14:30:45
```

## 📤 Azure Folder Structure

Files are organized as:
```
Container
├── maxio/
│   ├── customers/
│   │   └── YYYY/
│   │       └── MM/
│   │           └── DD/
│   │               └── customers_YYYYMMDD_HHMMSS.csv
│   ├── transactions/
│   │   └── YYYY/MM/DD/transactions_YYYYMMDD_HHMMSS.csv
│   └── invoices/
│       └── YYYY/MM/DD/invoices_YYYYMMDD_HHMMSS.csv
```

## 🔍 Test Endpoint Script Features

```
✅ Tests each endpoint individually
✅ Shows HTTP status codes
✅ Reports record counts
✅ Captures detailed error messages
✅ Shows sample fields from each endpoint
✅ Supports individual endpoint testing
```

## 🌐 Azure Ingestion Script Features

```
✅ Fetches all endpoints with pagination
✅ Standardizes date formatting
✅ Handles errors per endpoint (doesn't fail if one endpoint fails)
✅ Uploads to organized Azure folder structure by endpoint and date
✅ Detailed summary report with statistics
✅ Tracks record counts and file paths
✅ Logs all operations
```

## 📓 Jupyter Notebook Sections

1. **Import Libraries** - Load all required packages
2. **Configure Settings** - Set up API and Azure credentials
3. **Define Functions** - Date formatting and Azure upload functions
4. **Test Endpoints** - Initialize client and test all endpoints
5. **Format Data** - Convert to DataFrames with standardized dates
6. **Upload to Azure** - Upload each dataset with proper organization
7. **Summary Report** - Display results and sample data

## ⚠️ Error Handling

- Each endpoint is tested independently
- Failure in one endpoint doesn't stop others
- Detailed error messages for debugging
- Graceful degradation if endpoints are unavailable

## 🔧 Customization

### Add New Endpoint
1. Update `ENDPOINTS` list in scripts:
```python
{'key': 'new_endpoint', 'name': 'New Endpoint', 'folder': 'maxio/new_endpoint_data'}
```

2. Add date columns to `DATE_COLUMNS` if needed:
```python
'new_endpoint': ['created_at', 'updated_at']
```

### Change Date Format
Modify the `format_dates()` function in:
- `src/azure_ingest_maxio.py` (line ~71)
- `Maxio_Ingestion.ipynb` (cell 3)

Current format: `'%Y-%m-%d %H:%M:%S'`
Other options: `'%Y-%m-%d'`, `'%Y/%m/%d %H:%M:%S'`, etc.

## 📋 Configuration Matrix

| Setting | File Location | Environment Variable |
|---------|---------------|---------------------|
| Endpoints | All files | ENDPOINTS list |
| Date format | Functions | DATE_COLUMNS dict |
| Azure account | All files | AZURE_STORAGE_ACCOUNT |
| Container | All files | AZURE_CONTAINER_NAME |

## 🐛 Troubleshooting

### "Missing environment variables"
```bash
# Check .env file exists
ls -la .env

# Verify variables are set
echo $MAXIO_API_TOKEN
echo $AZURE_STORAGE_ACCOUNT
```

### "Authentication failed [401]"
```bash
# Verify Maxio token is valid
# Test in test_all_endpoints_maxio.py
```

### "Upload failed"
```bash
# Check Azure credentials
# Verify container exists
# Check storage account permissions
```

### "No records found"
```bash
# Normal if endpoint has no data
# Check API response in test script
```

## 📊 Expected Output

### Test Script Output
```
🚀 MAXIO API ENDPOINT TEST SUITE
────────────────────────────────────────────────
📊 Customers
   Endpoint: GET /customers
   ✅ Accessible - Records: 1234
   Sample fields: ['id', 'name', 'email']

📊 Subscriptions
   ✅ Accessible - Records: 5678

📋 SUMMARY REPORT
✅ Customers   | Records: 1234      | Status: SUCCESS
✅ Subscriptions | Records: 5678    | Status: SUCCESS
```

### Ingestion Script Output
```
🚀 MAXIO TO AZURE INGESTION PIPELINE
════════════════════════════════════════════════
✅ Maxio client initialized
📊 INGESTION SUMMARY
✅ customers        | Records: 1234    | Columns: 25
   Path: maxio/customers/2026/03/18/customers_20260318_153045.csv
```

## 📞 Support

For issues with:
- **Maxio API**: See `src/maxio_client.py`
- **Azure upload**: Check `AZURE_STORAGE_ACCOUNT` and credentials
- **Date formatting**: Check `DATE_COLUMNS` configuration
- **Endpoints**: Update endpoint list in script

---

**Last Updated:** March 18, 2024
**Integration Type:** Maxio API → Azure Blob Storage
**Format Standard:** ISO 8601 with time (YYYY-MM-DD HH:MM:SS)
