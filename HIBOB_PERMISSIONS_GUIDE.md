# HiBob API Service User - Permissions Configuration Guide

## Current Status
- ✅ **People Search API** - WORKING (48 employees fetched)
- ❌ **Company Endpoints** - NOT AUTHORIZED (no permissions)

## Why Only People Search Works

Your service user (ID: `SERVICE-33880`) was created successfully, BUT **by default, service users have NO access permissions**.

According to HiBob's API documentation, the People Search endpoint works because your admin likely assigned the **"Default Employee Fields"** permission, which includes:
- Basic employee data (firstName, surname, email)
- Root category
- About section
- Employment data
- Work information

## What Your Admin Needs to Do

To access company-level endpoints (departments, teams, holidays, leave types, working hours), your HiBob administrator must:

### Step 1: Create a Permission Group
1. Go to **Service Users** configuration in HiBob
2. Click **Create Permission Group** (or edit existing one)
3. Name it (e.g., "API - Data Integration")

### Step 2: Add Your Service User to the Group
1. In the permission group, add `SERVICE-33880` to the group members

### Step 3: Configure Feature Permissions
Enable these features in the permission group:
- ✅ **Company Management** (for departments, teams, holidays)
- ✅ **Time Off** (for leave types, leave requests)
- ✅ **Attendance** (for attendance data, working hours)
- ✅ **People Management** (already likely enabled)

### Step 4: Configure People's Data Permissions
For each feature enabled above, grant:
- ☑️ **View all employees' [Feature] section**
- ☑️ **View all employees' [Feature] section histories** (if you need historical data)

### Step 5: Set Access Rights
1. Go to **People's data** tab
2. Click **Access data for**
3. Select **Everyone** (or specific conditions if needed)
4. This grants access to all active employees

### Step 6: Test the API
After permissions are configured, test with:
```bash
cd /Users/apple/Downloads/FARGO/src
python hibob_test.py
```

## Endpoints Requiring Permissions

| Endpoint | Feature | Current Status | Required Permission |
|----------|---------|-----------------|-------------------|
| `/people/search` | People Management | ✅ Working | Default Employee Fields |
| `/company/departments` | Company Management | ❌ Restricted | Company - View |
| `/company/teams` | Company Management | ❌ Restricted | Company - View |
| `/company/holidays` | Company Management | ❌ Restricted | Company - View |
| `/company/leave-types` | Time Off | ❌ Restricted | Time Off - View |
| `/timeoff/requests` | Time Off | ❌ Restricted | Time Off - View |
| `/company/working-hours` | Attendance | ❌ Restricted | Attendance - View |
| `/attendance` | Attendance | ❌ Restricted | Attendance - View |

## Verification Checklist

After admin configures permissions, verify:
- [ ] Permission Group created
- [ ] SERVICE-33880 added to group
- [ ] Company Management feature enabled
- [ ] Time Off feature enabled
- [ ] Attendance feature enabled
- [ ] "View all employees" permissions granted
- [ ] Access rights set to "Everyone"
- [ ] Test runs `python hibob_test.py` successfully

## Troubleshooting

### Still seeing "Customer login" page?
- Confirm permissions were saved in the permission group
- Check that SERVICE-33880 is assigned to the group
- Wait 5-10 minutes for permissions to sync

### Still seeing "404 Not Found"?
- The endpoint might not exist for your account
- Contact HiBob support to confirm available endpoints

### Still getting empty data?
- API returns 200 but empty response
- Check Access rights → "Access data for" is set correctly
- Ensure "Everyone" or your employee filter includes the data

## Next Steps

1. **Share this with your HiBob Admin:**
   - Ask them to configure permissions following Steps 1-5 above
   - Provide them with Service User ID: `SERVICE-33880`
   - Request permissions for: People, Company Management, Time Off, Attendance

2. **Once permissions are granted:**
   - Run `python hibob_test.py` to verify
   - Run `python azure_ingest.py` to ingest data to Azure

3. **For ongoing support:**
   - HiBob API Docs: https://apidocs.hibob.com/
   - Service User Guide: https://apidocs.hibob.com/docs/api-service-users
   - Support: Contact HiBob administrator or support team

## Current Code Status

Your code is **correctly implemented** and ready to use. The issue is purely a **permissions configuration** on the HiBob side:

- ✅ Proper authentication headers
- ✅ Correct API endpoints
- ✅ Error handling for restricted endpoints
- ✅ Azure Blob Storage integration
- ⏳ Waiting for admin to grant company-level permissions

Once your admin configures the permissions, your data ingestion pipeline will work seamlessly!
