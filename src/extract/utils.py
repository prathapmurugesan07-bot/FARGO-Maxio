import logging
import os
from typing import Any, Dict, Tuple

from dotenv import load_dotenv

from .maxio_client import MaxioClient


load_dotenv()

ENDPOINTS = [
    {
        "key": "customers",
        "title": "FETCHING CUSTOMERS FROM MAXIO",
        "label": "Customers Data",
        "method_name": "get_customers",
    },
    {
        "key": "transactions",
        "title": "FETCHING TRANSACTIONS FROM MAXIO",
        "label": "Transactions Data",
        "method_name": "get_transactions",
    },
    {
        "key": "invoices",
        "title": "FETCHING INVOICES FROM MAXIO",
        "label": "Invoices Data",
        "method_name": "get_invoices",
    },
    {
        "key": "payments",
        "title": "FETCHING PAYMENTS FROM MAXIO",
        "label": "Payments Data",
        "method_name": "get_payments",
    },
    {
        "key": "revenue_entries",
        "title": "FETCHING REVENUE ENTRIES FROM MAXIO",
        "label": "Revenue Entries Data",
        "method_name": "get_revenue_entries",
    },
    {
        "key": "reports",
        "title": "FETCHING REPORT DEFINITIONS FROM MAXIO",
        "label": "Reports Data",
        "method_name": "get_reports",
    },
    {
        "key": "expenses",
        "title": "FETCHING EXPENSES FROM MAXIO",
        "label": "Expenses Data",
        "method_name": "get_expenses",
    },
]


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(level=level, format="%(levelname)s:%(name)s:%(message)s")
    return logging.getLogger(__name__)


def print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def load_maxio_settings() -> Dict[str, Any]:
    return {
        "maxio_api_token": os.getenv("MAXIO_API_TOKEN"),
        "maxio_url": os.getenv("MAXIO_URL"),
        "maxio_username": os.getenv("MAXIO_USERNAME"),
        "maxio_password": os.getenv("MAXIO_PASSWORD"),
    }


def validate_maxio_settings(settings: Dict[str, Any]) -> None:
    if not settings["maxio_url"]:
        raise ValueError("Missing environment variable: MAXIO_URL")

    if not settings["maxio_api_token"] and not (
        settings["maxio_username"] and settings["maxio_password"]
    ):
        raise ValueError(
            "Set MAXIO_API_TOKEN or MAXIO_USERNAME/MAXIO_PASSWORD in the environment."
        )


def create_maxio_client(settings: Dict[str, Any]) -> MaxioClient:
    return MaxioClient(
        api_token=settings["maxio_api_token"],
        username=settings["maxio_username"],
        password=settings["maxio_password"],
        base_url=settings["maxio_url"],
    )


def create_result(
    status: str = "PENDING",
    records: int = 0,
    columns: int = 0,
    blob_name: str = "",
    error: str = "",
) -> Dict[str, Any]:
    return {
        "status": status,
        "records": records,
        "columns": columns,
        "blob_name": blob_name,
        "error": error,
    }


def fetch_endpoint_result(
    client: MaxioClient,
    config: Dict[str, str],
) -> Tuple[Any, Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    endpoint_key = config["key"]
    fetch_method = getattr(client, config["method_name"])

    try:
        dataframe = fetch_method()
    except Exception as exc:
        logger.error("Failed to fetch %s: %s", endpoint_key, exc)
        return None, create_result(status="FETCH_FAILED", error=str(exc))

    if dataframe is None or dataframe.empty:
        logger.warning("%s: no data returned from Maxio", config["label"])
        return dataframe, create_result(status="NO_DATA")

    logger.info("Fetched %s rows for %s", len(dataframe), endpoint_key)
    return dataframe, create_result(
        status="SUCCESS",
        records=len(dataframe),
        columns=len(dataframe.columns),
    )


def run_client_test() -> None:
    configure_logging()
    settings = load_maxio_settings()
    validate_maxio_settings(settings)

    print_section("INITIALIZING MAXIO API CLIENT")
    client = create_maxio_client(settings)
    print("Maxio client initialized successfully")

    results: Dict[str, Dict[str, Any]] = {}

    for config in ENDPOINTS:
        print_section(config["title"])
        dataframe, result = fetch_endpoint_result(client, config)
        results[config["key"]] = result

        if result["status"] == "SUCCESS":
            print(f"Records: {result['records']}")
            print(f"Columns: {result['columns']}")
            print(f"Sample columns: {list(dataframe.columns[:3])}")
        elif result["status"] == "NO_DATA":
            print("Records: 0")
        else:
            print(f"Error: {result['error'][:120]}")

    print_client_test_summary(results)


def print_client_test_summary(results: Dict[str, Dict[str, Any]]) -> None:
    success_count = sum(1 for result in results.values() if result["status"] == "SUCCESS")
    no_data_count = sum(1 for result in results.values() if result["status"] == "NO_DATA")
    failed_count = sum(1 for result in results.values() if result["status"] == "FETCH_FAILED")
    total_records = sum(result["records"] for result in results.values())

    print_section("CLIENT TEST COMPLETE - SUMMARY")
    print(f"Endpoints tested: {len(results)}")
    print(f"Successful endpoints: {success_count}")
    print(f"No data endpoints: {no_data_count}")
    print(f"Failed endpoints: {failed_count}")
    print(f"Total accessible records: {total_records:,}")
    print("\nTEST RESULTS:")
    print("-" * 80)

    for endpoint_key, result in results.items():
        details = [result["status"]]
        if result["records"]:
            details.append(f"{result['records']} records")
        if result["columns"]:
            details.append(f"{result['columns']} columns")
        if result["error"]:
            details.append(result["error"][:120])

        endpoint_name = endpoint_key.replace("_", " ").title()
        print(f"  {endpoint_name:<18} | " + " | ".join(details))

    print("-" * 80)
    print("\nRun ingestion with: python src/load/azure_ingest_maxio.py\n")
