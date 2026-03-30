from __future__ import annotations

from datetime import datetime
from io import BytesIO
import os
import re

from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

from hibob_client import HiBobClient


load_dotenv()


DEFAULT_RAW_CONTAINER_NAME = "raw"
DEFAULT_HIBOB_CONTAINER_NAME = "hibob"


def normalize_column_name(column_name: object) -> str:
    normalized = str(column_name).replace(".", "_")
    normalized = re.sub(r"[^a-zA-Z0-9_]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.lower()


def normalize_dataframe_columns(dataframe) -> None:
    dataframe.columns = [normalize_column_name(column) for column in dataframe.columns]


def create_run_context(started_at: datetime | None = None) -> dict[str, str]:
    started = started_at or datetime.utcnow()
    return {
        "year": started.strftime("%Y"),
        "month": started.strftime("%m"),
        "day": started.strftime("%d"),
        "run_time": started.strftime("%Y%m%d_%H%M%S"),
    }


def build_raw_blob_name(run_context: dict[str, str]) -> str:
    return (
        f"hibob/employees_data/"
        f"{run_context['year']}/{run_context['month']}/{run_context['day']}/"
        f"employees_data_{run_context['run_time']}.csv"
    )


def upload_dataframe_to_blob(
    dataframe,
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    label: str,
) -> None:
    if dataframe is None or dataframe.empty:
        print(f"\n⚠️ {label} contains no records; skipping upload.")
        return

    csv_bytes = dataframe.to_csv(index=False).encode("utf-8")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(
        BytesIO(csv_bytes),
        length=len(csv_bytes),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )
    print(f"\n✅ Uploaded {label} to {container_name}: {blob_name}")


def get_container_name(env_var: str, description: str, default: str) -> str:
    name = os.getenv(env_var) or default
    if not name:
        raise ValueError(f"Missing environment variable for {description}.")
    return name


def main() -> None:
    hibob_service_user = os.getenv("HIBOB_SERVICE_USER")
    hibob_token = os.getenv("HIBOB_TOKEN")
    if not hibob_service_user or not hibob_token:
        raise ValueError("Missing HiBob credentials in environment.")

    raw_container = get_container_name(
        "AZURE_CONTAINER_NAME",
        "raw container",
        DEFAULT_RAW_CONTAINER_NAME,
    )
    hibob_container = get_container_name(
        "AZURE_CONTAINER_NAME_2",
        "HiBob container",
        DEFAULT_HIBOB_CONTAINER_NAME,
    )
    azure_account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    azure_account_key = os.getenv("AZURE_STORAGE_KEY")
    if not azure_account_name or not azure_account_key:
        raise ValueError("Missing Azure Storage credentials in environment.")

    client = HiBobClient(hibob_service_user, hibob_token)
    blob_service_client = BlobServiceClient(
        account_url=f"https://{azure_account_name}.blob.core.windows.net",
        credential=azure_account_key,
    )

    print("\n" + "=" * 80)
    print("FETCHING EMPLOYEE DATA FROM HIBOB")
    print("=" * 80)
    df_employees = client.get_all_employees(
        [
            "firstName",
            "surname",
            "email",
            "work.department",
            "work.title",
        ]
    )
    print(f"\nFetched {len(df_employees)} employees")
    print(df_employees.head())

    run_context = create_run_context()
    raw_blob_name = build_raw_blob_name(run_context)
    print("\n" + "=" * 80)
    print("UPLOADING RAW HIERARCHICAL FILE TO AZURE (RAW CONTAINER)")
    print("=" * 80)
    upload_dataframe_to_blob(
        df_employees,
        blob_service_client,
        raw_container,
        raw_blob_name,
        label="raw employee snapshot",
    )

    normalized_employees = df_employees.copy()
    normalize_dataframe_columns(normalized_employees)

    staging_blob_name = "employees_data/employees.csv"
    print("\n" + "=" * 80)
    print("UPLOADING NORMALIZED FILE TO AZURE (HIBOB CONTAINER)")
    print("=" * 80)
    upload_dataframe_to_blob(
        normalized_employees,
        blob_service_client,
        hibob_container,
        staging_blob_name,
        label="normalized employee snapshot",
    )


if __name__ == "__main__":
    main()
