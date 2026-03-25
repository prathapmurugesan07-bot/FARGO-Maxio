import os
import re
import time
from datetime import datetime
from io import BytesIO
from typing import Any, Callable, Dict, List, Tuple

from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

from extract.utils import (
    ENDPOINTS,
    configure_logging,
    create_maxio_client,
    create_result,
    fetch_endpoint_result,
    load_maxio_settings,
    print_section,
    validate_maxio_settings,
)


load_dotenv()


BlobNameBuilder = Callable[[str, Dict[str, Any]], str]
DataFrameTransform = Callable[[Any], Any]


def load_azure_settings() -> Dict[str, Any]:
    return {
        "azure_account_name": os.getenv("AZURE_STORAGE_ACCOUNT"),
        "azure_account_key": os.getenv("AZURE_STORAGE_KEY"),
        "container_name": os.getenv("AZURE_CONTAINER_NAME"),
        "azure_max_block_size": int(os.getenv("AZURE_MAX_BLOCK_SIZE_MB", "4")) * 1024 * 1024,
        "azure_max_single_put_size": int(os.getenv("AZURE_MAX_SINGLE_PUT_SIZE_MB", "4")) * 1024 * 1024,
        "azure_upload_timeout": int(os.getenv("AZURE_UPLOAD_TIMEOUT_SEC", "900")),
        "azure_connection_timeout": int(os.getenv("AZURE_CONNECTION_TIMEOUT_SEC", "30")),
        "azure_read_timeout": int(os.getenv("AZURE_READ_TIMEOUT_SEC", "300")),
        "azure_upload_max_concurrency": int(os.getenv("AZURE_UPLOAD_MAX_CONCURRENCY", "4")),
        "azure_upload_retries": int(os.getenv("AZURE_UPLOAD_RETRIES", "3")),
    }


def load_ingestion_settings() -> Dict[str, Any]:
    settings = load_maxio_settings()
    settings.update(load_azure_settings())
    return settings


def validate_azure_settings(settings: Dict[str, Any]) -> None:
    required_vars = {
        "AZURE_STORAGE_ACCOUNT": settings["azure_account_name"],
        "AZURE_STORAGE_KEY": settings["azure_account_key"],
        "AZURE_CONTAINER_NAME": settings["container_name"],
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")


def validate_ingestion_settings(settings: Dict[str, Any]) -> None:
    validate_maxio_settings(settings)
    validate_azure_settings(settings)


def create_blob_service_client(settings: Dict[str, Any]) -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{settings['azure_account_name']}.blob.core.windows.net",
        credential=settings["azure_account_key"],
        max_block_size=settings["azure_max_block_size"],
        max_single_put_size=settings["azure_max_single_put_size"],
        retry_total=settings["azure_upload_retries"] + 2,
        retry_connect=settings["azure_upload_retries"],
        retry_read=settings["azure_upload_retries"],
        retry_status=settings["azure_upload_retries"],
        connection_timeout=settings["azure_connection_timeout"],
        read_timeout=settings["azure_read_timeout"],
    )


def create_run_context() -> Dict[str, Any]:
    started_at = datetime.now()
    return {
        "started_at": started_at,
        "year": started_at.strftime("%Y"),
        "month": started_at.strftime("%m"),
        "day": started_at.strftime("%d"),
        "run_time": started_at.strftime("%Y%m%d_%H%M%S"),
    }


def build_folder_prefix(endpoint_key: str, run_context: Dict[str, Any]) -> str:
    return (
        f"maxio/{endpoint_key}/"
        f"{run_context['year']}/{run_context['month']}/{run_context['day']}/"
    )


def build_blob_name(endpoint_key: str, run_context: Dict[str, Any]) -> str:
    return f"{build_folder_prefix(endpoint_key, run_context)}{endpoint_key}_{run_context['run_time']}.csv"


def build_staging_blob_name(endpoint_key: str, run_context: Dict[str, Any]) -> str:
    del run_context
    return f"{endpoint_key}.csv"


def build_folder_placeholder_name(endpoint_key: str, run_context: Dict[str, Any]) -> str:
    return f"{build_folder_prefix(endpoint_key, run_context)}_folder_placeholder"


def identity_dataframe_transform(dataframe):
    return dataframe


def normalize_staging_column_name(column_name: Any) -> str:
    normalized = str(column_name).replace(".", "_")
    normalized = re.sub(r"[^a-zA-Z0-9_]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.lower()


def transform_staging_dataframe(dataframe):
    staged_dataframe = dataframe.copy()
    staged_dataframe.columns = [
        normalize_staging_column_name(column_name)
        for column_name in staged_dataframe.columns
    ]
    return staged_dataframe


def ensure_folder_structure(
    blob_service_client: BlobServiceClient,
    container_name: str,
    endpoint_key: str,
    run_context: Dict[str, Any],
    settings: Dict[str, Any],
) -> bool:
    import logging

    logger = logging.getLogger(__name__)
    placeholder_blob = build_folder_placeholder_name(endpoint_key, run_context)

    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=placeholder_blob,
        )
        blob_client.upload_blob(
            b"",
            overwrite=True,
            timeout=settings["azure_upload_timeout"],
        )
        logger.info("Folder structure ready: %s", build_folder_prefix(endpoint_key, run_context))
        return True
    except Exception as exc:
        logger.error("Failed to create folder structure for %s: %s", endpoint_key, exc)
        return False


def upload_dataframe_to_azure(
    dataframe,
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    label: str,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    import logging

    logger = logging.getLogger(__name__)

    if dataframe is None or dataframe.empty:
        return create_result(status="NO_DATA")

    try:
        csv_bytes = dataframe.to_csv(index=False).encode("utf-8")
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name,
        )

        for attempt in range(1, settings["azure_upload_retries"] + 1):
            try:
                blob_client.upload_blob(
                    BytesIO(csv_bytes),
                    length=len(csv_bytes),
                    overwrite=True,
                    timeout=settings["azure_upload_timeout"],
                    max_concurrency=settings["azure_upload_max_concurrency"],
                    content_settings=ContentSettings(
                        content_type="text/csv; charset=utf-8"
                    ),
                )
                logger.info("%s uploaded to %s", label, blob_name)
                return create_result(
                    status="UPLOADED",
                    records=len(dataframe),
                    columns=len(dataframe.columns),
                    blob_name=blob_name,
                )
            except Exception as exc:
                if attempt < settings["azure_upload_retries"]:
                    logger.warning(
                        "%s upload attempt %s/%s failed: %s",
                        label,
                        attempt,
                        settings["azure_upload_retries"],
                        exc,
                    )
                    time.sleep(2 ** (attempt - 1))
                    continue
                raise exc
    except Exception as exc:
        logger.error("%s upload failed: %s", label, exc)
        return create_result(
            status="UPLOAD_FAILED",
            records=0 if dataframe is None else len(dataframe),
            columns=0 if dataframe is None else len(dataframe.columns),
            blob_name=blob_name,
            error=str(exc),
        )


def fetch_and_upload_endpoint(
    client,
    config: Dict[str, str],
    blob_service_client: BlobServiceClient,
    container_name: str,
    run_context: Dict[str, Any],
    settings: Dict[str, Any],
    blob_name_builder: BlobNameBuilder,
    create_folder_structure: bool,
    dataframe_transform: DataFrameTransform,
) -> Dict[str, Any]:
    import logging

    logger = logging.getLogger(__name__)
    endpoint_key = config["key"]

    if create_folder_structure:
        if not ensure_folder_structure(
            blob_service_client=blob_service_client,
            container_name=container_name,
            endpoint_key=endpoint_key,
            run_context=run_context,
            settings=settings,
        ):
            return create_result(
                status="UPLOAD_FAILED",
                error="Failed to create Azure folder structure placeholder.",
            )

    dataframe, result = fetch_endpoint_result(client, config)
    if result["status"] != "SUCCESS":
        return result

    try:
        dataframe = dataframe_transform(dataframe)
    except Exception as exc:
        logger.error("Failed to transform %s for upload: %s", endpoint_key, exc)
        return create_result(
            status="UPLOAD_FAILED",
            records=len(dataframe),
            columns=len(dataframe.columns),
            error=str(exc),
        )

    blob_name = blob_name_builder(endpoint_key, run_context)
    return upload_dataframe_to_azure(
        dataframe=dataframe,
        blob_service_client=blob_service_client,
        container_name=container_name,
        blob_name=blob_name,
        label=config["label"],
        settings=settings,
    )


def run_ingestion(
    client,
    blob_service_client: BlobServiceClient,
    settings: Dict[str, Any],
    blob_name_builder: BlobNameBuilder = build_blob_name,
    create_folder_structure: bool = True,
    dataframe_transform: DataFrameTransform = identity_dataframe_transform,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    run_context = create_run_context()
    results = {config["key"]: create_result() for config in ENDPOINTS}

    for config in ENDPOINTS:
        print_section(config["title"])
        results[config["key"]] = fetch_and_upload_endpoint(
            client=client,
            config=config,
            blob_service_client=blob_service_client,
            container_name=settings["container_name"],
            run_context=run_context,
            settings=settings,
            blob_name_builder=blob_name_builder,
            create_folder_structure=create_folder_structure,
            dataframe_transform=dataframe_transform,
        )

    return results, run_context


def build_hierarchical_destination_lines(
    results: Dict[str, Dict[str, Any]],
    run_context: Dict[str, Any],
) -> List[str]:
    lines = ["  maxio/"]
    for endpoint_key in results:
        lines.append(
            f"    {endpoint_key}/"
            f"{run_context['year']}/{run_context['month']}/{run_context['day']}/"
        )
    return lines


def build_staging_destination_lines(results: Dict[str, Dict[str, Any]]) -> List[str]:
    return [f"  {endpoint_key}.csv" for endpoint_key in results]


def print_summary(
    results: Dict[str, Dict[str, Any]],
    container_name: str,
    run_context: Dict[str, Any],
    destination_title: str,
    destination_lines: List[str],
) -> None:
    total_endpoints = len(results)
    uploaded_count = sum(1 for result in results.values() if result["status"] == "UPLOADED")
    no_data_count = sum(1 for result in results.values() if result["status"] == "NO_DATA")
    failed_count = sum(
        1
        for result in results.values()
        if result["status"] in {"FETCH_FAILED", "UPLOAD_FAILED"}
    )
    uploaded_records = sum(
        result["records"] for result in results.values()
        if result["status"] == "UPLOADED"
    )
    failed_upload_records = sum(
        result["records"] for result in results.values()
        if result["status"] == "UPLOAD_FAILED"
    )

    print_section("INGESTION COMPLETE - SUMMARY")
    print(f"Started at: {run_context['started_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data uploaded with timestamp: {run_context['run_time']}")
    print(f"Azure container: {container_name}")
    print("\nUPLOAD RESULTS:")
    print("-" * 80)

    for endpoint_key, result in results.items():
        details = [result["status"]]
        if result["records"]:
            details.append(f"{result['records']} records")
        if result["blob_name"]:
            details.append(result["blob_name"])
        if result["error"]:
            details.append(result["error"][:120])

        endpoint_name = endpoint_key.replace("_", " ").title()
        print(f"  {endpoint_name:<18} | " + " | ".join(details))

    print("-" * 80)
    print(f"Uploaded endpoints: {uploaded_count}/{total_endpoints}")
    print(f"No data endpoints: {no_data_count}")
    print(f"Failed endpoints: {failed_count}")
    print(f"Records ingested to Azure: {uploaded_records:,}")
    if failed_upload_records:
        print(f"Records fetched but not uploaded: {failed_upload_records:,}")

    print(f"\n{destination_title}:")
    for line in destination_lines:
        print(line)
    print("\n" + "=" * 80 + "\n")


def run_azure_ingestion() -> None:
    configure_logging()
    settings = load_ingestion_settings()
    validate_ingestion_settings(settings)

    print_section("INITIALIZING MAXIO API CLIENT")
    client = create_maxio_client(settings)
    print("Maxio client initialized successfully")

    print_section("INITIALIZING AZURE BLOB STORAGE CLIENT")
    blob_service_client = create_blob_service_client(settings)
    print("Azure Blob Storage client initialized successfully")

    results, run_context = run_ingestion(
        client=client,
        blob_service_client=blob_service_client,
        settings=settings,
    )
    print_summary(
        results,
        settings["container_name"],
        run_context,
        destination_title="FOLDER STRUCTURE CREATED IN AZURE",
        destination_lines=build_hierarchical_destination_lines(results, run_context),
    )


def run_azure_staging_ingestion() -> None:
    configure_logging()
    settings = load_ingestion_settings()
    settings["container_name"] = os.getenv("AZURE_STAGING_CONTAINER_NAME", "staging")
    validate_ingestion_settings(settings)

    print_section("INITIALIZING MAXIO API CLIENT")
    client = create_maxio_client(settings)
    print("Maxio client initialized successfully")

    print_section("INITIALIZING AZURE BLOB STORAGE CLIENT")
    blob_service_client = create_blob_service_client(settings)
    print("Azure Blob Storage client initialized successfully")

    results, run_context = run_ingestion(
        client=client,
        blob_service_client=blob_service_client,
        settings=settings,
        blob_name_builder=build_staging_blob_name,
        create_folder_structure=False,
        dataframe_transform=transform_staging_dataframe,
    )
    print_summary(
        results,
        settings["container_name"],
        run_context,
        destination_title="FILES WRITTEN TO AZURE STAGING",
        destination_lines=build_staging_destination_lines(results),
    )
