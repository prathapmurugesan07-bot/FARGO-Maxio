import base64
import logging
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


class MaxioClient:
    """Client for extracting data from Maxio."""

    def __init__(
        self,
        api_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        api_token = api_token or os.getenv("MAXIO_API_TOKEN")
        username = username or os.getenv("MAXIO_USERNAME")
        password = password or os.getenv("MAXIO_PASSWORD")
        base_url = base_url or os.getenv("MAXIO_URL") or "https://e36.platform.maxio.com/fargosystems"

        self.base_url = base_url.rstrip("/") + "/api/v1.0"
        self.session = requests.Session()

        if api_token:
            logger.info("Using token-based authentication")
            self.session.headers.update(
                {
                    "Authorization": f"Token {api_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )
        elif username and password:
            logger.info("Using basic authentication")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            self.session.headers.update(
                {
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )
        else:
            raise ValueError(
                "Either MAXIO_API_TOKEN or MAXIO_USERNAME/MAXIO_PASSWORD must be provided."
            )

        logger.info("Initialized Maxio client for %s", self.base_url)

    def _request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        retries: int = 3,
    ) -> Any:
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint

        url = f"{self.base_url}{endpoint}"

        for attempt in range(retries):
            try:
                logger.info(
                    "Attempt %s/%s %s %s",
                    attempt + 1,
                    retries,
                    method.upper(),
                    url,
                )

                if method.upper() == "GET":
                    response = self.session.get(url, params=params, timeout=30)
                elif method.upper() == "POST":
                    response = self.session.post(url, json=payload, params=params, timeout=30)
                else:
                    response = self.session.request(
                        method,
                        url,
                        json=payload,
                        params=params,
                        timeout=30,
                    )

                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning("Rate limited. Retrying in %ss", wait_time)
                    time.sleep(wait_time)
                    continue

                if response.status_code in (200, 201):
                    logger.info("Request succeeded with status %s", response.status_code)
                    return response.json() if response.text else {}

                if response.status_code == 401:
                    raise Exception(
                        f"Authentication failed [401]: {response.text[:200]}"
                    )

                if response.status_code == 404:
                    raise Exception(
                        f"Endpoint not found [404] for {endpoint}: {response.text[:200]}"
                    )

                error_message = response.text[:500] if response.text else "No error message"
                logger.error(
                    "Request failed with status %s: %s",
                    response.status_code,
                    error_message,
                )
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    logger.info("Retrying in %ss", wait_time)
                    time.sleep(wait_time)
                    continue
            except requests.exceptions.Timeout as exc:
                logger.error("Request timed out: %s", exc)
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise Exception(f"Timeout after {retries} retries") from exc
            except requests.exceptions.RequestException as exc:
                logger.error("Request failed: %s", exc)
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise Exception(f"Request failed after {retries} retries: {exc}") from exc

        raise Exception(f"Failed after {retries} retries for endpoint: {endpoint}")

    def paginate(
        self,
        endpoint: str,
        method: str = "GET",
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        page = 1

        while True:
            params = {"page": page}
            logger.info("Fetching page %s from %s", page, endpoint)
            response = self._request(method, endpoint, params=params)

            if isinstance(response, dict) and "results" in response:
                page_records = response.get("results", []) or []
                total_count = response.get("count", len(page_records))
            elif isinstance(response, list):
                page_records = response
                total_count = len(page_records)
            else:
                logger.warning(
                    "Unexpected response format for %s: %s",
                    endpoint,
                    type(response).__name__,
                )
                page_records = []
                total_count = len(records)

            if not page_records:
                logger.info("No more records on page %s", page)
                break

            logger.info("Found %s records on page %s", len(page_records), page)
            records.extend(page_records)

            if len(records) >= total_count:
                logger.info("Fetched all %s records", len(records))
                break

            if max_pages and page >= max_pages:
                logger.info("Reached max_pages limit of %s", max_pages)
                break

            page += 1

        logger.info("Total records fetched: %s", len(records))
        return records

    def _get_dataframe(
        self,
        endpoint: str,
        label: str,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        logger.info("Fetching %s from Maxio", label)
        records = self.paginate(endpoint)

        if not records:
            logger.warning("No %s found", label)
            return pd.DataFrame()

        dataframe = pd.json_normalize(records)

        if fields:
            available_fields = [field for field in fields if field in dataframe.columns]
            if available_fields:
                dataframe = dataframe[available_fields]

        logger.info("Fetched %s %s", len(dataframe), label)
        logger.info("Columns: %s", list(dataframe.columns)[:5])
        return dataframe

    def get_customers(self, fields: Optional[List[str]] = None) -> pd.DataFrame:
        return self._get_dataframe("customers", "customers", fields=fields)

    def get_subscriptions(self) -> pd.DataFrame:
        return self._get_dataframe("subscriptions", "subscriptions")

    def get_transactions(self) -> pd.DataFrame:
        return self._get_dataframe("transactions", "transactions")

    def get_invoices(self) -> pd.DataFrame:
        return self._get_dataframe("invoices", "invoices")

    def get_payments(self) -> pd.DataFrame:
        return self._get_dataframe("payments", "payments")

    def get_revenue_entries(self) -> pd.DataFrame:
        return self._get_dataframe("revenue_entries", "revenue entries")

    def get_reports(self) -> pd.DataFrame:
        return self._get_dataframe("reports/definitions", "report definitions")

    def get_expenses(self) -> pd.DataFrame:
        return self._get_dataframe("expenses", "expenses")