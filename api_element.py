import concurrent

import time
import pandas as pd

from requests import HTTPError, Session
from requests.exceptions import RequestException
from datetime import timedelta
from urllib.parse import urljoin

from app_logger import get_logger

logger = get_logger(__name__)


class ElementClient:
    def __init__(
        self, element_username: str, element_password: str,
        element_base_url: str, max_retries: int = 3,
        backoff_factor: float = 0.5, timeout: tuple = (5, 30)
    ) -> None:
        self.username = element_username
        self.password = element_password
        self.base_url = element_base_url
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor  # Экспоненциальная задержка между попытками
        self.timeout = timeout  # (connection_timeout, read_timeout)
        self._session = self._create_session()

    def _create_session(self) -> Session:
        session = Session()
        session.auth = (self.username, self.password)
        session.headers.update({"Content-Type": "application/json"})
        return session

    def _request(self, method, url, **kwargs):
        retries = 0
        kwargs.setdefault("timeout", self.timeout)
        while retries <= self.max_retries:
            try:
                response = self._session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except (RequestException, HTTPError) as e:
                retries += 1
                if retries > self.max_retries:
                    raise RequestException(f"Failed after {self.max_retries} retries") from e
                wait = self.backoff_factor * (2 ** (retries - 1))
                logger.warning(f"Request failed: {e}. Retrying in {wait:.1f}s...")
                time.sleep(wait)

    def list_cars(self) -> list:
        url = urljoin(self.base_url, "Car/v1/Get")
        response = self._request("GET", url)
        return response.json()

    def list_drivers(
        self, **filters
    ) -> list:
        url = urljoin(self.base_url, "Driver/v1/Get")
        payload = {k: v for k, v in filters.items() if v is not None}
        response = self._request("POST", url, json=payload)
        return response.json()

    def list_companies(self) -> list:
        url = urljoin(self.base_url, "Company/v1/Get")
        response = self._request("GET", url)
        return response.json()

    def fetch_daily_payments(
        self, start_date: str, end_date: str, page_size=1000, page=1
    ) -> list:
        url = urljoin(self.base_url, "Reports/GetDocuments")
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        if len(dates) <= 2:
            payload = {
                "ДатаНачала": start_date,
                "ДатаОкончания": end_date,
                "Page": page,
                "PageSize": page_size
            }
            response = self._request("POST", url, json=payload)
            return response.json()
        payments = []
        for date in dates:
            start = date.strftime("%Y-%m-%d")
            end = (date + timedelta(days=1)).strftime("%Y-%m-%d")
            payload = {
                "ДатаНачала": start,
                "ДатаОкончания": end,
                "Page": page,
                "PageSize": page_size
            }
            response = self._request("POST", url, json=payload)
            payments.extend(response.json())
        return payments if payments else None

    def fetch_all_contracts(
        self, drivers_ids: list, chunk_size=100
    ) -> list:
        contracts = []
        total = len(drivers_ids)
        for i in range(0, total, chunk_size):
            chunk = drivers_ids[i:i+chunk_size]
            responses = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(self._fetch_contracts_for_driver, driver_id)
                    for driver_id in chunk
                ]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        responses.append(future.result())
                    except Exception as e:
                        logger.error(f"Error fetching contracts: {e}")

            for driver_id, contracts_data in zip(chunk, responses):
                if contracts_data:
                    contracts.extend([{
                        "DriverID": driver_id,
                        **contract
                    } for contract in contracts_data])
            logger.info(f"Processed {i+chunk_size}/{total} drivers")

        return contracts

    def _fetch_contracts_for_driver(self, driver_id: str) -> list:
        url = urljoin(self.base_url, "Driver/GetContractsDriver")
        try:
            response = self._request("POST", url, json={"ID": driver_id})
            return response.json()
        except RequestException as e:
            logger.error(f"Failed to fetch contracts for {driver_id}: {e}")
            return []
