import pandas as pd

from requests.sessions import Session
from datetime import timedelta
from contextlib import contextmanager
from urllib.parse import urljoin


class ElementClient:
    def __init__(
            self, element_username: str, element_password: str,
            element_base_url: str) -> None:
        """
        Initialize the Element client with the given parameters

        :param element_username: Element service username
        :param element_password: Element service password
        :param element_base_url: Element service base URL
        """
        self.username = element_username
        self.password = element_password
        self.base_url = element_base_url

    @contextmanager
    def get_session(self):
        """Context manager to interact with Element client"""
        element_session = Session()
        element_session.auth = (self.username, self.password)
        element_session.headers.update({'Content-Type': 'application/json'})
        try:
            yield element_session
        finally:
            element_session = None

    def list_cars(self) -> list:
        """
        Return list of cars from Element service
        """
        url = urljoin(self.base_url, 'Car/v1/Get')
        with self.get_session() as session:
            response = session.get(url, stream=True)
            response.raise_for_status()
            cars = response.json()
        return cars

    def list_drivers(
            self, ID: str = None, FIO: str = None,
            PhoneNumber: str = None, Status: str = None,
            DetailBalance: bool = False,
            FireDateFrom: str = None,
            FireDateTo: str = None) -> list:
        """
        Return list of drivers from Element service

        Filters:
            driver_id: Driver ID
            full_name: Driver full name
            phone_number: Driver phone number
            status: Driver status (Временно не работает, Кандидат, Отказ СБ, Работает, Уволен)
            include_balance_details: Include driver balance details in response
            dismissal_date_from: Driver dismissal date from
            dismissal_date_to: Driver dismissal date to

        """
        url = urljoin(self.base_url, 'Driver/v1/Get')
        args = locals()
        del args['self']
        del args['url']
        payload = {key: value for key, value in args.items() if value} 
        with self.get_session() as session:
            response = session.post(url, json=payload, stream=True)
            response.raise_for_status()
            drivers = response.json()
        return drivers

    def list_companies(self) -> list:
        """
        Return list of companies from Element service
        """
        url = urljoin(self.base_url, 'Company/v1/Get')
        with self.get_session() as session:
            response = session.get(url, stream=True)
            response.raise_for_status()
            companies = response.json()
        return companies

    def fetch_daily_payments(self, start_date: str, end_date: str) -> list:
        """
        Return list of daily payments from Element service

        Args:
            start_date: str = 'yyyy-mm-dd'  # or 'yyyy-mm-ddThh:mm:ss' format
            end_date: str = 'yyyy-mm-dd'  # or 'yyyy-mm-ddThh:mm:ss' format

        Returns:
            list: List of daily payments
        """
        url = urljoin(self.base_url, 'Reports/GetDocuments')
        payload = {
            'ДатаНачала': start_date,
            'ДатаОкончания': end_date
        }
        with self.get_session() as session:
            response = session.post(url, json=payload, stream=True)
            response.raise_for_status()
            payments = response.json()
        return payments

    def fetch_payments_for_period(
            self, start_date: str, end_date: str) -> list | None:
        """
        Fetch all daily payments for the given period

        Args:
            start_date: str = 'yyyy-mm-dd' format
            end_date: str = 'yyyy-mm-dd' format
        Returns:
            list | None
        """
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        payments = []
        for date in dates:
            start = date.strftime('%Y-%m-%d')
            end = (date + timedelta(days=1)).strftime('%Y-%m-%d')
            daily_payments = self.fetch_daily_payments(start, end)
            payments.extend(daily_payments)
        return payments if payments else None

    def fetch_contracts_by_driver_id(self, driver_id: str) -> list:
        """
        Return list of contracts by driver from Element service
        """
        url = urljoin(self.base_url, 'Driver/GetContractsDriver')
        with self.get_session() as session:
            response = session.post(url, json={'ID': driver_id}, stream=True)
            response.raise_for_status()
            contracts = response.json()
        return contracts

    def fetch_all_contracts(self, drivers_ids: list) -> list:
        """
        Return list of all contracts for all drivers from Element service
        """
        contracts = []
        for num, id in enumerate(drivers_ids, start=1):
            # print(f'Fetching contracts for driver {num}/{len(drivers_ids)}...')
            driver_contracts = self.fetch_contracts_by_driver_id(id)
            if driver_contracts:
                contracts_with_id = [
                    {'DriverID': id, **contract}
                    for contract in driver_contracts
                ]
                contracts.extend(contracts_with_id)
                # print(f'Found {len(driver_contracts)} contracts.')
        return contracts


# Extract all payments
# logger.info('Fetching all payments...')
# start_time = time.time()
# start_date = '2024-09-01'
# end_date = '2024-09-30'
# payments = client.fetch_all_payments(start_date, end_date)
# df_payments = pd.DataFrame(payments)
# df_payments['timestamp'] = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
# filename = os.path.join(target_folder, f'payments_{start_date}_{timestamp}.csv')
# df_payments.to_csv(filename, index=False, encoding='utf-8')
