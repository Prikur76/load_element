
import os
import requests
import pandas as pd

from datetime import timedelta

from app_logger import get_logger
from settings import element_params, S3_CONFIG, S3_BUCKET
from api_element import ElementClient
from api_s3 import S3Client


logger = get_logger(__name__)


def main() -> None:
    """Extract all data from Element service."""

    # Get Element service credentials and base URL from environment variables
    login, password, base_url = element_params.values()

    # Create Element client
    element_client = ElementClient(login, password, base_url)

    # Target directory for CSV files
    data_folder = os.path.join(os.getcwd(), 'data')
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # try:
    #     # Extract cars
    #     cars_df = pd.DataFrame(element_client.list_cars())
    #     timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    #     cars_df['timestamp'] = timestamp
    #     cars_filename = os.path.join(data_folder, f'cars_{timestamp}.csv')
    #     cars_df.to_csv(cars_filename, index=False, encoding='utf-8')

    #     # Extract drivers
    #     drivers_df = pd.DataFrame(element_client.list_drivers())
    #     timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    #     drivers_df['timestamp'] = timestamp
    #     drivers_filename = os.path.join(
    #         data_folder, f'drivers_{timestamp}.csv')
    #     drivers_df.to_csv(drivers_filename, index=False, encoding='utf-8')

    #     # Extract companies
    #     companies_df = pd.DataFrame(element_client.list_companies())
    #     timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    #     companies_df['timestamp'] = timestamp
    #     companies_filename = os.path.join(
    #         data_folder, f'companies_{timestamp}.csv')
    #     companies_df.to_csv(companies_filename, index=False, encoding='utf-8')

    #     # Extract daily payments
    #     logger.info('Fetching daily payments...')
    #     start_date = (pd.Timestamp.now() - timedelta(days=1))\
    #         .strftime("%Y-%m-%d")
    #     end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    #     payments = element_client.fetch_daily_payments(start_date, end_date)
    #     payments_df = pd.DataFrame(payments)
    #     timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    #     payments_df['timestamp'] = timestamp
    #     payments_filename = os.path.join(
    #         data_folder, f'payments_{start_date}_{timestamp}.csv')
    #     payments_df.to_csv(payments_filename, index=False, encoding='utf-8')

    #     # Extract all contracts
    #     drivers = element_client.list_drivers()
    #     drivers_ids = [
    #         driver['ID'] for driver in drivers
    #         if driver['PhoneNumber'].strip() and
    #         driver['DriversLicenseSerialNumber'].strip() and
    #         driver['DefaultID'].strip()
    #     ]
    #     contracts = element_client.fetch_all_contracts(drivers_ids)
    #     contracts_df = pd.DataFrame(contracts)
    #     timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    #     contracts_df['timestamp'] = timestamp
    #     contracts_filename = os.path.join(
    #         data_folder, f'contracts_{timestamp}.csv')
    #     contracts_df.to_csv(contracts_filename, index=False, encoding='utf-8')
    #     logger.info('Data extraction completed successfully.')

    #     # Upload data to S3
    #     s3 = S3Client(S3_CONFIG)
    #     bucket = S3_BUCKET
    #     for root, dirs, files in os.walk(data_folder):
    #         for file in files:
    #             file_path = os.path.join(root, file)
    #             prefix = 'data/'
    #             uploaded = s3.upload_object(bucket, file_path, prefix)
    #             message = f'file: {file}, uploaded: {uploaded}'
    #             logger.info(message)
    #     logger.info('Data uploaded to S3 successfully.')
    # except requests.exceptions.RequestException as e:
    #     logger.error(f'Request error: {e}')
    # except Exception as e:
    #     logger.error(f'Error: {e}')


if __name__ == '__main__':
    main()
