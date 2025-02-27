import os
import io
import boto3
import pandas as pd
import concurrent.futures

from datetime import timedelta
from botocore.client import Config
from botocore.exceptions import ClientError, BotoCoreError

from app_logger import get_logger
from settings import element_params, S3_CONFIG, S3_BUCKET, S3_BUCKET_DIRECT
from api_element import ElementClient

logger = get_logger(__name__)


class S3Client:
    def __init__(self, config: dict):
        config.update({
            "region_name": "ru1",
            "endpoint_url": "https://s3.timeweb.cloud",
            "config": Config(
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5},
                max_pool_connections=50
            )
        })
        self.s3_client = boto3.client("s3", **config)

    def upload_from_buffer(self, bucket: str, key: str, data: str) -> bool:
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType="text/csv"
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Upload failed: {e}")
            return False

    def upload_parallel(self, bucket: str, files: list, prefix: str = "") -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for file_path in files:
                key = os.path.basename(file_path)
                if prefix:
                    key = f"{prefix}/{key}"
                with open(file_path, "rb") as data:
                    future = executor.submit(
                        self.s3_client.put_object,
                        Bucket=bucket,
                        Key=key,
                        Body=data.read(),
                        ContentType="text/csv"
                    )
                    futures.append(future)
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error: {e}")


def main() -> None:
    login, password, base_url = element_params.values()
    s3 = S3Client(S3_CONFIG)
    element_client = ElementClient(login, password, base_url)
    data_folder = os.path.join(os.getcwd(), "data")
    os.makedirs(data_folder, exist_ok=True)
    
    timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    start_date = (pd.Timestamp.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    def upload_df(df, prefix, bucket=S3_BUCKET):
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        key = f"data/{prefix}_{timestamp}.csv"
        success = s3.upload_from_buffer(bucket, key, buffer.getvalue())
        logger.info(f"Upload {'success' if success else 'failed'}: {key}")

    try:
        # Drivers
        drivers = element_client.list_drivers()
        drivers_df = pd.DataFrame(drivers)
        drivers_df["timestamp"] = timestamp        
        upload_df(drivers_df, "drivers")
        
        # Phones
        phones = drivers_df["PhoneNumber"].astype(str).str.replace(r"\D+", "", regex=True)
        valid_phones = phones[phones.str.len().eq(11)].to_frame()
        phone_buffer = io.StringIO()
        valid_phones.to_csv(phone_buffer, index=False, header=False)
        phone_buffer.seek(0)
        s3.upload_from_buffer(S3_BUCKET_DIRECT, f"phones_{timestamp}.csv", phone_buffer.getvalue())
        
        # Cars
        cars_df = pd.DataFrame(element_client.list_cars())
        cars_df["timestamp"] = timestamp
        upload_df(cars_df, "cars")
        
        # Companies
        companies_df = pd.DataFrame(element_client.list_companies())
        companies_df["timestamp"] = timestamp
        upload_df(companies_df, "companies")
        
        # Payments
        payments = element_client.fetch_daily_payments(start_date, pd.Timestamp.now().strftime("%Y-%m-%d"))
        payments_df = pd.DataFrame(payments)
        payments_df["timestamp"] = timestamp
        upload_df(payments_df, f'payments_{start_date.replace("-", "")}')
        
        # Contracts
        valid_drivers = drivers_df[
            (drivers_df["PhoneNumber"].str.strip() != "") &
            (drivers_df["DriversLicenseSerialNumber"].str.strip() != "") &
            (drivers_df["DefaultID"].str.strip() != "")
        ]
        contracts = element_client.fetch_all_contracts(valid_drivers["ID"].tolist())
        contracts_df = pd.DataFrame(contracts)
        contracts_df["timestamp"] = timestamp
        upload_df(contracts_df, "contracts")

        # Parallel upload
        files_to_upload = [
            os.path.join(data_folder, f) 
            for f in os.listdir(data_folder) 
            if f.endswith(".csv")
        ]
        s3.upload_parallel(S3_BUCKET, files_to_upload, prefix="data/")
        
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
