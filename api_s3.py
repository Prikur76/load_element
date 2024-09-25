"""
https://yandex.cloud/ru/docs/storage/tools/boto
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
!pip install boto3
"""

import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from contextlib import contextmanager

from app_logger import get_logger

logger = get_logger(__name__)


class S3Client:
    def __init__(self, config: dict):
        """
        Initialize the S3 client with configuration

        :param config: Configuration for the S3 client
        """
        self.config = config
        self.config['config'] = Config(s3={'addressing_style': 'path'})

    @contextmanager
    def get_client(self):
        """Context manager to interact with S3 client"""
        s3_client = boto3.client('s3', **self.config)
        try:
            yield s3_client
        finally:
            s3_client = None

    def create_bucket(self, bucket_name: str) -> bool:
        """Create an S3 bucket

        Args:
            bucket_name (str): Bucket to create

        Returns:
            bool: True if bucket created, else False
        """
        try:
            with self.get_client() as s3_client:
                s3_client.create_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(f'Error creating bucket: {e}')
            return False
        return True

    def list_buckets(self) -> list:
        """Get list of buckets."""
        buckets = []
        with self.get_client() as client:
            response = client.list_buckets()
            if 'Buckets' in response:
                buckets = [bucket['Name'] for bucket in response['Buckets']]
        return buckets

    def delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete an S3 bucket.

        Args:
            bucket_name (str): Name of bucket to delete

        Returns:
            bool: True if bucket deleted, else False
        """
        try:
            with self.get_client() as client:
                client.delete_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(f'Error deleting bucket {bucket_name}: {e}')
            return False
        return True

    def list_objects(self, bucket_name: str) -> list:
        """Get list of objects in the specified S3 bucket."""
        objects = []
        with self.get_client() as client:
            response = client.list_objects(Bucket=bucket_name)
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
        return objects

    def get_object_metadata(self, bucket_name: str, obj_name: str) -> dict:
        """Get metadata for the specified object."""
        metadata = {}
        try:
            with self.get_client() as client:
                metadata = client.head_object(Bucket=bucket_name, Key=obj_name)
        except Exception as e:
            logger.error(f'Error getting metadata for {obj_name}: {e}')
        return metadata

    def create_object(
            self, bucket_name: str, object_name: str, content: str) -> bool:
        """
        Create an S3 object.

        Args:
            bucket_name (str): Bucket to create object in.
            object_name (str): Name of object.
            content (str): Object content.

        Returns:
            bool: True if object created, else False.
        """
        try:
            client = self.get_client()
            client.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=content)
        except Exception as e:
            logger.error(f"Error creating object {object_name}: {e}")
            return False
        return True

    def upload_object(
            self, bucket_name: str, file_path: str, prefix: str = '') -> bool:
        """
        Upload a file to an S3 bucket.

        Args:
            bucket_name (str): Bucket to upload to.
            file_path (str): Local path to file.

        Returns:
            bool: True if file was uploaded, else False.
        """
        object_name = os.path.basename(file_path)
        try:
            with self.get_client() as client, open(file_path, 'rb') as file:
                if prefix:
                    object_name = f'{prefix}{object_name}'
                client.upload_fileobj(file, bucket_name, object_name)
        except Exception as e:
            logger.error(f'Error uploading {object_name}: {e}')
            return False
        return True

    def copy_object(self, source_bucket: str, dest_bucket: str,
                    source_key: str, dest_key: str) -> bool:
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        try:
            with self.get_client() as client:
                client.copy_object(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    CopySource=copy_source
                )
        except Exception as e:
            logger.error(f'Error copying object: {e}')
            return False
        return True

    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete an S3 object.

        Args:
            bucket_name (str): Bucket to delete object in.
            object_name (str): Name of object.

        Returns:
            bool: True if object deleted, else False.
        """
        try:
            with self.get_client() as client:
                client.delete_object(Bucket=bucket_name, Key=object_name)
        except Exception as e:
            logger.error(f"Error deleting object {object_name}: {e}")
            return False
        return True

    def download_object(self, bucket_name: str, object_key: str,
                        target_path: str) -> bool:
        """Download an S3 object.

        Args:
            bucket_name (str): Bucket to download from.
            object_key (str): Key of object.
            target_path (str): Local path to download to.

        Returns:
            bool: True if object downloaded, else False.
        """
        try:
            with self.get_client() as client, open(target_path, 'wb') as file:
                client.download_fileobj(bucket_name, object_key, file)
        except ClientError as e:
            logger.error(f"Error downloading object {object_key}: {e}")
            return False
        return True

    def create_presigned_url(
            self, bucket_name: str, object_name: str,
            expiration: int = 3600) -> str | None:
        try:
            with self.get_client() as client:
                presigned_url = client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': object_name},
                    ExpiresIn=expiration)
        except ClientError as e:
            logger.error(e)
            return None
        return presigned_url
