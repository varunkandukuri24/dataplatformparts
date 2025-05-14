import boto3
import os
from typing import Any, Dict
import pandas as pd
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables
load_dotenv('config/.env')

class S3Handler:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.prefix = os.getenv('S3_PREFIX')

    def upload_dataframe(self, df: pd.DataFrame, key: str) -> bool:
        """
        Upload a pandas DataFrame to S3 as parquet file
        """
        try:
            # Convert DataFrame to parquet bytes
            parquet_buffer = df.to_parquet(index=False)
            
            # Upload to S3
            full_key = f"{self.prefix}/{key}.parquet"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_key,
                Body=parquet_buffer
            )
            return True
        except Exception as e:
            print(f"Error uploading to S3: {str(e)}")
            return False

    def read_parquet(self, key: str) -> pd.DataFrame:
        """
        Read a parquet file from S3 into a pandas DataFrame
        """
        try:
            full_key = f"{self.prefix}/{key}.parquet"
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=full_key
            )
            # Read the file content into a BytesIO object
            buffer = BytesIO(response['Body'].read())
            # Read the parquet file from the buffer
            return pd.read_parquet(buffer)
        except Exception as e:
            print(f"Error reading from S3: {str(e)}")
            return pd.DataFrame()

    def list_files(self, prefix: str = None) -> list:
        """
        List files in the S3 bucket with the given prefix
        """
        try:
            full_prefix = f"{self.prefix}/{prefix}" if prefix else self.prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=full_prefix
            )
            return [obj['Key'] for obj in response.get('Contents', [])]
        except Exception as e:
            print(f"Error listing files: {str(e)}")
            return [] 