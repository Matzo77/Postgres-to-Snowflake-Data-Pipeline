import boto3
from botocore.exceptions import ClientError
import os
import toml
import requests
import snowflake.connector
from urllib.parse import urlparse


def load_config(path='config.toml'):
    """"Loading configuration from .toml file"""
    with open(path,"r") as config_file:
        config = toml.load(config_file)
    return config

def download_to_lambda(bucket,file_key,tmp_path):
    """Downloading file from S3"""
    print("Initializing S3 client...")
    s3 = boto3.client('s3')
    print("S3 client initialized.")

    print(f"Downloading to {tmp_path}...")
    s3.download_file(
        bucket,
        file_key,
        tmp_path,
        ExtraArgs={'RequestPayer': 'requester'})
    print("Download complete")
    return

def upload_to_snowflake(tmp_path):
    """Uploading file to Snowflake"""
    config = load_config()
    print("Initializing Snowflake connection...")
    connection = snowflake.connector.connect(
        user=config['snowflake']['user'],
        password=os.environ.get('snowflake_password'), #password is saved as an environment variable
        account=config['snowflake']['account'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema']
    )
    print("Snowflake connection initialized.")

    print("Uploading file to Snowflake...")
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT csv_format
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
        """)

        cursor.execute("CREATE OR REPLACE STAGE my_stage FILE_FORMAT = csv_format")

        cursor.execute(f"PUT file://{tmp_path} @my_stage AUTO_COMPRESS=TRUE")

        table=config['snowflake']['table']

        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table}")
        cursor.execute(f"""
            COPY INTO {table} FROM @my_stage FILE_FORMAT = (FORMAT_NAME = 'csv_format')
        """)
    
    connection.commit()
    connection.close()
    print("Upload complete")
    return



def lambda_handler(event, context):
    config = load_config()

    parsed_url = urlparse(config['s3']['url'])

    bucket = parsed_url.netloc.split('.')[0]        # get the bucket name from url
    file_key = parsed_url.path.lstrip('/')          # get the file key from url
    file_name = os.path.basename(file_key)          # get the file name from url  

    tmp_path = f"/tmp/{file_name}"
    
    download_to_lambda(bucket, file_key, tmp_path)

    upload_to_snowflake(tmp_path)

    return
