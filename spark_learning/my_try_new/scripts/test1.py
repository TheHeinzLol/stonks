import boto3
from botocore.client import Config

# MinIO configuration
s3_url = "http://minio:9000"
access_key = "admin"
secret_key = "admin_password"
bucket_name = "airflow.learning"

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url=s3_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# List objects in the bucket
try:
    response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
    
    if 'Contents' in response:
        print(f"Files in bucket '{bucket_name}':")
        for obj in response['Contents']:
            print(f"- {obj['Key']} ({obj['Size']} bytes)")
    else:
        print(f"No files found in bucket '{bucket_name}'")
        
except Exception as e:
    print(f"Error: {e}")
