import os
import boto3
import pandas as pd
from io import StringIO

# Environment variables
BUCKET = os.environ["S3_BUCKET"]
ENDPOINT = os.environ["AWS_ENDPOINT_URL"]

# Connect to S3 (LocalStack)
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def main():
    # Read CSV from 'raw' folder
    obj = s3.get_object(Bucket=BUCKET, Key="raw/input.csv")
    df = pd.read_csv(obj["Body"])

    # Transform: filter and add column
    df = df[df["value"] > 10]
    df["value_x2"] = df["value"] * 2

    # Save transformed CSV to 'processed' folder
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key="processed/output.csv", Body=buffer.getvalue())

    print("ETL completed successfully")

if __name__ == "__main__":
    main()
    