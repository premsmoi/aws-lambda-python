import sys
import json
import boto3
import pandas as pd
import io

S3_BUCKET_NAME = 'phgluebucket'
OBJECT_KEY = 'data/sample_csv/dataload=20230214/Sample.csv'
OUTPUT_FILE = 'data/sample_csv/output/processed_sample.csv'

aws_access_key_id = 'AKIA3ABWUHOTXQ3LODXW'
aws_secret_access_key = '1FBnj+CdpnwO9iL1fDAFedl6eWg6i9AnaM+8Sb9s'

def save_csv(df: pd.DataFrame):
    s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    csv_buffer = io.StringIO()

    df.to_csv(csv_buffer, index=False, sep='|')
    s3_resource.Object(S3_BUCKET_NAME, OUTPUT_FILE).put(Body=csv_buffer.getvalue())

def load_csv():
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=OBJECT_KEY)
    content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(filepath_or_buffer=io.StringIO(content), delimiter='|')

    return df

def process_df(df: pd.DataFrame):
    df.columns = df.columns.str.strip()

    sum_row = df.sum().to_frame().T

    df = pd.concat([df, sum_row])

    return df

def lambda_handler(event = None, context = None):
    df = load_csv()
    df = process_df(df)

    print(df)
    save_csv(df)

if __name__ == '__main__':
    globals()[sys.argv[1]]()