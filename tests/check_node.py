import boto3
from botocore import UNSIGNED
from botocore.config import Config

def check_node_data():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = "hl-mainnet-node-data"
    prefix = "node_fills/"
    
    print(f"Listing {bucket}/{prefix}...")
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("No objects found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_node_data()
