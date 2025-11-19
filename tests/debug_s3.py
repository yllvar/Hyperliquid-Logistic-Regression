import boto3

def list_bucket():
    # Try using default credentials (signed)
    try:
        s3 = boto3.client('s3')
        bucket = "hyperliquid-archive"
        prefix = "market_data/20240101/00/l2Book/"
        
        print(f"Listing {bucket}/{prefix} with default credentials...")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5, RequestPayer='requester')
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("No objects found.")
    except Exception as e:
        print(f"Error with signed request: {e}")

if __name__ == "__main__":
    list_bucket()
