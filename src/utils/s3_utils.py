import boto3
from botocore import UNSIGNED
from botocore.config import Config

def get_s3_client(unsigned: bool = True):
    """
    Returns a boto3 S3 client.
    
    Args:
        unsigned (bool): If True, uses UNSIGNED signature (for public buckets).
    """
    if unsigned:
        return boto3.client('s3', config=Config(signature_version=UNSIGNED))
    else:
        return boto3.client('s3')
