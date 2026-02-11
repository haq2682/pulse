import os
from minio import Minio

def create_minio_client():
    # Strip protocol prefix from MINIO_ENDPOINT if present (Minio client expects hostname:port only)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    if "://" in minio_endpoint:
        minio_endpoint = minio_endpoint.split("://", 1)[1]
    
    return Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )

BUCKET_NAME = "pulse-bucket-1"
