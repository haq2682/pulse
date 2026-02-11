import os
from minio import Minio


def parse_minio_endpoint(endpoint_url):
    """
    Parse MinIO endpoint URL and strip protocol prefix if present.
    
    The MinIO Python client expects endpoint in format 'hostname:port' without
    protocol prefix. This function handles both formats:
    - With protocol: 'http://localhost:9000' -> 'localhost:9000'
    - Without protocol: 'localhost:9000' -> 'localhost:9000'
    
    Args:
        endpoint_url: MinIO endpoint URL (e.g., 'localhost:9000' or 'http://localhost:9000')
        
    Returns:
        str: Endpoint in 'hostname:port' format
        
    Raises:
        ValueError: If endpoint is empty or invalid after parsing
    """
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT cannot be empty")
    
    # Strip protocol prefix if present
    if "://" in endpoint_url:
        endpoint_url = endpoint_url.split("://", 1)[1]
    
    # Validate that we have a non-empty endpoint after parsing
    endpoint_url = endpoint_url.strip()
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT is invalid after removing protocol")
    
    return endpoint_url


def create_minio_client():
    # Parse MINIO_ENDPOINT to strip protocol prefix if present
    minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    
    return Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )

BUCKET_NAME = "pulse-bucket-1"
