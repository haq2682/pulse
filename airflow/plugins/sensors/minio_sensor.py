"""
Custom Airflow sensor: MinIO new-file detector.

Pokes the configured MinIO bucket/prefix and succeeds as soon as at least
one object appears (or a configurable minimum count is reached).  Uses the
MinIO Python SDK directly so no extra Airflow provider is required.

Usage in a DAG
--------------
    from plugins.sensors.minio_sensor import MinIONewFileSensor

    wait_for_ingested_files = MinIONewFileSensor(
        task_id="wait_for_ingested_files",
        bucket="{{ var.value.default_bucket }}",
        prefix="ingested/",
        min_objects=1,
        poke_interval=60,   # seconds
        timeout=3600,       # 1 h
        mode="reschedule",  # frees the worker slot between pokes
    )
"""

from __future__ import annotations

import logging
import os
import sys

from airflow.sensors.base import BaseSensorOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.pipeline_config import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)

log = logging.getLogger(__name__)


class MinIONewFileSensor(BaseSensorOperator):
    """
    Pokes a MinIO bucket prefix and succeeds when ≥ *min_objects* objects exist.

    Parameters
    ----------
    bucket : str
        MinIO bucket name.
    prefix : str
        Object key prefix to scan (e.g. "ingested/").
    min_objects : int
        Minimum number of objects required before the sensor returns True.
    minio_endpoint : str
        Optional override for the MinIO endpoint.
    minio_access_key : str
        Optional override for the access key.
    minio_secret_key : str
        Optional override for the secret key.
    minio_secure : bool
        Use TLS (default False for local docker).
    """

    template_fields = ("bucket", "prefix")

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        min_objects: int = 1,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        minio_secure: bool = MINIO_SECURE,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket          = bucket
        self.prefix          = prefix
        self.min_objects     = min_objects
        self.minio_endpoint  = (minio_endpoint or MINIO_ENDPOINT).lstrip("http://").lstrip("https://")
        self.minio_access_key = minio_access_key or MINIO_ACCESS_KEY
        self.minio_secret_key = minio_secret_key or MINIO_SECRET_KEY
        self.minio_secure    = minio_secure

    def poke(self, context) -> bool:
        from minio import Minio
        from minio.error import S3Error

        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure,
        )

        try:
            objects = client.list_objects(self.bucket, prefix=self.prefix, recursive=True)
            count   = sum(1 for _ in objects)
            log.info(
                "MinIONewFileSensor: found %d objects at %s/%s (need ≥%d)",
                count, self.bucket, self.prefix, self.min_objects,
            )
            return count >= self.min_objects

        except S3Error as exc:
            if exc.code == "NoSuchBucket":
                log.warning("Bucket '%s' does not exist yet", self.bucket)
                return False
            raise


class MinIOPrefixEmptySensor(BaseSensorOperator):
    """
    Succeeds when a MinIO prefix contains *zero* objects.

    Useful for waiting until a previous stage has moved / deleted its output.
    """

    template_fields = ("bucket", "prefix")

    def __init__(
        self,
        bucket: str,
        prefix: str,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        minio_secure: bool = MINIO_SECURE,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket           = bucket
        self.prefix           = prefix
        self.minio_endpoint   = (minio_endpoint or MINIO_ENDPOINT).lstrip("http://").lstrip("https://")
        self.minio_access_key = minio_access_key or MINIO_ACCESS_KEY
        self.minio_secret_key = minio_secret_key or MINIO_SECRET_KEY
        self.minio_secure     = minio_secure

    def poke(self, context) -> bool:
        from minio import Minio
        from minio.error import S3Error

        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure,
        )
        try:
            objects = client.list_objects(self.bucket, prefix=self.prefix, recursive=True)
            count   = sum(1 for _ in objects)
            log.info("MinIOPrefixEmptySensor: %d objects at %s/%s", count, self.bucket, self.prefix)
            return count == 0
        except S3Error as exc:
            if exc.code == "NoSuchBucket":
                return True   # nothing there → effectively empty
            raise
