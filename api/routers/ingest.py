"""
Ingest Stream Endpoint
======================

Exposes GET /ingest/stream — the endpoint that api_ingest_service.py polls
to fetch e-commerce data from a business's uploaded files in MinIO.

This fills the gap described in api_streaming_dag.py:
  "The Pulse backend must expose a route that returns e-commerce records
   in the canonical format expected by api_ingest_service.py."

Query parameters:
  business_id  (required)  Business whose ingested/ files to serve.
  since        (optional)  ISO-8601 datetime; return only files modified
                           after this timestamp.
  limit        (optional)  Max records per table per poll (default 500).

Response format (matches api_validation.APIDataFormat):
  {
    "tables": [
      { "table_name": "orders",    "data": [{...}, ...] },
      { "table_name": "customers", "data": [{...}, ...] }
    ]
  }

Incremental delivery
--------------------
Served S3 keys are recorded in Redis (set key: ingest_stream:{business_id}:served).
Each file is only returned ONCE per business stream.  To replay all files,
delete that Redis key:
  redis-cli DEL ingest_stream:<business_id>:served
"""

import io
import csv
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aioredis
import boto3
from botocore.client import Config
from fastapi import APIRouter, HTTPException, Query

# ---------------------------------------------------------------------------
# MinIO / S3-compatible client (same config as onboarding router)
# ---------------------------------------------------------------------------
_MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
_MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
_MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")

s3 = boto3.client(
    "s3",
    endpoint_url=_MINIO_ENDPOINT,
    aws_access_key_id=_MINIO_ACCESS,
    aws_secret_access_key=_MINIO_SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

# Served-file tracking keys auto-expire after 7 days of inactivity
_SERVED_KEY_TTL = 86_400 * 7

router = APIRouter(prefix="/ingest", tags=["ingest"])

# ---------------------------------------------------------------------------
# File parsers
# ---------------------------------------------------------------------------

def _parse_csv(content: bytes, limit: int) -> List[Dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")))
    rows: List[Dict[str, Any]] = []
    for i, row in enumerate(reader):
        if i >= limit:
            break
        rows.append(dict(row))
    return rows


def _parse_json(content: bytes, limit: int) -> List[Dict[str, Any]]:
    data = json.loads(content)
    if isinstance(data, list):
        return data[:limit]
    if isinstance(data, dict):
        for key in ("data", "records", "rows", "items", "results"):
            if key in data and isinstance(data[key], list):
                return data[key][:limit]
        return [data]
    return []


def _parse_parquet(content: bytes, limit: int) -> List[Dict[str, Any]]:
    try:
        import pandas as pd
        df = pd.read_parquet(io.BytesIO(content))
        return df.head(limit).to_dict(orient="records")
    except ImportError:
        return []


def _parse_file(s3_key: str, content: bytes, limit: int) -> List[Dict[str, Any]]:
    lower = s3_key.lower()
    try:
        if lower.endswith(".csv"):
            return _parse_csv(content, limit)
        if lower.endswith((".json", ".ndjson")):
            return _parse_json(content, limit)
        if lower.endswith(".parquet"):
            return _parse_parquet(content, limit)
    except Exception as exc:
        print(f"[ingest] Failed to parse {s3_key}: {exc}")
    return []


def _table_name_from_key(s3_key: str) -> str:
    """Derive a table name from an S3 key (filename without extension)."""
    filename = s3_key.rsplit("/", 1)[-1]
    name = filename.rsplit(".", 1)[0]
    return name.lower().replace("-", "_")


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/stream")
async def stream_data(
    business_id: str = Query(..., description="Business ID (MinIO bucket name)"),
    since: Optional[str] = Query(
        None,
        description="ISO-8601 datetime — return only files modified after this timestamp",
    ),
    limit: int = Query(
        500, ge=1, le=5_000,
        description="Max records per table returned per poll",
    ),
):
    """
    Return e-commerce records from a business's MinIO ``ingested/`` folder.

    Designed to be polled continuously by ``api_ingest_service.py``.
    Each eligible file is served exactly once per business stream; subsequent
    polls only return newly uploaded files.
    """
    # -- Parse optional since filter -----------------------------------------
    since_dt: Optional[datetime] = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid `since` value: {since!r}. Use ISO-8601, e.g. 2024-01-15T10:30:00Z",
            )

    # -- List objects in MinIO ------------------------------------------------
    prefix = f"{business_id}/ingested/"
    try:
        resp = s3.list_objects_v2(Bucket=business_id, Prefix=prefix)
    except Exception as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Could not list objects for business '{business_id}': {exc}",
        )

    objects = resp.get("Contents", [])
    if not objects:
        return {"tables": []}

    # -- Filter: skip already-served files and apply since filter ------------
    served_key = f"ingest_stream:{business_id}:served"
    already_served: set = await redis.smembers(served_key)

    _READABLE_EXTS = (".csv", ".json", ".ndjson", ".parquet")
    new_keys: List[str] = []
    for obj in objects:
        key: str = obj["Key"]
        if key in already_served:
            continue
        last_modified: datetime = obj["LastModified"]
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)
        if since_dt and last_modified <= since_dt:
            continue
        if any(key.lower().endswith(ext) for ext in _READABLE_EXTS):
            new_keys.append(key)

    if not new_keys:
        return {"tables": []}

    # -- Download, parse, and build response ----------------------------------
    tables: List[Dict[str, Any]] = []
    newly_served: List[str] = []

    for key in new_keys:
        try:
            body = s3.get_object(Bucket=business_id, Key=key)["Body"].read()
        except Exception as exc:
            print(f"[ingest] Could not download {key}: {exc}")
            continue

        rows = _parse_file(key, body, limit)
        if rows:
            tables.append({
                "table_name": _table_name_from_key(key),
                "data": rows,
            })
            newly_served.append(key)

    # -- Persist served keys in Redis -----------------------------------------
    if newly_served:
        await redis.sadd(served_key, *newly_served)
        await redis.expire(served_key, _SERVED_KEY_TTL)

    return {"tables": tables}
