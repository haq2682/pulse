import os
import signal
import logging
from fastapi import APIRouter, Depends, HTTPException, Response, Request, Query, UploadFile, File
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from database import get_db
from sqlalchemy import text
import aioredis
import pycountry
import uuid
from rapidfuzz import process, fuzz
import json
import boto3
from botocore.client import Config
from typing import List
import subprocess
from datetime import datetime
import asyncio
import time
import re

redis = aioredis.from_url(
    f"redis://{os.getenv('REDIS_HOST', '10.5.0.11')}:{os.getenv('REDIS_PORT', '6379')}",
    decode_responses=True,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
# boto3 requires a scheme in endpoint_url; docker-compose's .env already
# includes one, but the Kubernetes deployment.yaml sets this to a bare
# host:port (the format the plain `minio` SDK and other clients in this
# codebase expect instead) - prepend http:// when it's missing rather than
# assume the caller always provides one. MinIO has no TLS listener anywhere
# in this project (no MINIO_SECURE var exists), so http is always correct.
if not MINIO_ENDPOINT.startswith(("http://", "https://")):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MAPPING_LOG_DIR = os.getenv("MAPPING_LOG_DIR", "/tmp")
MAPPING_PROCESS_TTL = 86400  # 24 hours in seconds


def _mapping_pid_is_alive(pid: int) -> bool:
    """
    True if `pid` is a real, still-executing process on this host - i.e.
    NOT gone (ESRCH) and NOT a zombie.

    os.kill(pid, 0) alone is not enough: a process that has exited but
    hasn't been reaped by its parent yet (a zombie - the state a
    kernel-OOM-killed subprocess.Popen child is left in until this
    process's event loop happens to await/poll it) still occupies a valid
    PID slot, so os.kill(pid, 0) succeeds and does NOT raise - verified
    live, this is exactly what left the mapping pipeline reporting
    "running" forever in the onboarding UI after the OOM-killed subprocess
    became a zombie. /proc/<pid>/status's State line is the reliable way
    to tell the two apart.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists (different owner/container in a multi-replica setup) - alive.
        return True

    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("State:"):
                    return "Z" not in line
    except FileNotFoundError:
        # Process vanished between the kill(0) check and reading /proc - gone.
        return False
    except OSError:
        # /proc unavailable for some other reason - fall back to kill(0)'s answer.
        pass
    return True
MAPPING_LOG_MAX_LINES = 500  # Maximum number of log lines to return to avoid large responses
MANUAL_MAPPING_TIMEOUT_SECONDS = 300  # 5 minutes timeout for applying manual mappings

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

router = APIRouter(
    prefix="/onboarding",
    tags=["onboarding"],
)

logger = logging.getLogger("pulse")


_QUALIFIED_REF_RE = re.compile(r'^\s*"(?P<table>[^"]+)"\s*\.\s*"(?P<column>[^"]+)"\s*$')
_UNQUOTED_REF_RE = re.compile(r'^\s*(?P<table>[A-Za-z_][\w$]*)\s*\.\s*(?P<column>[A-Za-z_][\w$]*)\s*$')


def _build_qualified_ref(table_name: str, column_name: str) -> str:
    table = str(table_name or "").strip().strip('"')
    column = str(column_name or "").strip().strip('"')
    return f'"{table}"."{column}"' if table and column else ""


def _parse_source_ref(raw_value, fallback_table: str = None):
    if raw_value is None:
        return None, None

    if isinstance(raw_value, dict):
        source_table = (
            raw_value.get("table")
            or raw_value.get("sourceTable")
            or raw_value.get("source_table")
        )
        source_column = (
            raw_value.get("originalColumn")
            or raw_value.get("column")
            or raw_value.get("value")
            or raw_value.get("sourceColumn")
            or raw_value.get("source_column")
        )
        return (
            str(source_table).strip().strip('"') if source_table else None,
            str(source_column).strip().strip('"') if source_column else None,
        )

    raw_str = str(raw_value).strip()
    if not raw_str:
        return None, None

    qualified_match = _QUALIFIED_REF_RE.match(raw_str)
    if qualified_match:
        return (
            qualified_match.group("table").strip(),
            qualified_match.group("column").strip(),
        )

    unquoted_match = _UNQUOTED_REF_RE.match(raw_str)
    if unquoted_match:
        return (
            unquoted_match.group("table").strip(),
            unquoted_match.group("column").strip(),
        )

    return (fallback_table, raw_str.strip().strip('"'))


def _build_extra_col_index(mapping_results_payload) -> dict:
    payload = mapping_results_payload or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}

    index = {}
    if not isinstance(payload, dict):
        return index

    for item in payload.get("extra_cols", []):
        if not isinstance(item, dict):
            continue
        table = str(item.get("table") or "").strip()
        column = str(item.get("column") or "").strip()
        if not table or not column:
            continue
        index.setdefault(column, set()).add(table)
    return index


def _normalize_table_mappings(table_mappings_payload, extra_col_index=None):
    if not isinstance(table_mappings_payload, dict):
        return {}

    normalized = {}
    for target_table, table_map in table_mappings_payload.items():
        target_table_str = str(target_table or "").strip()
        if not target_table_str or not isinstance(table_map, dict):
            continue

        normalized[target_table_str] = {}
        for canonical_col, raw_source_ref in table_map.items():
            canonical_col_str = str(canonical_col or "").strip()
            if not canonical_col_str:
                continue

            source_table, source_col = _parse_source_ref(raw_source_ref, fallback_table=target_table_str)
            if not source_col:
                continue

            if not source_table and extra_col_index:
                candidate_tables = sorted(list(extra_col_index.get(source_col, set())))
                if len(candidate_tables) == 1:
                    source_table = candidate_tables[0]

            source_table = source_table or target_table_str
            qualified_ref = _build_qualified_ref(source_table, source_col)
            if qualified_ref:
                normalized[target_table_str][canonical_col_str] = qualified_ref

        if not normalized[target_table_str]:
            normalized.pop(target_table_str, None)

    return normalized


_TABLE_JOIN_KEYS = {
    "addresses": {"address_id"},
    "customers": {"customer_id", "address_id"},
    "suppliers": {"supplier_id"},
    "categories": {"category_id"},
    "products": {"product_id", "category_id", "supplier_id"},
    "inventory": {"inventory_id", "product_id", "supplier_id"},
    "wishlist": {"wishlist_id", "customer_id", "product_id"},
    "shopping_cart": {"cart_id", "customer_id", "session_id"},
    "cart_items": {"cart_item_id", "cart_id", "product_id"},
    "orders": {"order_id", "customer_id"},
    "order_items": {"order_item_id", "order_id", "product_id"},
    "payments": {"payment_id", "order_id"},
    "reviews": {"review_id", "product_id", "customer_id"},
    "marketing_campaigns": {"campaign_id"},
    "customer_sessions": {"session_id", "customer_id"},
}


def _find_invalid_cross_table_mappings(normalized_mappings: dict) -> list:
    invalid = []
    if not isinstance(normalized_mappings, dict):
        return invalid

    for target_table, table_map in normalized_mappings.items():
        if not isinstance(table_map, dict):
            continue

        target_table_norm = str(target_table or "").strip().lower()
        target_keys = _TABLE_JOIN_KEYS.get(target_table_norm, set())

        for canonical_col, source_ref in table_map.items():
            source_table, source_col = _parse_source_ref(source_ref, fallback_table=target_table_norm)
            source_table_norm = str(source_table or "").strip().lower()
            source_col_norm = str(source_col or "").strip()
            if not source_col_norm:
                continue

            if not source_table_norm or source_table_norm == target_table_norm:
                continue

            source_keys = _TABLE_JOIN_KEYS.get(source_table_norm, set())
            common_keys = sorted(list(target_keys & source_keys))

            if not common_keys:
                invalid.append(
                    {
                        "targetTable": target_table_norm,
                        "targetColumn": str(canonical_col or "").strip(),
                        "sourceTable": source_table_norm,
                        "sourceColumn": source_col_norm,
                        "reason": (
                            f"No valid join key between '{target_table_norm}' and "
                            f"'{source_table_norm}'."
                        ),
                    }
                )

    return invalid

def _airflow_auth_header() -> str:
    """Return a Basic-auth header value for Airflow REST API calls."""
    import base64
    user = os.getenv("AIRFLOW_USERNAME", "admin")
    pwd  = os.getenv("AIRFLOW_PASSWORD", "admin")
    return "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()


def _dag_active_run_exists_for_bucket(dag_id: str, business_id: str) -> bool:
    """
    Return True if the given Airflow DAG already has a ``running`` or ``queued``
    run whose ``conf.bucket`` matches *business_id*.

    Called synchronously from within ``run_in_threadpool`` so it never blocks
    the async event loop.  Any network error is treated as "no active run found"
    (fail-open) so a temporary Airflow outage cannot prevent a new run from
    being triggered.
    """
    import urllib.request
    import urllib.error
    import json as _json

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    # Fetch the 50 most-recent running/queued runs for this DAG in one call.
    # 50 is far more than any realistic concurrent-run count for a single DAG.
    url = (
        f"{airflow_base}/api/v1/dags/{dag_id}/dagRuns"
        "?state=running&state=queued&limit=50&order_by=-execution_date"
    )
    req = urllib.request.Request(
        url,
        headers={"Authorization": _airflow_auth_header()},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = _json.loads(resp.read())
        for run in data.get("dag_runs", []):
            conf = run.get("conf") or {}
            if conf.get("bucket") == business_id:
                print(
                    f"[idempotency] DAG '{dag_id}' already has an active run "
                    f"(run_id={run.get('dag_run_id')}) for bucket '{business_id}' — skipping trigger."
                )
                return True
    except Exception as exc:
        # Fail-open: if we cannot reach Airflow, allow the trigger rather than
        # silently blocking the user's confirmation.
        print(f"[idempotency] Could not check active runs for DAG '{dag_id}': {exc} — proceeding.")
    return False


async def _trigger_api_streaming_dag(business_id: str, api_url: str) -> None:
    """
    Idempotently trigger the api_streaming Airflow DAG.

    Checks for an already-running or queued run for this tenant before
    creating a new DAG run, so a double-confirm or HTTP retry never spawns
    a second competing streaming job on the same Kafka consumer group.
    """
    import urllib.request
    import urllib.error
    import json as _json

    # Idempotency guard — runs in threadpool to avoid blocking the event loop.
    already_running = await run_in_threadpool(
        _dag_active_run_exists_for_bucket, "api_streaming", business_id
    )
    if already_running:
        return

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    poll_interval = int(os.getenv("API_POLL_INTERVAL", "30"))

    url = f"{airflow_base}/api/v1/dags/api_streaming/dagRuns"
    # Airflow Stable REST API accepts dag-run payload keys like dag_run_id,
    # logical_date, note, and conf. It does NOT accept top-level "params"
    # (that causes HTTP 400: {"params": ["Unknown field."]} on strict schemas).
    # The DAG reads dag_run.conf already, so conf-only is sufficient.
    payload = _json.dumps({
        "conf": {
            "bucket":        business_id,
            "api_url":       api_url,
            "poll_interval": poll_interval,
        },
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": _airflow_auth_header(),
        },
        method="POST",
    )

    def _do_request():
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            raise RuntimeError(
                f"Airflow api_streaming trigger failed with HTTP {e.code}: {body}"
            ) from e

    await run_in_threadpool(_do_request)


async def _trigger_db_streaming_dag(business_id: str, db_uri: str, db_tables: str) -> None:
    """
    Idempotently trigger the db_streaming Airflow DAG.

    Checks for an already-running or queued run for this tenant before
    creating a new DAG run, so a double-confirm or HTTP retry never spawns
    two competing Debezium CDC streaming jobs sharing the same Kafka
    consumer group and MinIO checkpoint.
    """
    import urllib.request
    import urllib.error
    import json as _json

    # Idempotency guard — runs in threadpool to avoid blocking the event loop.
    already_running = await run_in_threadpool(
        _dag_active_run_exists_for_bucket, "db_streaming", business_id
    )
    if already_running:
        return

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")

    url = f"{airflow_base}/api/v1/dags/db_streaming/dagRuns"
    # Airflow Stable REST API accepts dag-run payload keys like dag_run_id,
    # logical_date, note, and conf. It does NOT accept top-level "params"
    # (that causes HTTP 400: {"params": ["Unknown field."]} on strict schemas).
    # The DAG reads dag_run.conf already, so conf-only is sufficient.
    payload = _json.dumps({
        "conf": {
            "bucket":    business_id,
            "db_uri":    db_uri,
            "db_tables": db_tables,
        },
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": _airflow_auth_header(),
        },
        method="POST",
    )

    def _do_request():
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            raise RuntimeError(
                f"Airflow db_streaming trigger failed with HTTP {e.code}: {body}"
            ) from e

    await run_in_threadpool(_do_request)


async def terminate_mapping_process(process_id: int):
    """
    Helper function to gracefully terminate a mapping process.
    First tries SIGTERM for graceful shutdown, then SIGKILL if needed.
    
    Args:
        process_id: The process ID to terminate
    """
    try:
        # Try graceful shutdown with SIGTERM
        os.kill(process_id, signal.SIGTERM)
        # Wait a bit for graceful shutdown
        await asyncio.sleep(1)
        try:
            # Check if still alive using signal 0 (doesn't kill, just checks existence)
            os.kill(process_id, 0)
            # Process still running, force kill with SIGKILL
            os.kill(process_id, signal.SIGKILL)
        except OSError:
            # Process already terminated, which is good
            pass
    except OSError as e:
        # Process may have already terminated
        print(f"Process {process_id} may have already terminated: {e}")

async def get_or_set_cache(key, func, expire=3600):
    cached = await redis.get(key)
    if cached:
        return json.loads(cached)
    data = func()
    await redis.set(key, json.dumps(data), ex=expire)
    return data

def get_currency_suggestions(query: str, limit=10):
    choices = {f"{c.alpha_3} - {c.name}": c.alpha_3 for c in pycountry.currencies}
    results = process.extract(query, choices.keys(), scorer=fuzz.WRatio, limit=limit)
    return [{"label": k, "value": choices[k]} for k, score, _ in results if score > 50][:limit]

def get_region_suggestions(query: str, limit=10):
    choices = {f"{c.name} ({c.alpha_2})": c.alpha_2 for c in pycountry.countries}
    results = process.extract(query, choices.keys(), scorer=fuzz.WRatio, limit=limit)
    return [{"label": k, "value": choices[k]} for k, score, _ in results if score > 50][:limit]

def empty_bucket(bucket_name):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

@router.post("/create")
async def create_onboarding(request: Request, db=Depends(get_db)):
    body = await request.json()
    user_id = body.get("userId")
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")
        result = db.execute(text("SELECT onboarding_id, current_step FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": user_id})
        existing = result.fetchone()
        if existing:
            onboarding_id, current_step = existing
            return {
                "status": 200,
                "onboarding_id": onboarding_id,
                "current_step": current_step,
                "message": "Onboarding already exists."
            }
        else:
            onboarding_id = str(uuid.uuid4())
            db.execute(text("INSERT INTO onboarding (onboarding_id, user_id, current_step) VALUES (:onboarding_id, :user_id, :current_step)"), {"onboarding_id": onboarding_id, "user_id": user_id, "current_step": "business"})
            db.commit()
            return {"status": 200, "onboarding_id": onboarding_id, "current_step": "business", "message": "Onboarding created."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/create-business")
async def create_business(request: Request, db=Depends(get_db)):
    try:
        body = await request.json()
        userId = body.get("userId")
        businessName = body.get("businessName")
        businessRegion = body.get("businessRegion")
        businessCurrency = body.get("businessCurrency")
        if not userId or not businessName or not businessRegion or not businessCurrency:
            raise HTTPException(status_code=400, detail="Authenticated User, Business Name, Business Region, and Business Currency are required")
        onboarding = db.execute(text("SELECT onboarding_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        business_id = str(uuid.uuid4())
        businessCurrency = businessCurrency.get("value")
        businessRegion = businessRegion.get("value")
        db.execute(text("INSERT INTO businesses (business_id, user_id, business_name, business_region, business_currency) VALUES (:business_id, :user_id, :business_name, :business_region, :business_currency)"), {"business_id": business_id, "user_id": userId, "business_name": businessName, "business_region": businessRegion, "business_currency": businessCurrency})
        db.commit()
        db.execute(text("UPDATE onboarding SET current_step = :next_step, business_id = :business_id WHERE user_id = :user_id AND is_completed = false"), {"next_step": "data-type", "user_id": userId, "business_id": business_id})
        db.commit()
        try:
            existing_buckets = s3.list_buckets().get('Buckets', [])
            if any(b['Name'] == business_id for b in existing_buckets):
                print(f"Bucket '{business_id}' already exists")
            else:
                s3.create_bucket(Bucket=business_id)
                print(f"Bucket '{business_id}' created successfully")
        except Exception as e:
            print("Error creating bucket:", e)
        return {"status": 200}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/select-data-type")
async def select_data_type(request: Request, db=Depends(get_db)):
    try:
        body = await request.json()
        userId = body.get("userId")
        dataType = body.get("dataType")
        if not userId or not dataType:
            raise HTTPException(status_code=400, detail="Authenticated User and Data Type are required")
        onboarding = db.execute(text("SELECT onboarding_id, business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        db.execute(text("UPDATE onboarding SET current_step = :next_step, ingestion_type = :ingestion_type WHERE user_id = :user_id AND is_completed = false"), {"next_step": "connect", "ingestion_type": dataType, "user_id": userId})
        db.commit()
        return {"status": 200}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/get-data-type")
async def get_data_type(userId: str, db=Depends(get_db)):
    try:
        if not userId:
            raise HTTPException(status_code=400, detail="Authenticated User is required")
        onboarding = db.execute(text("SELECT ingestion_type, business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        return {"status": 200, "dataType": onboarding_record[0], "businessId": onboarding_record[1]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/upload-chunk")
async def upload_chunk(
    request: Request,
    db=Depends(get_db)
):
    upload_id = None
    try:
        form = await request.form()
        chunk = await form["chunk"].read()
        chunk_index = int(form["chunkIndex"])
        total_chunks = int(form["totalChunks"])
        file_id = form["fileId"]
        file_name = form["fileName"]
        file_size = int(form["fileSize"])
        file_type = form["fileType"]
        user_id = form["userId"]

        onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": user_id})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")

        business_id = onboarding_record[0]
        s3_key = f"ingested/{file_name}"

        if chunk_index == 0:
            existing = db.execute(text("SELECT file_id FROM uploaded_files WHERE file_id = :file_id"), {"file_id": file_id})
            if not existing.fetchone():
                db.execute(text(
                    "INSERT INTO uploaded_files (file_id, business_id, file_name, file_size, file_type, s3_key, upload_status) "
                    "VALUES (:file_id, :business_id, :file_name, :file_size, :file_type, :s3_key, :upload_status)"
                ), {"file_id": file_id, "business_id": business_id, "file_name": file_name, "file_size": file_size, "file_type": file_type, "s3_key": s3_key, "upload_status": "uploading"})
                db.commit()

            multipart = s3.create_multipart_upload(Bucket=business_id, Key=s3_key)
            upload_id = multipart["UploadId"]
            await redis.set(f"upload:{file_id}:upload_id", upload_id, ex=86400)
        else:
            upload_id = await redis.get(f"upload:{file_id}:upload_id")

        part_number = chunk_index + 1
        part = s3.upload_part(
            Bucket=business_id,
            Key=s3_key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=chunk
        )

        parts_key = f"upload:{file_id}:parts"
        parts_json = await redis.get(parts_key) or "[]"
        parts = json.loads(parts_json)
        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
        await redis.set(parts_key, json.dumps(parts), ex=86400)

        if chunk_index == total_chunks - 1:
            parts = sorted(parts, key=lambda x: x["PartNumber"])
            s3.complete_multipart_upload(
                Bucket=business_id,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )

            db.execute(text("UPDATE uploaded_files SET upload_status = :status WHERE file_id = :file_id"),
                      {"status": "completed", "file_id": file_id})
            db.commit()

            await redis.delete(f"upload:{file_id}:upload_id")
            await redis.delete(f"upload:{file_id}:parts")

        return {"status": 200, "chunkIndex": chunk_index}
    except Exception as e:
        if 'file_id' in locals():
            db.execute(text("UPDATE uploaded_files SET upload_status = :status WHERE file_id = :file_id"),
                      {"status": "failed", "file_id": file_id})
            db.commit()
        # Abort the S3 multipart upload on failure so it doesn't linger as an
        # orphaned, billable-storage part set that MinIO never cleans up on
        # its own (no lifecycle rule is configured for incomplete uploads).
        if upload_id and 'business_id' in locals() and 's3_key' in locals():
            try:
                s3.abort_multipart_upload(Bucket=business_id, Key=s3_key, UploadId=upload_id)
            except Exception:
                pass
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/uploaded-files")
async def get_uploaded_files(userId: str, db=Depends(get_db)):
    try:
        if not userId:
            raise HTTPException(status_code=400, detail="userId is required")
        
        onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record or not onboarding_record[0]:
            return {"status": 200, "files": []}
        
        business_id = onboarding_record[0]
        files = db.execute(text(
            "SELECT file_id, file_name, file_size, file_type, upload_status, created_at "
            "FROM uploaded_files WHERE business_id = :business_id AND upload_status = 'completed' "
            "ORDER BY created_at DESC"
        ), {"business_id": business_id})
        
        result = []
        for row in files:
            result.append({
                "fileId": row[0],
                "fileName": row[1],
                "fileSize": row[2],
                "fileType": row[3],
                "uploadStatus": row[4],
                "createdAt": row[5].isoformat() if row[5] else None
            })
        
        return {"status": 200, "files": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/delete-file")
async def delete_file(request: Request, db=Depends(get_db)):
    try:
        body = await request.json()
        file_id = body.get("fileId")
        user_id = body.get("userId")
        
        if not file_id or not user_id:
            raise HTTPException(status_code=400, detail="fileId and userId are required")
        
        onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": user_id})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        business_id = onboarding_record[0]
        
        file_record = db.execute(text(
            "SELECT s3_key FROM uploaded_files WHERE file_id = :file_id AND business_id = :business_id"
        ), {"file_id": file_id, "business_id": business_id})
        file_data = file_record.fetchone()
        
        if not file_data:
            raise HTTPException(status_code=404, detail="File not found")
        
        s3_key = file_data[0]
        s3.delete_object(Bucket=business_id, Key=s3_key)
        
        db.execute(text("DELETE FROM uploaded_files WHERE file_id = :file_id"), {"file_id": file_id})
        db.commit()
        
        return {"status": 200}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/cancel")
async def cancel_onboarding(request: Request, db=Depends(get_db)):
    try:
        body = await request.json()
        userId = body.get("userId")
        if not userId:
            raise HTTPException(status_code=400, detail="Authenticated User is required")
        onboarding = db.execute(text("SELECT onboarding_id, business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        db.execute(text("DELETE FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        db.commit()
        if onboarding_record[1]:
            db.execute(text("DELETE FROM uploaded_files WHERE business_id = :business_id"), {"business_id": onboarding_record[1]})
            db.execute(text("DELETE FROM businesses WHERE business_id = :business_id"), {"business_id": onboarding_record[1]})
            db.commit()
            try:
                empty_bucket(onboarding_record[1])
                s3.delete_bucket(Bucket=onboarding_record[1])
            except Exception as e:
                print("Error deleting bucket:", e)
        return {"status": 200}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@router.get("/get-current-step")
async def get_current_step(userId: str, db=Depends(get_db)):
    try:
        if not userId:
            raise HTTPException(status_code=400, detail="Authenticated User is required")
        onboarding = db.execute(text("SELECT current_step FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        return {"status": 200, "currentStep": onboarding_record[0]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/api/currencies")
async def currencies(query: str = Query("")):
    return await get_or_set_cache(f"currencies:{query}", lambda: get_currency_suggestions(query))

@router.get("/api/regions")
async def regions(query: str = Query("")):
    return await get_or_set_cache(f"regions:{query}", lambda: get_region_suggestions(query))

@router.post("/start-mapping")
async def start_mapping(request: Request, db=Depends(get_db)):
    """
    Start the mapping pipeline in background using subprocess.Popen.
    Supports batch, db, and api modes with connectivity validation.
    Saves the pipeline state in PostgreSQL onboarding table.
    """
    try:
        body = await request.json()
        
        # Get userId from body and verify it matches authenticated user
        body_user_id = body.get("userId")
        if body_user_id is None:
            raise HTTPException(status_code=403, detail="Authentication Required")
        
        # Use authenticated user ID
        user_id = body_user_id
        mode = body.get("mode", "batch")  # Default to batch mode
        db_uri = body.get("dbUri")  # For db mode
        api_url = body.get("apiUrl")  # For api mode
        db_tables = body.get("dbTables", [])  # For db mode
        
        # Validate required parameters for each mode
        if mode == "db" and not db_uri:
            raise HTTPException(status_code=400, detail="Database URI is required for db mode")
        
        if mode == "api" and not api_url:
            raise HTTPException(status_code=400, detail="API URL is required for api mode")
        
        # Validate connectivity for db and api modes (run in threadpool to avoid blocking)
        if mode == "db":
            from utils.connectivity_validator import validate_database_connection, discover_and_match_db_tables
            success, message = await run_in_threadpool(validate_database_connection, db_uri, 10)
            if not success:
                raise HTTPException(status_code=400, detail=message)
            print(f"Database connectivity validated: {message}")

            # Auto-discover tables when the caller did not specify any
            if not db_tables:
                try:
                    db_tables = await run_in_threadpool(discover_and_match_db_tables, db_uri, 10)
                    print(f"Auto-discovered {len(db_tables)} tables matching canonical schema: {db_tables}")
                except Exception as disc_err:
                    logger.warning("Table auto-discovery failed (non-fatal): %s", disc_err)

        elif mode == "api":
            from utils.connectivity_validator import validate_api_endpoint
            success, message = await run_in_threadpool(validate_api_endpoint, api_url, 10)
            if not success:
                raise HTTPException(status_code=400, detail=message)
            print(f"API endpoint connectivity validated: {message}")
        
        # Get the onboarding record
        onboarding = db.execute(
            text("SELECT onboarding_id, business_id, current_step, mapping_status FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        onboarding_id, business_id, current_step, mapping_status = onboarding_record
        
        if not business_id:
            raise HTTPException(status_code=400, detail="Business ID not found")
        
        # Check if mapping is already running
        if mapping_status == "running":
            # Verify that the tracked mapping process is actually still alive
            # This avoids getting stuck in a permanent "running" state if the
            # subprocess crashed or the Redis PID key expired
            pid_key = f"mapping_pid:{onboarding_id}"
            mapping_pid = await redis.get(pid_key)
            
            process_still_running = False
            if mapping_pid:
                try:
                    process_still_running = _mapping_pid_is_alive(int(mapping_pid))
                except ValueError:
                    process_still_running = False

            if process_still_running:
                return {
                    "status": 200,
                    "message": "Mapping pipeline is already running",
                    "mapping_status": "running",
                }
            
            # The mapping was marked as running, but no live process is associated
            # with it anymore. Reset the stale status to allow a new run
            db.execute(
                text("""
                    UPDATE onboarding
                    SET mapping_status = :mapping_status,
                        mapping_error = :mapping_error,
                        mapping_completed_at = :completed_at
                    WHERE user_id = :user_id AND is_completed = false
                """),
                {
                    "mapping_status": "failed",
                    "mapping_error": "Previous mapping process was not running but status was 'running'. Status reset to allow retry.",
                    "completed_at": datetime.utcnow(),
                    "user_id": user_id
                }
            )
            db.commit()
            # Continue to start a new mapping run
        
        # Update the onboarding record to indicate mapping is in progress
        db.execute(
            text("""
                UPDATE onboarding 
                SET current_step = :current_step,
                    mapping_status = :mapping_status,
                    mapping_started_at = :started_at,
                    mapping_error = NULL,
                    mapping_completed_at = NULL
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "current_step": "mapping-in-progress",
                "mapping_status": "running",
                "started_at": datetime.utcnow(),
                "user_id": user_id
            }
        )
        db.commit()
        
        # Get the path to the mapping script
        # In docker, the mapping folder is mounted at /app/mapping
        script_path = "/app/mapping/run_mapping.py"
        
        # Check if script exists
        if not os.path.exists(script_path):
            raise HTTPException(status_code=500, detail=f"Mapping script not found at {script_path}")
        
        # Build the command to run the mapping pipeline
        cmd = [
            "python3",
            "-u",  # Unbuffered output for real-time logging
            script_path,
            "--mode", mode,
            "--business-id", business_id
        ]
        
        # Add mode-specific parameters
        if mode == "db":
            cmd.extend(["--db-uri", db_uri])
            if db_tables:
                cmd.extend(["--db-tables", ",".join(db_tables)])
            # Bound the initial mapping run so it exits after the first
            # micro-batch.  The db_streaming Airflow DAG takes over
            # continuous CDC streaming once the user confirms mappings.
            cmd.append("--trigger-once")
        elif mode == "api":
            cmd.extend(["--api-url", api_url])
            # Bound the initial mapping run so it exits after collecting enough
            # schema data.  The api_streaming Airflow DAG takes over continuous
            # polling once the user confirms their column mappings.
            initial_poll_duration = int(os.getenv("API_INITIAL_POLL_DURATION", "120"))
            cmd.extend(["--poll-duration", str(initial_poll_duration)])
            cmd.append("--trigger-once")

        # Store connection details in the onboarding record so confirm-mapping
        # can retrieve them later to trigger the corresponding Airflow DAG.
        if mode == "api" and api_url:
            db.execute(
                text("""
                    UPDATE onboarding
                    SET api_url = :api_url
                    WHERE user_id = :user_id AND is_completed = false
                """),
                {"api_url": api_url, "user_id": user_id}
            )
            db.commit()
        elif mode == "db" and db_uri:
            db_tables_str = ",".join(db_tables) if db_tables else ""
            db.execute(
                text("""
                    UPDATE onboarding
                    SET db_uri = :db_uri, db_tables = :db_tables
                    WHERE user_id = :user_id AND is_completed = false
                """),
                {"db_uri": db_uri, "db_tables": db_tables_str, "user_id": user_id}
            )
            db.commit()
        
        # Start the mapping pipeline in background using subprocess.Popen
        # Use stdout and stderr redirection to avoid blocking
        # Ensure log directory exists
        if not os.path.exists(MAPPING_LOG_DIR):
            try:
                os.makedirs(MAPPING_LOG_DIR, exist_ok=True)
            except Exception as dir_error:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Failed to create log directory: {str(dir_error)}"
                )
        
        log_file_path = os.path.join(MAPPING_LOG_DIR, f"mapping_{mode}_{business_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log")
        try:
            # Open log file for subprocess stdout/stderr with line buffering for real-time logs
            log_file = open(log_file_path, 'w', buffering=1)  # Line buffered for immediate writes
            # Inherit environment variables from parent process
            process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd="/app/mapping",
                env=os.environ.copy(),  # Pass all environment variables
                close_fds=True  # Close all file descriptors except stdio (prevents descriptor leaks)
            )
            # Close file descriptor in parent process - subprocess has its own copy
            # This prevents resource leak in parent while subprocess can still write
            log_file.close()
        except (IOError, OSError) as file_error:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create log file: {str(file_error)}"
            )
        
        # Store the process ID in Redis for tracking
        await redis.set(f"mapping_process:{business_id}", str(process.pid), ex=MAPPING_PROCESS_TTL)
        await redis.set(f"mapping_log:{business_id}", log_file_path, ex=MAPPING_PROCESS_TTL)
        
        # Print to API logs for visibility
        print(f"=" * 80)
        print(f"Mapping pipeline started:")
        print(f"  Mode: {mode}")
        print(f"  Business ID: {business_id}")
        print(f"  Process ID: {process.pid}")
        print(f"  Log file: {log_file_path}")
        print(f"  Command: {' '.join(cmd)}")
        print(f"=" * 80)
        
        return {
            "status": 200,
            "message": f"Mapping pipeline started successfully in {mode} mode",
            "mapping_status": "running",
            "process_id": process.pid,
            "log_file": log_file_path,
            "mode": mode
        }
        
    except HTTPException:
        # Re-raise HTTPExceptions without modification (validation errors, etc.)
        raise
    except Exception as e:
        # Update the onboarding record to indicate mapping failed
        # Set current_step back to "connect" so user can retry
        if 'user_id' in locals():
            try:
                db.execute(
                    text("""
                        UPDATE onboarding 
                        SET mapping_status = :mapping_status,
                            mapping_error = :error,
                            current_step = :current_step
                        WHERE user_id = :user_id AND is_completed = false
                    """),
                    {
                        "mapping_status": "failed",
                        "error": str(e),
                        "current_step": "connect",
                        "user_id": user_id
                    }
                )
                db.commit()
            except Exception as db_error:
                print(f"Error updating database after failure: {db_error}")
        # Generic error message to avoid leaking internal details
        print(f"Error starting mapping: {e}")
        raise HTTPException(status_code=500, detail="Failed to start mapping pipeline")

@router.post("/cancel-mapping")
async def cancel_mapping(request: Request, db=Depends(get_db)):
    """
    Cancel the running mapping pipeline or manual mapping process.
    
    If cancelling during initial auto-mapping: reverts to connect step
    If cancelling during manual mapping application: stays on mapping page with completed status
    """
    try:
        body = await request.json()
        
        body_user_id = body.get("userId")
        if body_user_id is None:
            raise HTTPException(status_code=403, detail="Authentication Required")
        
        # Use authenticated user ID
        user_id = body_user_id
        
        # Optional parameter to indicate if this is during manual mapping
        during_manual_mapping = body.get("duringManualMapping", False)
        
        # Get the onboarding record
        onboarding = db.execute(
            text("SELECT onboarding_id, business_id, mapping_status, current_step FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        onboarding_id, business_id, mapping_status, current_step = onboarding_record
        
        if not business_id:
            raise HTTPException(status_code=400, detail="Business ID not found")
        
        # Get the process ID from Redis
        process_id_str = await redis.get(f"mapping_process:{business_id}")
        
        if process_id_str:
            try:
                process_id = int(process_id_str)
                # Use helper function to terminate the process
                await terminate_mapping_process(process_id)
            except ValueError:
                print(f"Invalid process ID: {process_id_str}")
        
        # Determine the behavior based on context
        # If user is on mapping page (current_step='mapping') or during_manual_mapping flag is set,
        # this means they're cancelling manual mapping application, so we preserve the completed state
        is_manual_mapping_cancellation = during_manual_mapping or current_step == 'mapping'
        
        # Update the database to indicate mapping was cancelled
        try:
            if is_manual_mapping_cancellation:
                # User was on mapping page reviewing results - restore to completed state
                # This allows them to stay on the mapping page and try again
                db.execute(
                    text("""
                        UPDATE onboarding 
                        SET mapping_status = :mapping_status,
                            current_step = :current_step
                        WHERE user_id = :user_id AND is_completed = false
                    """),
                    {
                        "mapping_status": "completed",  # Restore to completed since initial mapping finished
                        "current_step": "mapping",  # Keep them on mapping page
                        "user_id": user_id
                    }
                )
                message = "Manual mapping cancelled. You can adjust your mappings and try again."
            else:
                # User was running initial auto-mapping - revert to connect
                db.execute(
                    text("""
                        UPDATE onboarding 
                        SET mapping_status = :mapping_status,
                            mapping_error = :error,
                            mapping_completed_at = :completed_at,
                            current_step = :current_step
                        WHERE user_id = :user_id AND is_completed = false
                    """),
                    {
                        "mapping_status": "cancelled",
                        "error": "Mapping was cancelled by user",
                        "completed_at": datetime.utcnow(),
                        "current_step": "connect",
                        "user_id": user_id
                    }
                )
                message = "Mapping pipeline cancelled successfully"
            
            db.commit()
            
            # Clear Redis keys only after successful database commit
            await redis.delete(f"mapping_process:{business_id}")
            await redis.delete(f"mapping_log:{business_id}")

            # For initial auto-mapping cancellation (not manual review), also
            # tear down the streaming-layer resources that were created so far
            # (Debezium connector, Kafka topics, Spark checkpoint, Airflow DAG).
            if not is_manual_mapping_cancellation:
                from services.pipeline_service import PipelineService
                from routers.pipeline import websocket_manager as _ws_manager
                ps = PipelineService(db, _ws_manager)
                try:
                    await ps.cleanup_streaming_resources(business_id)
                except Exception as _ce:
                    print(f"Warning: streaming resource cleanup failed during cancel-mapping: {_ce}")
            
        except Exception as db_error:
            db.rollback()
            print(f"Error updating database during cancellation: {db_error}")
            raise HTTPException(
                status_code=500, 
                detail="Failed to update database during cancellation"
            )
        
        return {
            "status": 200,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Generic error message to avoid leaking internal details
        print(f"Error cancelling mapping: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel mapping pipeline")

@router.get("/mapping-status")
async def get_mapping_status(request: Request, userId: str, db=Depends(get_db)):
    """
    Get the current status of the mapping pipeline.
    Returns the status from the PostgreSQL onboarding table.
    """
    try:
        # Get authenticated user from middleware
        
        # Verify userId matches authenticated user
        if str(userId) is None:
            raise HTTPException(status_code=403, detail="Authentication Required")
        
        # Get the onboarding record
        onboarding = db.execute(
            text("""
                SELECT 
                    current_step,
                    mapping_status,
                    mapping_error,
                    mapping_started_at,
                    mapping_completed_at,
                    business_id
                FROM onboarding 
                WHERE user_id = :user_id
                AND is_completed = false
            """),
            {"user_id": userId}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        current_step, mapping_status, mapping_error, mapping_started_at, mapping_completed_at, business_id = onboarding_record
        
        # If mapping is running, check if the process is still alive
        if mapping_status == "running" and business_id:
            process_id_str = await redis.get(f"mapping_process:{business_id}")
            if process_id_str:
                process_dead = False
                try:
                    process_id = int(process_id_str)
                    # _mapping_pid_is_alive checks process existence on THIS host
                    # only. In a multi-replica deployment the PID may belong to a
                    # different container, so it only treats the process as dead
                    # when the OS explicitly says "no such process" (ESRCH) or the
                    # process is a zombie - never on EPERM (exists, different
                    # owner/container - assume alive) or an unreadable /proc.
                    process_dead = not _mapping_pid_is_alive(process_id)
                except ValueError:
                    process_dead = True  # Invalid PID stored in Redis

                if process_dead:
                    # Check if it completed successfully by looking at the MinIO bucket for mapped files
                    try:
                        # List objects in the mapped folder with timestamp checking
                        # Run in threadpool to avoid blocking async event loop
                        response = await run_in_threadpool(
                            s3.list_objects_v2,
                            Bucket=business_id,
                            Prefix="mapped/"
                        )
                        
                        # Check if files exist AND were created after mapping started
                        has_recent_files = False
                        if response.get('KeyCount', 0) > 0:
                            for obj in response.get('Contents', []):
                                last_modified = obj.get('LastModified')
                                # Convert timezone-aware LastModified to naive UTC for comparison
                                # mapping_started_at from DB is typically naive UTC
                                if last_modified:
                                    last_modified_naive = last_modified.replace(tzinfo=None)
                                # Ensure files were created after mapping started
                                if mapping_started_at and last_modified and last_modified_naive >= mapping_started_at:
                                    has_recent_files = True
                                    break
                        
                        if has_recent_files:
                            # Files exist in mapped folder and were created after mapping started
                            db.execute(
                                text("""
                                    UPDATE onboarding 
                                    SET mapping_status = :mapping_status,
                                        mapping_completed_at = :completed_at,
                                        current_step = :current_step
                                    WHERE user_id = :user_id AND is_completed = false
                                """),
                                {
                                    "mapping_status": "completed",
                                    "completed_at": datetime.utcnow(),
                                    "current_step": "mapping",
                                    "user_id": userId
                                }
                            )
                            db.commit()
                            mapping_status = "completed"
                            current_step = "mapping"
                        else:
                            # No files in mapped folder or files are stale, consider it failed
                            # Set current_step back to "connect" so user can retry
                            db.execute(
                                text("""
                                    UPDATE onboarding 
                                    SET mapping_status = :mapping_status,
                                        mapping_error = :error,
                                        current_step = :current_step
                                    WHERE user_id = :user_id AND is_completed = false
                                """),
                                {
                                    "mapping_status": "failed",
                                    "error": "Mapping process terminated without producing output",
                                    "current_step": "connect",
                                    "user_id": userId
                                }
                            )
                            db.commit()
                            mapping_status = "failed"
                            current_step = "connect"
                            mapping_error = "Mapping process terminated without producing output"
                    except Exception as e:
                        print(f"Error checking MinIO bucket: {e}")
        
        return {
            "status": 200,
            "current_step": current_step,
            "mapping_status": mapping_status,
            "mapping_error": mapping_error,
            "mapping_started_at": mapping_started_at.isoformat() if mapping_started_at else None,
            "mapping_completed_at": mapping_completed_at.isoformat() if mapping_completed_at else None
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/mapping-logs")
async def get_mapping_logs(request: Request, userId: str, db=Depends(get_db)):
    """
    Get the logs from the mapping pipeline subprocess.
    Returns the last N lines of the log file for the current mapping process.
    """
    try:
        if str(userId) is None:
            raise HTTPException(status_code=403, detail="Authentication required")
        
        # Get the business_id from onboarding record
        onboarding = db.execute(
            text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": userId}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record or not onboarding_record[0]:
            raise HTTPException(status_code=404, detail="Onboarding record or business ID not found")
        
        business_id = onboarding_record[0]
        
        # Get the log file path from Redis
        log_file_path = await redis.get(f"mapping_log:{business_id}")
        
        if not log_file_path:
            return {
                "status": 200,
                "logs": "",
                "message": "No active mapping process or log file not found"
            }
        
        # Check if log file exists
        if not os.path.exists(log_file_path):
            return {
                "status": 200,
                "logs": "",
                "message": f"Log file does not exist at {log_file_path}"
            }
        
        # Read the log file (last N lines to avoid large responses)
        try:
            with open(log_file_path, 'r') as f:
                lines = f.readlines()
                # Get last MAPPING_LOG_MAX_LINES lines
                last_lines = lines[-MAPPING_LOG_MAX_LINES:] if len(lines) > MAPPING_LOG_MAX_LINES else lines
                log_content = ''.join(last_lines)
                
            return {
                "status": 200,
                "logs": log_content,
                "log_file": log_file_path,
                "total_lines": len(lines),
                "returned_lines": len(last_lines)
            }
        except Exception as read_error:
            return {
                "status": 200,
                "logs": "",
                "message": f"Error reading log file: {str(read_error)}"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/mapping-logs-stream")
async def stream_mapping_logs(request: Request, userId: str, db=Depends(get_db)):
    """
    Stream mapping pipeline logs in real-time using Server-Sent Events (SSE).
    The frontend can consume this to show live output as the pipeline runs.
    """
    
    if str(userId) is None:
        raise HTTPException(status_code=403, detail="Authentication Required")
    
    # Get the business_id from onboarding record
    onboarding = db.execute(
        text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
        {"user_id": userId}
    )
    onboarding_record = onboarding.fetchone()
    
    if not onboarding_record or not onboarding_record[0]:
        raise HTTPException(status_code=404, detail="Onboarding record or business ID not found")
    
    business_id = onboarding_record[0]
    
    async def event_stream():
        """Generator function to stream log file content in real-time."""
        try:
            # Get the log file path from Redis
            log_file_path = await redis.get(f"mapping_log:{business_id}")
            
            if not log_file_path:
                yield f"data: {{\"type\": \"error\", \"message\": \"No active mapping process\"}}\n\n"
                return
            
            # Wait for log file to be created (max 5 seconds)
            wait_time = 0
            while not os.path.exists(log_file_path) and wait_time < 5:
                await asyncio.sleep(0.5)
                wait_time += 0.5
            
            if not os.path.exists(log_file_path):
                yield f"data: {{\"type\": \"error\", \"message\": \"Log file not found\"}}\n\n"
                return
            
            # Send start event
            yield f"data: {{\"type\": \"start\", \"message\": \"Connected to log stream\"}}\n\n"
            
            # Track the last position in the file
            last_position = 0
            
            # Timeout configuration: stop after no updates for this duration
            STREAM_TIMEOUT_SECONDS = 600  # 10 minutes
            POLL_INTERVAL_SECONDS = 0.5
            max_no_updates = int(STREAM_TIMEOUT_SECONDS / POLL_INTERVAL_SECONDS)  # 1200 iterations
            no_update_count = 0
            
            # Keep reading until process completes or no more updates
            while no_update_count < max_no_updates:
                try:
                    with open(log_file_path, 'r') as f:
                        # Seek to last known position
                        f.seek(last_position)
                        
                        # Read new lines
                        new_lines = f.readlines()
                        
                        if new_lines:
                            # Reset no-update counter
                            no_update_count = 0
                            
                            # Send each new line as an SSE message
                            for line in new_lines:
                                # Escape special characters for JSON
                                line_safe = json.dumps(line.rstrip('\n'))
                                yield f"data: {{\"type\": \"log\", \"line\": {line_safe}}}\n\n"
                            
                            # Update position
                            last_position = f.tell()
                        else:
                            # No new lines, increment counter
                            no_update_count += 1
                        
                        # Check if process is still running
                        process_id_str = await redis.get(f"mapping_process:{business_id}")
                        if process_id_str:
                            try:
                                process_id = int(process_id_str)
                                if not _mapping_pid_is_alive(process_id):
                                    raise OSError("mapping process no longer alive")
                                # Process is running, continue monitoring
                            except (OSError, ValueError):
                                # Process is done, read any remaining output and exit
                                f.seek(last_position)
                                remaining_lines = f.readlines()
                                for line in remaining_lines:
                                    line_safe = json.dumps(line.rstrip('\n'))
                                    yield f"data: {{\"type\": \"log\", \"line\": {line_safe}}}\n\n"
                                yield f"data: {{\"type\": \"complete\", \"message\": \"Process completed\"}}\n\n"
                                break
                    
                    # Small delay before checking for more output
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    
                except Exception as read_error:
                    yield f"data: {{\"type\": \"error\", \"message\": \"Error reading log: {str(read_error)}\"}}\n\n"
                    break
            
            # Send completion event if we timed out
            if no_update_count >= max_no_updates:
                # Kill the process if it's still running
                process_id_str = await redis.get(f"mapping_process:{business_id}")
                if process_id_str:
                    try:
                        process_id = int(process_id_str)
                        # Use helper function to terminate the process
                        await terminate_mapping_process(process_id)
                    except ValueError:
                        pass  # Invalid PID
                
                # Update database to indicate timeout
                # Note: Using SessionLocal here as we're in a streaming context and need a separate session
                try:
                    from database import SessionLocal
                    db_session = SessionLocal()
                    try:
                        db_session.execute(
                            text("""
                                UPDATE onboarding 
                                SET mapping_status = :mapping_status,
                                    mapping_error = :error,
                                    mapping_completed_at = :completed_at,
                                    current_step = :current_step
                                WHERE business_id = :business_id AND is_completed = false
                            """),
                            {
                                "mapping_status": "failed",
                                "error": "Timeout occurred while mapping your data, please try again.",
                                "completed_at": datetime.utcnow(),
                                "current_step": "connect",
                                "business_id": business_id
                            }
                        )
                        db_session.commit()
                    except Exception as commit_error:
                        db_session.rollback()
                        print(f"Error committing database changes after timeout: {commit_error}")
                    finally:
                        db_session.close()
                except Exception as db_error:
                    print(f"Error updating database after timeout: {db_error}")
                
                # Clear Redis keys
                await redis.delete(f"mapping_process:{business_id}")
                await redis.delete(f"mapping_log:{business_id}")
                
                yield f"data: {{\"type\": \"timeout\", \"message\": \"Timeout occurred while mapping your data, please try again.\"}}\n\n"
                
        except Exception as e:
            yield f"data: {{\"type\": \"error\", \"message\": \"Stream error: {str(e)}\"}}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )

@router.get("/mapping-results")
async def get_mapping_results(request: Request, userId: str, db=Depends(get_db)):
    """
    Get the mapping results (missing_cols and extra_cols) from the database.
    Returns formatted mapping data for the frontend.
    """
    try:
        # Verify userId matches authenticated user
        if str(userId) is None:
            raise HTTPException(status_code=403, detail="Cannot access another user's mapping results")
        
        # Get the mapping results from database
        onboarding = db.execute(
            text("""
                SELECT 
                    business_id,
                    mapping_results,
                    mapping_status
                FROM onboarding 
                WHERE user_id = :user_id
                AND is_completed = false
            """),
            {"user_id": userId}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        business_id, mapping_results, mapping_status = onboarding_record
        
        # If mapping is not completed yet, return appropriate status
        if mapping_status != "completed":
            return {
                "status": 200,
                "mapping_status": mapping_status,
                "missing_cols": [],
                "extra_cols": [],
                "all_fields_identified": False,
                "message": "Mapping in progress..."
            }
        
        # Parse mapping results from JSONB
        if mapping_results:
            missing_cols = mapping_results.get("missing_cols", [])
            extra_cols = mapping_results.get("extra_cols", [])
            
            # Check if all fields are identified
            all_fields_identified = len(missing_cols) == 0
            
            # Generate appropriate message based on results
            if all_fields_identified:
                message = "✅ All required columns have been successfully mapped!"
            else:
                missing_count = len(missing_cols)
                message = f"⚠️ {missing_count} required column{'s' if missing_count != 1 else ''} missing. Please review and map manually if needed."
            
            return {
                "status": 200,
                "mapping_status": mapping_status,
                "missing_cols": missing_cols,
                "extra_cols": extra_cols,
                "all_fields_identified": all_fields_identified,
                "message": message
            }
        else:
            # No mapping results yet, check Redis for cached results
            if business_id:
                mapping_results_str = await redis.get(f"mapping_results:{business_id}")
                if mapping_results_str:
                    mapping_results = json.loads(mapping_results_str)
                    missing_cols = mapping_results.get("missing_cols", [])
                    extra_cols = mapping_results.get("extra_cols", [])
                    
                    # Save to database for persistence
                    db.execute(
                        text("""
                            UPDATE onboarding 
                            SET mapping_results = :mapping_results
                            WHERE user_id = :user_id AND is_completed = false
                        """),
                        {
                            "mapping_results": json.dumps(mapping_results),  # ← SOLUTION: convert to JSON string
                            "user_id": userId
                        }
                    )
                    db.commit()
                    
                    all_fields_identified = len(missing_cols) == 0
                    
                    # Generate appropriate message based on results
                    if all_fields_identified:
                        message = "✅ All required columns have been successfully mapped!"
                    else:
                        missing_count = len(missing_cols)
                        message = f"⚠️ {missing_count} required column{'s' if missing_count != 1 else ''} missing. Please review and map manually if needed."
                    
                    return {
                        "status": 200,
                        "mapping_status": mapping_status,
                        "missing_cols": missing_cols,
                        "extra_cols": extra_cols,
                        "all_fields_identified": all_fields_identified,
                        "message": message
                    }
            
            # No results available
            return {
                "status": 200,
                "mapping_status": mapping_status,
                "missing_cols": [],
                "extra_cols": [],
                "all_fields_identified": False,
                "message": "No mapping results available yet."
            }
    
    except HTTPException:
        # Re-raise HTTPExceptions (401, 403, 404) without modification
        raise
    except Exception as e:
        print(f"Error getting mapping results: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/save-manual-mappings")
async def save_manual_mappings(request: Request, db=Depends(get_db)):
    """
    Save manual column mappings provided by the user.
    This endpoint just saves the mappings without processing.
    Use /apply-manual-mappings to actually apply them to the data.
    """
    try:
        body = await request.json()
        
        # Get userId from body and verify it matches authenticated user
        body_user_id = body.get("userId")
        if body_user_id is None:
            raise HTTPException(status_code=403, detail="Authentication Required")
        
        # Use authenticated user ID
        user_id = body_user_id
        manual_mappings = body.get("manualMappings", {})
        
        # Get the onboarding record
        onboarding = db.execute(
            text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        business_id = onboarding_record[0]

        mapping_results_row = db.execute(
            text("SELECT mapping_results FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id},
        ).fetchone()
        mapping_results_payload = mapping_results_row[0] if mapping_results_row else None
        extra_col_index = _build_extra_col_index(mapping_results_payload)
        normalized_manual_mappings = _normalize_table_mappings(manual_mappings, extra_col_index=extra_col_index)
        
        # Save manual mappings to database
        db.execute(
            text("""
                UPDATE onboarding 
                SET manual_mappings = :manual_mappings
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "manual_mappings": json.dumps(normalized_manual_mappings),
                "user_id": user_id
            }
        )
        db.commit()
        
        # Store in Redis for the mapping pipeline to use
        if business_id:
            await redis.set(
                f"manual_mappings:{business_id}",
                json.dumps(normalized_manual_mappings),
                ex=MAPPING_PROCESS_TTL,
            )
        
        return {
            "status": 200,
            "message": "Manual mappings saved successfully"
        }
    
    except HTTPException:
        # Re-raise HTTPExceptions (401, 403, 404) without modification
        raise
    except Exception as e:
        print(f"Error saving manual mappings: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/apply-manual-mappings")
async def apply_manual_mappings(request: Request, db=Depends(get_db)):
    """
    Apply manual column mappings to files in the mapped-temp folder.
    This moves files from mapped-temp to mapped folder after applying the mappings.
    Does NOT re-run the entire mapping pipeline - just renames columns and moves files.
    """
    try:
        body = await request.json()
        
        # Get userId from body and verify it matches authenticated user
        body_user_id = body.get("userId")
        if body_user_id is None:
            raise HTTPException(status_code=403, detail="Authentication Required")
        
        # Use authenticated user ID
        user_id = body_user_id
        manual_mappings = body.get("manualMappings", {})
        
        # Get the onboarding record
        onboarding = db.execute(
            text("SELECT business_id, mapping_status, auto_mappings FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()

        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")

        business_id, mapping_status, auto_mappings_payload = onboarding_record
        
        if not business_id:
            raise HTTPException(status_code=400, detail="Business ID not found")
        
        # Verify mapping is completed (initial mapping pipeline has finished)
        if mapping_status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot apply manual mappings - initial mapping status is '{mapping_status}', must be 'completed'"
            )
        
        mapping_results_row = db.execute(
            text("SELECT mapping_results FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id},
        ).fetchone()
        mapping_results_payload = mapping_results_row[0] if mapping_results_row else None
        extra_col_index = _build_extra_col_index(mapping_results_payload)
        normalized_manual_mappings = _normalize_table_mappings(manual_mappings, extra_col_index=extra_col_index)

        invalid_cross_table_mappings = _find_invalid_cross_table_mappings(normalized_manual_mappings)
        if invalid_cross_table_mappings:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Invalid cross-table manual mappings. Please select a source column with a valid join path.",
                    "invalidMappings": invalid_cross_table_mappings,
                },
            )

        # Save manual mappings to database first
        db.execute(
            text("""
                UPDATE onboarding 
                SET manual_mappings = :manual_mappings
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "manual_mappings": json.dumps(normalized_manual_mappings),
                "user_id": user_id
            }
        )
        db.commit()
        
        # Apply the manual mappings using the apply_manual_mappings.py script
        script_path = "/app/mapping/apply_manual_mappings.py"

        # Check if script exists
        if not os.path.exists(script_path):
            raise HTTPException(status_code=500, detail=f"Manual mapping script not found at {script_path}")

        # Parsed so the script can resolve a same-table mapping's raw primary-key
        # column (via the auto-mapping pass) and join on it instead of assuming
        # row order/count between mapped-temp and ingested/ — needed for db/api
        # initial loads whose ingested/ data can span multiple chunk files.
        auto_mappings = {}
        if isinstance(auto_mappings_payload, dict):
            auto_mappings = auto_mappings_payload
        elif isinstance(auto_mappings_payload, str):
            try:
                parsed_auto_mappings = json.loads(auto_mappings_payload)
                if isinstance(parsed_auto_mappings, dict):
                    auto_mappings = parsed_auto_mappings
            except Exception:
                pass

        # Build the command to run the manual mapping script
        cmd = [
            "python3",
            script_path,
            "--bucket-name", business_id,
            "--manual-mappings", json.dumps(normalized_manual_mappings),
            "--auto-mappings", json.dumps(auto_mappings),
        ]
        
        # Run the script synchronously (it's fast since it just renames columns)
        # Generous timeout to handle large datasets with many tables
        # Typically takes 1-2 seconds per table, timeout allows for slower I/O
        try:
            result = subprocess.run(
                cmd,
                cwd="/app/mapping",
                env=os.environ.copy(),
                capture_output=True,
                text=True,
                timeout=MANUAL_MAPPING_TIMEOUT_SECONDS
            )
            
            if result.returncode != 0:
                print(f"Manual mapping script failed: {result.stderr}")
                raise HTTPException(status_code=500, detail="Failed to apply manual mappings. Please check your mapping configuration and try again.")
            
            print(f"Manual mapping output: {result.stdout}")
            
            # Check for warnings in output
            if "⚠️" in result.stdout or "failed" in result.stdout.lower():
                print(f"⚠️  Manual mapping completed with warnings. Check output above.")
            
        except subprocess.TimeoutExpired:
            raise HTTPException(
                status_code=500, 
                detail="Manual mapping timed out. Please try again."
            )
        except Exception as e:
            print(f"Error running manual mapping script: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to apply manual mappings. Please try again.")
        
        # Get updated mapping results from Redis
        mapping_results_str = await redis.get(f"mapping_results:{business_id}")
        if mapping_results_str:
            mapping_results = json.loads(mapping_results_str)
        else:
            mapping_results = {"missing_cols": [], "extra_cols": []}
        
        # Update database with new mapping results
        db.execute(
            text("""
                UPDATE onboarding 
                SET mapping_results = :mapping_results
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "mapping_results": json.dumps(mapping_results),
                "user_id": user_id
            }
        )
        db.commit()
        
        # Clear the "use temp folder" flag now that mapped-temp → mapped
        # migration has been completed.  This ensures that the continuous
        # db_streaming / api_streaming Airflow DAG writes subsequent
        # micro-batches directly to mapped/ rather than mapped-temp/.
        try:
            await redis.delete(f"streaming_use_temp:{business_id}")
        except Exception as _redis_err:
            print(f"Warning: could not clear streaming_use_temp flag: {_redis_err}")

        # Build response message
        message = "Manual mappings applied successfully"
        if len(mapping_results.get("missing_cols", [])) > 0:
            message += f" ({len(mapping_results['missing_cols'])} columns still missing)"
        
        return {
            "status": 200,
            "message": message,
            "mapping_results": mapping_results,
            "business_id": business_id
        }
    
    except HTTPException:
        # Re-raise HTTPExceptions (401, 403, 404) without modification
        raise
    except Exception as e:
        print(f"Error applying manual mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/confirm-mapping")
async def confirm_mapping(request: Request, db=Depends(get_db)):
    """
    Confirm that user has reviewed the mapping results and accepts them.
    This marks the onboarding as complete and triggers the data processing pipeline.
    """
    try:
        body = await request.json()
        
        # Get userId from body and verify it matches authenticated user
        body_user_id = body.get("userId")
        if body_user_id is None:
            raise HTTPException(status_code=403, detail="Authentication Required")
        
        # Use authenticated user ID
        user_id = body_user_id
        
        # Get the onboarding record
        onboarding = db.execute(
            text("""
                SELECT business_id, mapping_status, current_step, ingestion_type, api_url, db_uri, db_tables,
                       manual_mappings, mapping_results, auto_mappings
                FROM onboarding
                WHERE user_id = :user_id AND is_completed = false
            """),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()

        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")

        (
            business_id,
            mapping_status,
            current_step,
            ingestion_type,
            stored_api_url,
            stored_db_uri,
            stored_db_tables,
            stored_manual_mappings,
            stored_mapping_results,
            stored_auto_mappings,
        ) = onboarding_record
        ingestion_type = (ingestion_type or "").strip().lower()

        def _as_dict(payload):
            if payload is None:
                return None
            if isinstance(payload, dict):
                return payload
            if isinstance(payload, str):
                try:
                    parsed = json.loads(payload)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    return None
            return None

        if ingestion_type not in ("batch", "db", "api"):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid ingestion_type in onboarding record. "
                    "Expected one of: batch, db, api."
                ),
            )
        
        # Verify mapping is completed
        if mapping_status != "completed":
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot confirm mapping - status is '{mapping_status}', must be 'completed'"
            )

        def _as_dict(payload):
            if payload is None:
                return None
            if isinstance(payload, dict):
                return payload
            if isinstance(payload, str):
                try:
                    parsed = json.loads(payload)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    return None
            return None

        # Resolve manual mappings BEFORE migration so the latest frontend
        # selections are used when moving mapped-temp/ -> mapped/.
        body_has_manual_mappings = "manualMappings" in body and isinstance(body.get("manualMappings"), dict)
        body_manual_mappings = body.get("manualMappings") if body_has_manual_mappings else None

        try:
            redis_manual_mappings = None
            redis_mm_str = await redis.get(f"manual_mappings:{business_id}")
            if redis_mm_str:
                redis_manual_mappings = _as_dict(redis_mm_str)
        except Exception as _redis_mm_err:
            print(f"Warning: could not read manual mappings from Redis in confirm-mapping: {_redis_mm_err}")
            redis_manual_mappings = None

        resolved_manual_mappings = (
            body_manual_mappings
            if body_has_manual_mappings
            else redis_manual_mappings
            if isinstance(redis_manual_mappings, dict)
            else _as_dict(stored_manual_mappings)
            if _as_dict(stored_manual_mappings) is not None
            else {}
        )

        extra_col_index = _build_extra_col_index(stored_mapping_results)
        normalized_manual_mappings = _normalize_table_mappings(
            resolved_manual_mappings,
            extra_col_index=extra_col_index,
        )
        normalized_auto_mappings = _normalize_table_mappings(
            _as_dict(stored_auto_mappings) or {},
            extra_col_index=extra_col_index,
        )

        combined_mappings = {
            table_name: dict(table_mappings)
            for table_name, table_mappings in normalized_auto_mappings.items()
            if isinstance(table_name, str) and isinstance(table_mappings, dict)
        }

        for table_name, table_mappings in normalized_manual_mappings.items():
            if table_name not in combined_mappings or not isinstance(combined_mappings[table_name], dict):
                combined_mappings[table_name] = {}
            combined_mappings[table_name].update(table_mappings)

        try:
            await redis.set(
                f"manual_mappings:{business_id}",
                json.dumps(normalized_manual_mappings),
                ex=MAPPING_PROCESS_TTL,
            )
        except Exception as _redis_set_err:
            print(f"Warning: could not persist manual mappings to Redis in confirm-mapping: {_redis_set_err}")

        db.execute(
            text("""
                UPDATE onboarding
                SET manual_mappings = :manual_mappings,
                    combined_mappings = :combined_mappings
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "manual_mappings": json.dumps(normalized_manual_mappings),
                "combined_mappings": json.dumps(combined_mappings),
                "user_id": user_id,
            },
        )
        db.commit()

        try:
            await redis.set(
                f"combined_mappings:{business_id}",
                json.dumps(combined_mappings),
                ex=MAPPING_PROCESS_TTL,
            )
        except Exception as _redis_set_combined_err:
            print(f"Warning: could not persist combined mappings to Redis in confirm-mapping: {_redis_set_combined_err}")
        
        # Safety guard: always clear the streaming_use_temp flag here so the
        # Airflow continuous streaming job never routes subsequent micro-batches
        # to mapped-temp, regardless of whether apply-manual-mappings was called
        # before confirm-mapping or there was a partial failure in that endpoint.
        try:
            await redis.delete(f"streaming_use_temp:{business_id}")
        except Exception as _redis_clear_err:
            print(f"Warning: could not clear streaming_use_temp flag in confirm-mapping: {_redis_clear_err}")

        # ── Guarantee mapped-temp/ → mapped/ migration ────────────────────────
        # The apply-manual-mappings endpoint is only called from the frontend when
        # the user filled in at least one manual mapping.  When the user had no
        # missing columns (auto-mapped perfectly) or skipped all suggestions, the
        # frontend calls confirm-mapping directly, skipping apply-manual-mappings.
        # To ensure mapped/ always exists before the Airflow streaming job starts,
        # we run apply_manual_mappings.py here unconditionally.  If it finds no
        # files in mapped-temp/ (because apply-manual-mappings already migrated
        # them), it exits non-zero with "No mapped files found" — that is treated
        # as a non-fatal success (migration already done).
        if ingestion_type in ("db", "api", "batch"):
            try:
                _script_path = "/app/mapping/apply_manual_mappings.py"
                if os.path.exists(_script_path):
                    _migration_result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: subprocess.run(
                            [
                                "python3", _script_path,
                                "--bucket-name", business_id,
                                "--manual-mappings", json.dumps(normalized_manual_mappings),
                            ],
                            cwd="/app/mapping",
                            env=os.environ.copy(),
                            capture_output=True,
                            text=True,
                            timeout=MANUAL_MAPPING_TIMEOUT_SECONDS,
                        )
                    )
                    if _migration_result.returncode == 0:
                        print(
                            f"✅ confirm-mapping: mapped-temp/ → mapped/ migration done "
                            f"for {business_id}"
                        )
                    else:
                        # "No mapped files found" means apply-manual-mappings already
                        # ran and cleaned up mapped-temp/ — treat as success.
                        _err_tail = (_migration_result.stderr or "")[-400:]
                        if "No mapped files found" in _err_tail or "No mapped files found" in (_migration_result.stdout or ""):
                            print(
                                f"ℹ️  confirm-mapping: mapped-temp/ already empty for "
                                f"{business_id} (apply-manual-mappings ran earlier)"
                            )
                        else:
                            print(
                                f"⚠️  confirm-mapping: migration script exited "
                                f"{_migration_result.returncode} for {business_id}: {_err_tail}"
                            )
            except Exception as _migration_err:
                # Non-fatal — log but do not abort confirm-mapping.
                print(
                    f"⚠️  confirm-mapping: non-fatal migration error for {business_id}: "
                    f"{_migration_err}"
                )

        print(f"User {user_id} confirmed mapping for business {business_id}")
        
        # Trigger data processing pipeline
        try:
            from services.pipeline_service import PipelineService
            from services.websocket_manager import WebSocketManager

            # Use global websocket manager from pipeline router
            from routers.pipeline import websocket_manager

            pipeline_service = PipelineService(db, websocket_manager)
            pipeline_id = await pipeline_service.start_pipeline(business_id, user_id)

            print(f"Pipeline {pipeline_id} started for business {business_id}")

        except Exception as pipeline_error:
            # Log pipeline start error but don't fail the mapping confirmation
            print(f"Warning: Failed to start pipeline automatically: {pipeline_error}")
            import traceback
            traceback.print_exc()

        # For API mode: trigger api_streaming so continuous polling +
        # StreamingNormalization starts under Airflow supervision.
        warnings = []

        if ingestion_type == "api":
            api_url_for_trigger = (stored_api_url or body.get("apiUrl") or "").strip()
            if not api_url_for_trigger:
                raise HTTPException(
                    status_code=400,
                    detail="API onboarding is missing api_url. Re-run /onboarding/start-mapping in api mode before confirming.",
                )

            if api_url_for_trigger != (stored_api_url or ""):
                db.execute(
                    text("""
                        UPDATE onboarding
                        SET api_url = :api_url
                        WHERE user_id = :user_id AND is_completed = false
                    """),
                    {"api_url": api_url_for_trigger, "user_id": user_id},
                )
                db.commit()

            try:
                await _trigger_api_streaming_dag(
                    business_id=business_id,
                    api_url=api_url_for_trigger,
                )
                print(f"api_streaming DAG triggered for business {business_id}")
            except Exception as dag_error:
                warning_message = f"Failed to trigger api_streaming DAG: {dag_error}"
                print(f"⚠️  {warning_message}")
                warnings.append(warning_message)

        # For DB mode: trigger db_streaming so Debezium CDC +
        # StreamingNormalization starts under Airflow supervision.
        if ingestion_type == "db":
            db_uri_for_trigger = (stored_db_uri or body.get("dbUri") or "").strip()
            db_tables_for_trigger = (stored_db_tables or body.get("dbTables") or "")
            if isinstance(db_tables_for_trigger, list):
                db_tables_for_trigger = ",".join([str(t).strip() for t in db_tables_for_trigger if str(t).strip()])
            else:
                db_tables_for_trigger = str(db_tables_for_trigger or "").strip()

            if not db_uri_for_trigger:
                raise HTTPException(
                    status_code=400,
                    detail="DB onboarding is missing db_uri. Re-run /onboarding/start-mapping in db mode before confirming.",
                )

            if not db_tables_for_trigger:
                raise HTTPException(
                    status_code=400,
                    detail="DB onboarding is missing db_tables. Re-run /onboarding/start-mapping in db mode before confirming.",
                )

            if db_uri_for_trigger != (stored_db_uri or "") or (db_tables_for_trigger or "") != (stored_db_tables or ""):
                db.execute(
                    text("""
                        UPDATE onboarding
                        SET db_uri = :db_uri, db_tables = :db_tables
                        WHERE user_id = :user_id AND is_completed = false
                    """),
                    {
                        "db_uri": db_uri_for_trigger,
                        "db_tables": db_tables_for_trigger,
                        "user_id": user_id,
                    },
                )
                db.commit()

            try:
                await _trigger_db_streaming_dag(
                    business_id=business_id,
                    db_uri=db_uri_for_trigger,
                    db_tables=db_tables_for_trigger,
                )
                print(f"db_streaming DAG triggered for business {business_id}")
            except Exception as dag_error:
                warning_message = f"Failed to trigger db_streaming DAG: {dag_error}"
                print(f"⚠️  {warning_message}")
                warnings.append(warning_message)

        # Mark onboarding complete only after streaming DAG trigger succeeded
        # (or immediately for batch mode).
        db.execute(
            text("""
                UPDATE onboarding
                SET is_completed = :is_completed,
                    current_step = :current_step
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "is_completed": True,
                "current_step": "mapping",
                "user_id": user_id,
            }
        )
        db.commit()
        
        return {
            "status": 200,
            "message": "Mapping confirmed and onboarding completed successfully",
            "is_completed": True,
            "business_id": business_id,
            "warnings": warnings,
        }
    
    except HTTPException:
        # Re-raise HTTPExceptions (401, 403, 404) without modification
        raise
    except Exception as e:
        print(f"Error confirming mapping: {e}")
        raise HTTPException(status_code=400, detail=str(e))
