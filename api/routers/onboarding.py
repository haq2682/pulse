import os
import signal
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

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MAPPING_LOG_DIR = os.getenv("MAPPING_LOG_DIR", "/tmp")
MAPPING_PROCESS_TTL = 86400  # 24 hours in seconds
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

async def _trigger_api_streaming_dag(business_id: str, api_url: str) -> None:
    """
    Trigger the api_streaming Airflow DAG via the Airflow REST API.

    Called from confirm-mapping for API-mode businesses so that the user's
    external endpoint is polled continuously (with Airflow auto-restart).
    The initial bounded subprocess started during start-mapping has already
    exited (poll_duration window), so there is no duplication.
    """
    import urllib.request
    import urllib.error
    import json as _json

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    airflow_user = os.getenv("AIRFLOW_USERNAME", "admin")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "admin")
    poll_interval = int(os.getenv("API_POLL_INTERVAL", "30"))

    url = f"{airflow_base}/api/v1/dags/api_streaming/dagRuns"
    payload = _json.dumps({
        "conf": {
            "bucket":        business_id,
            "api_url":       api_url,
            "poll_interval": poll_interval,
        }
    }).encode()

    credentials = f"{airflow_user}:{airflow_password}"
    import base64
    encoded = base64.b64encode(credentials.encode()).decode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Basic {encoded}",
        },
        method="POST",
    )

    def _do_request():
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read()

    await run_in_threadpool(_do_request)


async def _trigger_db_streaming_dag(business_id: str, db_uri: str, db_tables: str) -> None:
    """
    Trigger the db_streaming Airflow DAG via the Airflow REST API.

    Called from confirm-mapping for DB-mode businesses so that Debezium CDC
    streaming runs continuously under Airflow supervision (with auto-restart).
    The initial bounded subprocess started during start-mapping (--trigger-once)
    has already exited, so there is no duplication.
    """
    import urllib.request
    import urllib.error
    import json as _json

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    airflow_user = os.getenv("AIRFLOW_USERNAME", "admin")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "admin")

    url = f"{airflow_base}/api/v1/dags/db_streaming/dagRuns"
    payload = _json.dumps({
        "conf": {
            "bucket":    business_id,
            "db_uri":    db_uri,
            "db_tables": db_tables,
        }
    }).encode()

    credentials = f"{airflow_user}:{airflow_password}"
    import base64
    encoded = base64.b64encode(credentials.encode()).decode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Basic {encoded}",
        },
        method="POST",
    )

    def _do_request():
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read()

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

# @router.post("/upload-chunk")
# async def upload_chunk(
#     request: Request,
#     db=Depends(get_db)
# ):
#     try:
#         form = await request.form()
#         chunk = await form["chunk"].read()
#         chunk_index = int(form["chunkIndex"])
#         total_chunks = int(form["totalChunks"])
#         file_id = form["fileId"]
#         file_name = form["fileName"]
#         file_size = int(form["fileSize"])
#         file_type = form["fileType"]
#         user_id = form["userId"]
        
#         onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id AND is_completed = false"), {"user_id": user_id})
#         onboarding_record = onboarding.fetchone()
#         if not onboarding_record:
#             raise HTTPException(status_code=404, detail="Onboarding record not found")
        
#         business_id = onboarding_record[0]
#         s3_key = f"ingested/{file_name}"
        
#         if chunk_index == 0:
#             existing = db.execute(text("SELECT file_id FROM uploaded_files WHERE file_id = :file_id"), {"file_id": file_id})
#             if not existing.fetchone():
#                 db.execute(text(
#                     "INSERT INTO uploaded_files (file_id, business_id, file_name, file_size, file_type, s3_key, upload_status) "
#                     "VALUES (:file_id, :business_id, :file_name, :file_size, :file_type, :s3_key, :upload_status)"
#                 ), {"file_id": file_id, "business_id": business_id, "file_name": file_name, "file_size": file_size, "file_type": file_type, "s3_key": s3_key, "upload_status": "uploading"})
#                 db.commit()
            
#             multipart = s3.create_multipart_upload(Bucket=business_id, Key=s3_key)
#             upload_id = multipart["UploadId"]
#             await redis.set(f"upload:{file_id}:upload_id", upload_id, ex=86400)
#         else:
#             upload_id = await redis.get(f"upload:{file_id}:upload_id")
        
#         part_number = chunk_index + 1
#         part = s3.upload_part(
#             Bucket=business_id,
#             Key=s3_key,
#             PartNumber=part_number,
#             UploadId=upload_id,
#             Body=chunk
#         )
        
#         parts_key = f"upload:{file_id}:parts"
#         parts_json = await redis.get(parts_key) or "[]"
#         parts = json.loads(parts_json)
#         parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
#         await redis.set(parts_key, json.dumps(parts), ex=86400)
        
#         if chunk_index == total_chunks - 1:
#             parts = sorted(parts, key=lambda x: x["PartNumber"])
#             s3.complete_multipart_upload(
#                 Bucket=business_id,
#                 Key=s3_key,
#                 UploadId=upload_id,
#                 MultipartUpload={"Parts": parts}
#             )
            
#             db.execute(text("UPDATE uploaded_files SET upload_status = :status WHERE file_id = :file_id"), 
#                       {"status": "completed", "file_id": file_id})
#             db.commit()
            
#             await redis.delete(f"upload:{file_id}:upload_id")
#             await redis.delete(f"upload:{file_id}:parts")
        
#         return {"status": 200, "chunkIndex": chunk_index}
#     except Exception as e:
#         if 'file_id' in locals():
#             db.execute(text("UPDATE uploaded_files SET upload_status = :status WHERE file_id = :file_id"), 
#                       {"status": "failed", "file_id": file_id})
#             db.commit()
#         raise HTTPException(status_code=400, detail=str(e))

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
            from utils.connectivity_validator import validate_database_connection
            success, message = await run_in_threadpool(validate_database_connection, db_uri, 10)
            if not success:
                raise HTTPException(status_code=400, detail=message)
            print(f"Database connectivity validated: {message}")
            
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
                    os.kill(int(mapping_pid), 0)
                    process_still_running = True
                except OSError:
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
                try:
                    process_id = int(process_id_str)
                    # Check if process is still running
                    # os.kill with signal 0 doesn't kill the process, just checks if it exists
                    os.kill(process_id, 0)
                except (OSError, ValueError, PermissionError):
                    # Process is not running anymore:
                    # - OSError: Process doesn't exist or already terminated
                    # - ValueError: Invalid PID
                    # - PermissionError: Process exists but owned by different user (not accessible)
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
                                # Check if process exists using signal 0 (Unix-specific)
                                # This doesn't kill the process, just tests if it exists
                                os.kill(process_id, 0)
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
        
        # Save manual mappings to database
        db.execute(
            text("""
                UPDATE onboarding 
                SET manual_mappings = :manual_mappings
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "manual_mappings": json.dumps(manual_mappings),
                "user_id": user_id
            }
        )
        db.commit()
        
        # Store in Redis for the mapping pipeline to use
        if business_id:
            await redis.set(f"manual_mappings:{business_id}", json.dumps(manual_mappings), ex=MAPPING_PROCESS_TTL)
        
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
            text("SELECT business_id, mapping_status FROM onboarding WHERE user_id = :user_id AND is_completed = false"),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()
        
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        business_id, mapping_status = onboarding_record
        
        if not business_id:
            raise HTTPException(status_code=400, detail="Business ID not found")
        
        # Verify mapping is completed (initial mapping pipeline has finished)
        if mapping_status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot apply manual mappings - initial mapping status is '{mapping_status}', must be 'completed'"
            )
        
        # Save manual mappings to database first
        db.execute(
            text("""
                UPDATE onboarding 
                SET manual_mappings = :manual_mappings
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "manual_mappings": json.dumps(manual_mappings),
                "user_id": user_id
            }
        )
        db.commit()
        
        # Apply the manual mappings using the apply_manual_mappings.py script
        script_path = "/app/mapping/apply_manual_mappings.py"
        
        # Check if script exists
        if not os.path.exists(script_path):
            raise HTTPException(status_code=500, detail=f"Manual mapping script not found at {script_path}")
        
        # Build the command to run the manual mapping script
        cmd = [
            "python3",
            script_path,
            "--bucket-name", business_id,
            "--manual-mappings", json.dumps(manual_mappings)
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
                error_msg = f"Manual mapping failed: {result.stderr}"
                print(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
            
            print(f"Manual mapping output: {result.stdout}")
            
            # Check for warnings in output
            if "⚠️" in result.stdout or "failed" in result.stdout.lower():
                print(f"⚠️  Manual mapping completed with warnings. Check output above.")
            
        except subprocess.TimeoutExpired:
            raise HTTPException(
                status_code=500, 
                detail=f"Manual mapping took too long (timeout after {MANUAL_MAPPING_TIMEOUT_SECONDS} seconds)"
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to run manual mapping: {str(e)}")
        
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
                SELECT business_id, mapping_status, current_step, ingestion_type, api_url, db_uri, db_tables
                FROM onboarding
                WHERE user_id = :user_id AND is_completed = false
            """),
            {"user_id": user_id}
        )
        onboarding_record = onboarding.fetchone()

        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")

        business_id, mapping_status, current_step, ingestion_type, stored_api_url, stored_db_uri, stored_db_tables = onboarding_record
        
        # Verify mapping is completed
        if mapping_status != "completed":
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot confirm mapping - status is '{mapping_status}', must be 'completed'"
            )
        
        # Update onboarding to mark as complete
        db.execute(
            text("""
                UPDATE onboarding 
                SET is_completed = :is_completed,
                    current_step = :current_step
                WHERE user_id = :user_id AND is_completed = false
            """),
            {
                "is_completed": True,
                "current_step": "mapping",  # Keep on mapping step but mark as complete
                "user_id": user_id
            }
        )
        db.commit()
        
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

        # For API mode: trigger the api_streaming Airflow DAG so the user's
        # external API is polled continuously (with Airflow auto-restart on crash).
        if ingestion_type == "api" and stored_api_url:
            try:
                await _trigger_api_streaming_dag(
                    business_id=business_id,
                    api_url=stored_api_url,
                )
                print(f"api_streaming DAG triggered for business {business_id}")
            except Exception as dag_error:
                # Non-fatal: log and continue — the user's onboarding is complete.
                print(f"Warning: Failed to trigger api_streaming DAG: {dag_error}")
                import traceback
                traceback.print_exc()

        # For DB mode: trigger the db_streaming Airflow DAG so Debezium CDC
        # streams continuously under Airflow supervision (with auto-restart).
        if ingestion_type == "db" and stored_db_uri:
            try:
                await _trigger_db_streaming_dag(
                    business_id=business_id,
                    db_uri=stored_db_uri,
                    db_tables=stored_db_tables or "",
                )
                print(f"db_streaming DAG triggered for business {business_id}")
            except Exception as dag_error:
                # Non-fatal: log and continue — the user's onboarding is complete.
                print(f"Warning: Failed to trigger db_streaming DAG: {dag_error}")
                import traceback
                traceback.print_exc()
        
        return {
            "status": 200,
            "message": "Mapping confirmed and onboarding completed successfully",
            "is_completed": True,
            "business_id": business_id
        }
    
    except HTTPException:
        # Re-raise HTTPExceptions (401, 403, 404) without modification
        raise
    except Exception as e:
        print(f"Error confirming mapping: {e}")
        raise HTTPException(status_code=400, detail=str(e))
