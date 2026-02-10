import os
from fastapi import APIRouter, Depends, HTTPException, Response, Request, Query, UploadFile, File
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

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

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
        result = db.execute(text("SELECT onboarding_id, current_step FROM onboarding WHERE user_id = :user_id"), {"user_id": user_id})
        existing = result.fetchone()
        if existing:
            onboarding_id, current_step = existing
            return {
                "status": 200,
                "onboarding_id": onboarding_id,
                "current_step": current_step,
                "message": "Onboarding already exists."
            }
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
        onboarding = db.execute(text("SELECT onboarding_id FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        business_id = str(uuid.uuid4())
        businessCurrency = businessCurrency.get("value")
        businessRegion = businessRegion.get("value")
        db.execute(text("INSERT INTO businesses (business_id, user_id, business_name, business_region, business_currency) VALUES (:business_id, :user_id, :business_name, :business_region, :business_currency)"), {"business_id": business_id, "user_id": userId, "business_name": businessName, "business_region": businessRegion, "business_currency": businessCurrency})
        db.commit()
        db.execute(text("UPDATE onboarding SET current_step = :next_step, business_id = :business_id WHERE user_id = :user_id"), {"next_step": "data-type", "user_id": userId, "business_id": business_id})
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
        onboarding = db.execute(text("SELECT onboarding_id, business_id FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        db.execute(text("UPDATE onboarding SET current_step = :next_step, ingestion_type = :ingestion_type WHERE user_id = :user_id"), {"next_step": "connect", "ingestion_type": dataType, "user_id": userId})
        db.commit()
        return {"status": 200}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/get-data-type")
async def get_data_type(userId: str, db=Depends(get_db)):
    try:
        if not userId:
            raise HTTPException(status_code=400, detail="Authenticated User is required")
        onboarding = db.execute(text("SELECT ingestion_type, business_id FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
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
        
#         onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id"), {"user_id": user_id})
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
        
        onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
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
        
        onboarding = db.execute(text("SELECT business_id FROM onboarding WHERE user_id = :user_id"), {"user_id": user_id})
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
        onboarding = db.execute(text("SELECT onboarding_id, business_id FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
        onboarding_record = onboarding.fetchone()
        if not onboarding_record:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        db.execute(text("DELETE FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
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
        onboarding = db.execute(text("SELECT current_step FROM onboarding WHERE user_id = :user_id"), {"user_id": userId})
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
    Saves the pipeline state in PostgreSQL onboarding table.
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        mode = body.get("mode", "batch")  # Default to batch mode
        
        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")
        
        # Get the onboarding record
        onboarding = db.execute(
            text("SELECT onboarding_id, business_id, current_step, mapping_status FROM onboarding WHERE user_id = :user_id"),
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
            return {"status": 200, "message": "Mapping pipeline is already running", "mapping_status": "running"}
        
        # Update the onboarding record to indicate mapping is in progress
        db.execute(
            text("""
                UPDATE onboarding 
                SET current_step = :current_step,
                    mapping_status = :mapping_status,
                    mapping_started_at = :started_at,
                    mapping_error = NULL,
                    mapping_completed_at = NULL
                WHERE user_id = :user_id
            """),
            {
                "current_step": "mapping_in_progress",
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
            script_path,
            "--mode", mode,
            "--business-id", business_id
        ]
        
        # Start the mapping pipeline in background using subprocess.Popen
        # Use stdout and stderr redirection to avoid blocking
        log_file_path = f"/tmp/mapping_{business_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
        with open(log_file_path, 'w') as log_file:
            # Inherit environment variables from parent process
            process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd="/app/mapping",
                env=os.environ.copy()  # Pass all environment variables
            )
        
        # Store the process ID in Redis for tracking
        await redis.set(f"mapping_process:{business_id}", str(process.pid), ex=86400)
        await redis.set(f"mapping_log:{business_id}", log_file_path, ex=86400)
        
        return {
            "status": 200,
            "message": "Mapping pipeline started successfully",
            "mapping_status": "running",
            "process_id": process.pid
        }
        
    except Exception as e:
        # Update the onboarding record to indicate mapping failed
        if 'user_id' in locals():
            try:
                db.execute(
                    text("""
                        UPDATE onboarding 
                        SET mapping_status = :mapping_status,
                            mapping_error = :error
                        WHERE user_id = :user_id
                    """),
                    {
                        "mapping_status": "failed",
                        "error": str(e),
                        "user_id": user_id
                    }
                )
                db.commit()
            except:
                pass
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/mapping-status")
async def get_mapping_status(userId: str, db=Depends(get_db)):
    """
    Get the current status of the mapping pipeline.
    Returns the status from the PostgreSQL onboarding table.
    """
    try:
        if not userId:
            raise HTTPException(status_code=400, detail="userId is required")
        
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
                    os.kill(process_id, 0)  # This will raise an exception if process doesn't exist
                except (OSError, ValueError):
                    # Process is not running anymore, check if it completed successfully
                    # by looking at the MinIO bucket for mapped files
                    try:
                        # List objects in the mapped folder
                        response = s3.list_objects_v2(Bucket=business_id, Prefix="mapped/")
                        if response.get('KeyCount', 0) > 0:
                            # Files exist in mapped folder, consider it completed
                            db.execute(
                                text("""
                                    UPDATE onboarding 
                                    SET mapping_status = :mapping_status,
                                        mapping_completed_at = :completed_at,
                                        current_step = :current_step
                                    WHERE user_id = :user_id
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
                            # No files in mapped folder, consider it failed
                            db.execute(
                                text("""
                                    UPDATE onboarding 
                                    SET mapping_status = :mapping_status,
                                        mapping_error = :error
                                    WHERE user_id = :user_id
                                """),
                                {
                                    "mapping_status": "failed",
                                    "error": "Mapping process terminated without producing output",
                                    "user_id": userId
                                }
                            )
                            db.commit()
                            mapping_status = "failed"
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