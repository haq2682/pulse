"""
Service for managing the data processing pipeline execution.
"""
import os
import uuid
import asyncio
import subprocess
from datetime import datetime
from typing import Optional, Dict
from sqlalchemy import text
from database import SessionLocal
import aioredis
import boto3
from botocore.client import Config

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# Pipeline phase configurations
PIPELINE_PHASES = [
    {
        "name": "cleaning",
        "script_path": "/home/runner/work/pulse/pulse/cleaning/cleaning.py",
        "description": "Cleaning data",
        "progress_weight": 25
    },
    {
        "name": "transformation",
        "script_path": "/home/runner/work/pulse/pulse/transformation/transformation.py",
        "description": "Transforming data",
        "progress_weight": 25
    },
    {
        "name": "analysis",
        "script_path": "/home/runner/work/pulse/pulse/analysis/analysis.py",
        "description": "Analyzing data",
        "progress_weight": 25
    },
    {
        "name": "machine-learning",
        "script_path": "/home/runner/work/pulse/pulse/machine-learning/infer_all.py",
        "description": "Running ML inference",
        "progress_weight": 25
    }
]


async def update_pipeline_progress(
    db,
    pipeline_id: str,
    status: str,
    current_phase: Optional[str] = None,
    progress_percentage: Optional[int] = None,
    step_description: Optional[str] = None,
    error_message: Optional[str] = None
):
    """
    Update pipeline execution progress in database.
    
    Args:
        db: Database session
        pipeline_id: Pipeline execution ID
        status: Pipeline status (running, completed, failed, cancelled)
        current_phase: Current phase name
        progress_percentage: Progress percentage (0-100)
        step_description: Description of current step
        error_message: Error message if failed
    """
    update_fields = ["status = :status"]
    params = {"pipeline_id": pipeline_id, "status": status}
    
    if current_phase is not None:
        update_fields.append("current_phase = :current_phase")
        params["current_phase"] = current_phase
    
    if progress_percentage is not None:
        update_fields.append("progress_percentage = :progress_percentage")
        params["progress_percentage"] = progress_percentage
    
    if step_description is not None:
        update_fields.append("step_description = :step_description")
        params["step_description"] = step_description
    
    if error_message is not None:
        update_fields.append("error_message = :error_message")
        params["error_message"] = error_message
    
    if status in ["completed", "failed", "cancelled"]:
        update_fields.append("completed_at = :completed_at")
        params["completed_at"] = datetime.utcnow()
    
    query = f"UPDATE pipeline_executions SET {', '.join(update_fields)} WHERE pipeline_id = :pipeline_id"
    
    db.execute(text(query), params)
    db.commit()
    
    # Also store in Redis for WebSocket broadcasting
    await redis.setex(
        f"pipeline_status:{pipeline_id}",
        3600,  # 1 hour TTL
        f"{status}|{current_phase or ''}|{progress_percentage or 0}|{step_description or ''}"
    )


async def run_pipeline_phase(
    phase_config: Dict,
    business_id: str,
    pipeline_id: str,
    db,
    phase_index: int
):
    """
    Run a single phase of the pipeline.
    
    Args:
        phase_config: Phase configuration dictionary
        business_id: Business ID (used as bucket name)
        pipeline_id: Pipeline execution ID
        db: Database session
        phase_index: Index of current phase (for progress calculation)
        
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    phase_name = phase_config["name"]
    script_path = phase_config["script_path"]
    description = phase_config["description"]
    
    # Calculate progress percentage
    base_progress = sum(PIPELINE_PHASES[i]["progress_weight"] for i in range(phase_index))
    phase_weight = phase_config["progress_weight"]
    
    # Update status to running this phase
    await update_pipeline_progress(
        db,
        pipeline_id,
        status="running",
        current_phase=phase_name,
        progress_percentage=base_progress,
        step_description=f"Starting {description}"
    )
    
    try:
        # Prepare command and environment based on phase
        env = os.environ.copy()
        env["BUCKET_NAME"] = business_id
        
        if phase_name == "machine-learning":
            # ML script accepts --bucket-name argument
            cmd = ["python", script_path, "--bucket-name", business_id]
        else:
            # Other scripts use environment variable
            cmd = ["python", script_path]
        
        # Run subprocess
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        
        # Store process ID in Redis
        await redis.setex(f"pipeline_process:{pipeline_id}:{phase_name}", 3600, str(process.pid))
        
        # Wait for completion
        stdout, stderr = await process.communicate()
        
        # Check return code
        if process.returncode != 0:
            error_msg = f"Phase '{phase_name}' failed with return code {process.returncode}. Error: {stderr.decode('utf-8', errors='ignore')}"
            print(f"❌ {error_msg}")
            
            await update_pipeline_progress(
                db,
                pipeline_id,
                status="failed",
                current_phase=phase_name,
                progress_percentage=base_progress,
                error_message=error_msg
            )
            
            return False, error_msg
        
        # Phase completed successfully
        final_progress = base_progress + phase_weight
        print(f"✅ Phase '{phase_name}' completed successfully")
        print(f"   Output: {stdout.decode('utf-8', errors='ignore')[:500]}")
        
        await update_pipeline_progress(
            db,
            pipeline_id,
            status="running",
            current_phase=phase_name,
            progress_percentage=final_progress,
            step_description=f"Completed {description}"
        )
        
        # Clean up Redis process ID
        await redis.delete(f"pipeline_process:{pipeline_id}:{phase_name}")
        
        return True, None
        
    except Exception as e:
        error_msg = f"Phase '{phase_name}' encountered an error: {str(e)}"
        print(f"❌ {error_msg}")
        
        await update_pipeline_progress(
            db,
            pipeline_id,
            status="failed",
            current_phase=phase_name,
            error_message=error_msg
        )
        
        return False, error_msg


async def execute_pipeline(business_id: str, user_id: str, db=None):
    """
    Execute the complete data processing pipeline.
    
    Args:
        business_id: Business ID (used as bucket name)
        user_id: User ID
        db: Optional database session (will create new one if not provided)
        
    Returns:
        str: Pipeline execution ID
    """
    # Create new database session for background task
    if db is None:
        db = SessionLocal()
    
    try:
        # Create pipeline execution record
        pipeline_id = str(uuid.uuid4())
        
        db.execute(
            text("""
                INSERT INTO pipeline_executions 
                (pipeline_id, business_id, user_id, status, progress_percentage, step_description)
                VALUES (:pipeline_id, :business_id, :user_id, :status, :progress, :description)
            """),
            {
                "pipeline_id": pipeline_id,
                "business_id": business_id,
                "user_id": user_id,
                "status": "running",
                "progress": 0,
                "description": "Pipeline started"
            }
        )
        db.commit()
        
        print(f"🚀 Starting pipeline execution {pipeline_id} for business {business_id}")
        
        # Execute each phase sequentially
        for idx, phase_config in enumerate(PIPELINE_PHASES):
            success, error_msg = await run_pipeline_phase(
                phase_config,
                business_id,
                pipeline_id,
                db,
                idx
            )
            
            if not success:
                print(f"❌ Pipeline {pipeline_id} failed at phase {phase_config['name']}")
                return pipeline_id
        
        # All phases completed successfully
        await update_pipeline_progress(
            db,
            pipeline_id,
            status="completed",
            progress_percentage=100,
            step_description="Pipeline has completed execution"
        )
        
        print(f"✅ Pipeline {pipeline_id} completed successfully")
        
        return pipeline_id
    
    finally:
        # Close database session if we created it
        if db:
            db.close()


async def cancel_pipeline(pipeline_id: str, business_id: str, db):
    """
    Cancel a running pipeline and clean up resources.
    
    Args:
        pipeline_id: Pipeline execution ID
        business_id: Business ID
        db: Database session
    """
    # Get current status
    result = db.execute(
        text("SELECT status, current_phase FROM pipeline_executions WHERE pipeline_id = :pipeline_id"),
        {"pipeline_id": pipeline_id}
    )
    row = result.fetchone()
    
    if not row:
        raise ValueError("Pipeline not found")
    
    status, current_phase = row
    
    if status not in ["running"]:
        raise ValueError(f"Cannot cancel pipeline with status '{status}'")
    
    # Kill running processes
    for phase in PIPELINE_PHASES:
        process_id_str = await redis.get(f"pipeline_process:{pipeline_id}:{phase['name']}")
        if process_id_str:
            try:
                import signal
                os.kill(int(process_id_str), signal.SIGTERM)
                await asyncio.sleep(1)
                try:
                    os.kill(int(process_id_str), 0)
                    os.kill(int(process_id_str), signal.SIGKILL)
                except OSError:
                    pass
            except (OSError, ValueError) as e:
                print(f"Could not kill process {process_id_str}: {e}")
            
            await redis.delete(f"pipeline_process:{pipeline_id}:{phase['name']}")
    
    # Clean up MinIO folders
    folders_to_clean = ["cleaned", "transformed", "analytics", "machine-learning"]
    for folder in folders_to_clean:
        try:
            # List and delete objects in folder
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=business_id, Prefix=f"{folder}/"):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    s3.delete_objects(Bucket=business_id, Delete={"Objects": objects})
                    print(f"Cleaned up {len(objects)} objects from {folder}/")
        except Exception as e:
            print(f"Error cleaning up {folder}/: {e}")
    
    # Update database
    await update_pipeline_progress(
        db,
        pipeline_id,
        status="cancelled",
        step_description="Pipeline cancelled by user"
    )
    
    # Clean up Redis
    await redis.delete(f"pipeline_status:{pipeline_id}")
    
    print(f"Pipeline {pipeline_id} cancelled successfully")
