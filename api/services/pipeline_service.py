"""
Pipeline service for orchestrating data processing pipeline execution.

This service handles:
- Starting and managing subprocess execution for cleaning, transformation, analysis, and ML
- Real-time progress tracking and updates to PostgreSQL
- WebSocket broadcasting of progress updates
- Error handling and logging
- Pipeline cancellation and cleanup
"""

import os
import asyncio
import subprocess
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
import json
from sqlalchemy import text


class PipelineService:
    """Service for managing data processing pipeline execution."""
    
    # Pipeline phases with their scripts and progress weights
    PIPELINE_PHASES = [
        {
            "name": "cleaning",
            "script": "cleaning/cleaning.py",
            "description": "Cleaning Data",
            "weight": 25  # Progress percentage weight
        },
        {
            "name": "transformation",
            "script": "transformation/transformation.py",
            "description": "Transforming & Aggregating Data",
            "weight": 30
        },
        {
            "name": "analysis",
            "script": "analysis/analysis.py",
            "description": "Analyzing Data",
            "weight": 30
        },
        {
            "name": "machine-learning",
            "script": "machine-learning/infer_all.py",
            "description": "Running ML Predictions",
            "weight": 15
        }
    ]
    
    def __init__(self, db, websocket_manager=None):
        """
        Initialize pipeline service.
        
        Args:
            db: Database connection
            websocket_manager: WebSocket manager for broadcasting updates (optional)
        """
        self.db = db
        self.websocket_manager = websocket_manager
        self.project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    async def start_pipeline(self, business_id: str, user_id: str) -> str:
        """
        Start the data processing pipeline for a business.
        
        Args:
            business_id: The business ID (used as bucket name)
            user_id: The user ID
            
        Returns:
            pipeline_id: The unique pipeline execution ID
        """
        pipeline_id = str(uuid.uuid4())
        
        # Create pipeline status record
        self.db.execute(
            text("""
                INSERT INTO pipeline_status 
                (pipeline_id, business_id, user_id, status, current_step, progress_percentage, started_at)
                VALUES (:pipeline_id, :business_id, :user_id, :status, :current_step, :progress, :started_at)
            """),
            {
                "pipeline_id": pipeline_id,
                "business_id": business_id,
                "user_id": user_id,
                "status": "running",
                "current_step": "Initializing Pipeline",
                "progress": 0,
                "started_at": datetime.now()
            }
        )
        self.db.commit()
        
        # Broadcast initial status via WebSocket
        await self._broadcast_progress(business_id, {
            "pipeline_id": pipeline_id,
            "status": "running",
            "current_step": "Initializing Pipeline",
            "progress": 0
        })
        
        # Start pipeline execution in background
        asyncio.create_task(self._execute_pipeline(pipeline_id, business_id, user_id))
        
        return pipeline_id
    
    async def _execute_pipeline(self, pipeline_id: str, business_id: str, user_id: str):
        """
        Execute the complete pipeline asynchronously.
        
        Args:
            pipeline_id: Pipeline execution ID
            business_id: Business ID (bucket name)
            user_id: User ID
        """
        try:
            cumulative_progress = 0
            process_ids = {}
            
            for phase in self.PIPELINE_PHASES:
                phase_name = phase["name"]
                
                # Check if pipeline was cancelled
                status = self._get_pipeline_status(pipeline_id)
                if status == "cancelled":
                    print(f"Pipeline {pipeline_id} was cancelled before {phase_name} phase")
                    return
                
                # Update status for this phase
                await self._update_progress(
                    pipeline_id, business_id,
                    status="running",
                    current_step=phase["description"],
                    progress=cumulative_progress
                )
                
                # Execute phase
                print(f"\n{'='*60}")
                print(f"Starting {phase_name} phase for business {business_id}")
                print(f"{'='*60}")
                
                success, process_id = await self._execute_phase(
                    phase, business_id, pipeline_id
                )
                
                if process_id:
                    process_ids[phase_name] = process_id
                
                if not success:
                    # Phase failed
                    error_msg = f"Pipeline failed during {phase_name} phase"
                    await self._update_progress(
                        pipeline_id, business_id,
                        status="failed",
                        current_step=phase["description"],
                        progress=cumulative_progress,
                        error_message=error_msg,
                        process_ids=process_ids
                    )
                    return
                
                # Update cumulative progress
                cumulative_progress += phase["weight"]
                await self._update_progress(
                    pipeline_id, business_id,
                    status="running",
                    current_step=phase["description"],
                    progress=min(cumulative_progress, 100),
                    process_ids=process_ids
                )
            
            # Pipeline completed successfully
            await self._update_progress(
                pipeline_id, business_id,
                status="completed",
                current_step="Pipeline completed successfully",
                progress=100,
                completed=True,
                process_ids=process_ids
            )
            
            print(f"\n{'='*60}")
            print(f"Pipeline {pipeline_id} completed successfully!")
            print(f"{'='*60}")
            
        except Exception as e:
            print(f"Pipeline execution error: {e}")
            import traceback
            traceback.print_exc()
            
            await self._update_progress(
                pipeline_id, business_id,
                status="failed",
                current_step="Pipeline Error",
                progress=cumulative_progress,
                error_message=str(e)
            )
    
    async def _execute_phase(
        self, phase: Dict[str, Any], business_id: str, pipeline_id: str
    ) -> tuple[bool, Optional[int]]:
        """
        Execute a single phase of the pipeline.
        
        Args:
            phase: Phase configuration dict
            business_id: Business ID (bucket name)
            pipeline_id: Pipeline execution ID
            
        Returns:
            tuple: (success: bool, process_id: Optional[int])
        """
        script_path = os.path.join(self.project_root, phase["script"])
        
        # Check if script exists
        if not os.path.exists(script_path):
            print(f"ERROR: Script not found: {script_path}")
            return False, None
        
        # Build command
        cmd = [
            "python3",
            script_path,
            "--bucket-name", business_id
        ]
        
        print(f"Executing: {' '.join(cmd)}")
        
        try:
            # Start subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.project_root
            )
            
            # Stream output in real-time
            stdout_task = asyncio.create_task(
                self._stream_output(process.stdout, f"[{phase['name']}] ", pipeline_id)
            )
            stderr_task = asyncio.create_task(
                self._stream_output(process.stderr, f"[{phase['name']} ERROR] ", pipeline_id)
            )
            
            # Wait for process to complete
            return_code = await process.wait()
            
            # Wait for output streaming to complete
            await asyncio.gather(stdout_task, stderr_task)
            
            if return_code == 0:
                print(f"✅ {phase['name']} phase completed successfully")
                return True, process.pid
            else:
                print(f"❌ {phase['name']} phase failed with return code {return_code}")
                return False, process.pid
                
        except Exception as e:
            print(f"Error executing {phase['name']} phase: {e}")
            import traceback
            traceback.print_exc()
            return False, None
    
    async def _stream_output(self, stream, prefix: str, pipeline_id: str):
        """
        Stream subprocess output in real-time.
        
        Args:
            stream: Subprocess stdout or stderr stream
            prefix: Prefix for log lines
            pipeline_id: Pipeline execution ID
        """
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                
                decoded_line = line.decode('utf-8').rstrip()
                if decoded_line:
                    print(f"{prefix}{decoded_line}")
                    
        except Exception as e:
            print(f"Error streaming output: {e}")
    
    async def _update_progress(
        self,
        pipeline_id: str,
        business_id: str,
        status: str,
        current_step: str,
        progress: int,
        error_message: Optional[str] = None,
        completed: bool = False,
        process_ids: Optional[Dict] = None
    ):
        """
        Update pipeline progress in database and broadcast via WebSocket.
        
        Args:
            pipeline_id: Pipeline execution ID
            business_id: Business ID
            status: Pipeline status
            current_step: Current step description
            progress: Progress percentage (0-100)
            error_message: Error message if failed
            completed: Whether pipeline is completed
            process_ids: Dict of process IDs for each phase
        """
        try:
            # Prepare update data
            update_data = {
                "pipeline_id": pipeline_id,
                "status": status,
                "current_step": current_step,
                "progress": min(progress, 100),
                "error_message": error_message
            }
            
            if completed:
                update_data["completed_at"] = datetime.now()
            
            if process_ids:
                update_data["process_ids"] = json.dumps(process_ids)
            
            # Build dynamic UPDATE query
            set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys() if k != "pipeline_id"])
            query = f"UPDATE pipeline_status SET {set_clause} WHERE pipeline_id = :pipeline_id"
            
            self.db.execute(text(query), update_data)
            self.db.commit()
            
            # Broadcast via WebSocket
            await self._broadcast_progress(business_id, {
                "pipeline_id": pipeline_id,
                "status": status,
                "current_step": current_step,
                "progress": progress,
                "error_message": error_message
            })
            
        except Exception as e:
            print(f"Error updating progress: {e}")
            import traceback
            traceback.print_exc()
    
    async def _broadcast_progress(self, business_id: str, data: Dict[str, Any]):
        """
        Broadcast progress update via WebSocket.
        
        Args:
            business_id: Business ID (used as channel/room)
            data: Progress data to broadcast
        """
        if self.websocket_manager:
            try:
                await self.websocket_manager.broadcast(
                    message=data,
                    business_id=business_id
                )
            except Exception as e:
                print(f"Error broadcasting progress: {e}")
    
    def _get_pipeline_status(self, pipeline_id: str) -> Optional[str]:
        """
        Get current pipeline status from database.
        
        Args:
            pipeline_id: Pipeline execution ID
            
        Returns:
            Status string or None
        """
        try:
            result = self.db.execute(
                text("SELECT status FROM pipeline_status WHERE pipeline_id = :pipeline_id"),
                {"pipeline_id": pipeline_id}
            ).fetchone()
            
            return result[0] if result else None
        except Exception as e:
            print(f"Error getting pipeline status: {e}")
            return None
    
    async def cancel_pipeline(self, pipeline_id: str, business_id: str) -> bool:
        """
        Cancel a running pipeline.
        
        Args:
            pipeline_id: Pipeline execution ID
            business_id: Business ID
            
        Returns:
            True if cancelled successfully, False otherwise
        """
        try:
            # Get process IDs before updating status
            result = self.db.execute(
                text("SELECT process_ids FROM pipeline_status WHERE pipeline_id = :pipeline_id"),
                {"pipeline_id": pipeline_id}
            ).fetchone()
            
            process_ids_json = result[0] if result else None
            
            # Update status to cancelled
            self.db.execute(
                text("""
                    UPDATE pipeline_status 
                    SET status = 'cancelled', 
                        current_step = 'Pipeline cancelled by user',
                        completed_at = :completed_at
                    WHERE pipeline_id = :pipeline_id
                """),
                {
                    "pipeline_id": pipeline_id,
                    "completed_at": datetime.now()
                }
            )
            self.db.commit()
            
            # Broadcast cancellation
            await self._broadcast_progress(business_id, {
                "pipeline_id": pipeline_id,
                "status": "cancelled",
                "current_step": "Pipeline cancelled by user",
                "progress": 0
            })
            
            # Terminate running processes if we have process IDs
            if process_ids_json:
                try:
                    import json
                    import signal
                    
                    process_ids = json.loads(process_ids_json)
                    print(f"Attempting to terminate processes: {process_ids}")
                    
                    for phase_name, pid in process_ids.items():
                        try:
                            # Send SIGTERM to gracefully terminate
                            os.kill(pid, signal.SIGTERM)
                            print(f"Sent SIGTERM to {phase_name} process (PID: {pid})")
                        except ProcessLookupError:
                            # Process already terminated
                            print(f"Process {pid} ({phase_name}) already terminated")
                        except Exception as e:
                            print(f"Error terminating process {pid} ({phase_name}): {e}")
                            
                except Exception as e:
                    print(f"Error parsing or terminating processes: {e}")
            
            return True
            
        except Exception as e:
            print(f"Error cancelling pipeline: {e}")
            return False
    
    async def cleanup_pipeline_data(self, business_id: str):
        """
        Clean up pipeline data from MinIO for a cancelled/failed pipeline.
        
        Args:
            business_id: Business ID (bucket name)
        """
        try:
            import boto3
            from botocore.client import Config
            
            # Initialize S3 client for MinIO
            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                config=Config(signature_version="s3v4"),
                region_name="us-east-1"
            )
            
            # Folders to clean up
            folders = ["cleaned", "transformed", "analytics", "ml-predictions"]
            
            for folder in folders:
                prefix = f"{folder}/"
                
                # List and delete objects
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=business_id, Prefix=prefix):
                    if "Contents" in page:
                        objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                        if objects:
                            s3_client.delete_objects(
                                Bucket=business_id,
                                Delete={"Objects": objects}
                            )
                            print(f"Deleted {len(objects)} objects from {business_id}/{folder}/")
            
            print(f"✅ Cleaned up pipeline data for business {business_id}")
            
        except Exception as e:
            print(f"Error cleaning up pipeline data: {e}")
            import traceback
            traceback.print_exc()
    
    def get_pipeline_status_info(self, business_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current pipeline status for a business.
        
        Args:
            business_id: Business ID
            
        Returns:
            Pipeline status info dict or None
        """
        try:
            result = self.db.execute(
                text("""
                    SELECT pipeline_id, status, current_step, progress_percentage, 
                           started_at, completed_at, error_message
                    FROM pipeline_status
                    WHERE business_id = :business_id
                    ORDER BY started_at DESC
                    LIMIT 1
                """),
                {"business_id": business_id}
            ).fetchone()
            
            if result:
                return {
                    "pipeline_id": result[0],
                    "status": result[1],
                    "current_step": result[2],
                    "progress": result[3],
                    "started_at": result[4].isoformat() if result[4] else None,
                    "completed_at": result[5].isoformat() if result[5] else None,
                    "error_message": result[6]
                }
            
            return None
            
        except Exception as e:
            print(f"Error getting pipeline status info: {e}")
            return None
