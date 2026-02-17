"""
Streaming Pipeline Service for real-time data processing with CDC and Kafka.

This service handles:
- Streaming data processing with Spark Structured Streaming
- Integration with Kafka topics from CDC (DB mode) and API ingestion
- Real-time progress tracking and updates
- WebSocket broadcasting of streaming pipeline status
"""

import os
import asyncio
import subprocess
import uuid
import json
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text


class StreamingPipelineService:
    """Service for managing streaming data processing pipeline execution."""
    
    # Streaming pipeline phases with their scripts and progress weights
    STREAMING_PIPELINE_PHASES = [
        {
            "name": "streaming_cleaning",
            "script": "cleaning/streaming_cleaning.py",
            "description": "Real-time Data Cleaning (Streaming)",
            "weight": 25,
            "is_continuous": True  # Runs continuously
        },
        {
            "name": "streaming_transformation",
            "script": "transformation/streaming_transformation.py",
            "description": "Real-time Transformation & Aggregation (Streaming)",
            "weight": 35,
            "is_continuous": True
        },
        {
            "name": "streaming_ml_inference",
            "script": "machine-learning/streaming_ml_inference.py",
            "description": "Real-time ML Predictions (Streaming)",
            "weight": 40,
            "is_continuous": True
        }
    ]
    
    def __init__(self, db, websocket_manager=None):
        """
        Initialize streaming pipeline service.
        
        Args:
            db: Database connection
            websocket_manager: WebSocket manager for broadcasting updates (optional)
        """
        self.db = db
        self.websocket_manager = websocket_manager
        
        # Determine project root
        current_file = os.path.abspath(__file__)
        
        if '/api/services/' in current_file or current_file.endswith('/api/services/streaming_pipeline_service.py'):
            # Development structure: go up 3 levels
            self.project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
        elif '/services/' in current_file:
            # Container structure: go up 2 levels
            self.project_root = os.path.dirname(os.path.dirname(current_file))
        else:
            # Fallback
            self.project_root = os.path.dirname(current_file)
        
        print(f"StreamingPipelineService initialized with project_root: {self.project_root}")
    
    async def start_streaming_pipeline(
        self, 
        business_id: str, 
        user_id: str,
        mode: str = "db"  # db, api, or batch
    ) -> str:
        """
        Start the streaming data processing pipeline for a business.
        
        Args:
            business_id: The business ID (used as bucket name and for Kafka topics)
            user_id: The user ID
            mode: Ingestion mode - "db" (CDC), "api", or "batch"
            
        Returns:
            pipeline_id: The unique pipeline execution ID
        """
        pipeline_id = str(uuid.uuid4())
        
        # Create pipeline status record
        self.db.execute(
            text("""
                INSERT INTO pipeline_status 
                (pipeline_id, business_id, user_id, status, current_step, progress_percentage, 
                 started_at, pipeline_mode, pipeline_type)
                VALUES (:pipeline_id, :business_id, :user_id, :status, :current_step, 
                        :progress_percentage, :started_at, :mode, :type)
            """),
            {
                "pipeline_id": pipeline_id,
                "business_id": business_id,
                "user_id": user_id,
                "status": "running",
                "current_step": "Initializing Streaming Pipeline",
                "progress_percentage": 0,
                "started_at": datetime.now(),
                "mode": mode,
                "type": "streaming"
            }
        )
        self.db.commit()
        
        # Broadcast initial status
        await self._broadcast_progress(business_id, {
            "pipeline_id": pipeline_id,
            "status": "running",
            "current_step": "Initializing Streaming Pipeline",
            "progress": 0,
            "mode": mode,
            "type": "streaming"
        })
        
        # Start streaming pipeline execution in background
        asyncio.create_task(
            self._execute_streaming_pipeline_with_new_connection(
                pipeline_id, business_id, user_id, mode
            )
        )
        
        return pipeline_id
    
    async def _execute_streaming_pipeline_with_new_connection(
        self, 
        pipeline_id: str, 
        business_id: str, 
        user_id: str,
        mode: str
    ):
        """
        Wrapper to execute streaming pipeline with a new database connection.
        """
        from database import get_db_connection
        
        print(f"Creating new database connection for streaming pipeline {pipeline_id}")
        db_connection = get_db_connection()
        print(f"Database connection created: {db_connection}")
        
        try:
            await self._execute_streaming_pipeline(
                pipeline_id, business_id, user_id, mode, db_connection
            )
        except Exception as e:
            print(f"Error in streaming pipeline execution: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                db_connection.close()
                print(f"Streaming pipeline {pipeline_id} database connection closed")
            except Exception as e:
                print(f"Error closing database connection: {e}")
    
    async def _execute_streaming_pipeline(
        self, 
        pipeline_id: str, 
        business_id: str, 
        user_id: str,
        mode: str,
        db_connection=None
    ):
        """
        Execute the streaming pipeline phases.
        
        For streaming mode, all phases run continuously, so we:
        1. Start all streaming queries
        2. Monitor their health
        3. Report 100% when all are running successfully
        """
        try:
            cumulative_progress = 0
            process_ids = {}
            running_processes = []
            
            # Start all streaming phases
            for i, phase in enumerate(self.STREAMING_PIPELINE_PHASES):
                phase_name = phase["name"]
                
                # Check if pipeline was cancelled
                status = self._get_pipeline_status(pipeline_id, db_connection=db_connection)
                if status == "cancelled":
                    print(f"Streaming pipeline {pipeline_id} was cancelled before {phase_name}")
                    return
                
                # Update status for this phase
                await self._update_progress(
                    pipeline_id, business_id,
                    status="running",
                    current_step=phase["description"],
                    progress=cumulative_progress,
                    db_connection=db_connection
                )
                
                # Start streaming phase
                print(f"\n{'='*60}")
                print(f"Starting {phase_name} streaming phase for business {business_id}")
                print(f"Mode: {mode}")
                print(f"{'='*60}")
                
                success, process = await self._start_streaming_phase(
                    phase, business_id, pipeline_id, mode
                )
                
                if process:
                    process_ids[phase_name] = process.pid
                    running_processes.append({
                        "name": phase_name,
                        "process": process
                    })
                
                if not success:
                    error_msg = f"Failed to start {phase_name} streaming phase"
                    await self._update_progress(
                        pipeline_id, business_id,
                        status="failed",
                        current_step=phase["description"],
                        progress=cumulative_progress,
                        error_message=error_msg,
                        failed_phase=phase_name,
                        process_ids=process_ids,
                        db_connection=db_connection
                    )
                    return
                
                # Update cumulative progress
                cumulative_progress += phase["weight"]
                await self._update_progress(
                    pipeline_id, business_id,
                    status="running",
                    current_step=phase["description"],
                    progress=min(cumulative_progress, 95),  # Don't show 100% until all confirmed running
                    process_ids=process_ids,
                    db_connection=db_connection
                )
                
                # Small delay between starting phases
                await asyncio.sleep(5)
            
            # Give queries time to initialize
            print("Waiting for all streaming queries to initialize...")
            await asyncio.sleep(10)
            
            # Verify all queries are running successfully
            all_running = True
            for proc_info in running_processes:
                if proc_info["process"].returncode is not None:
                    all_running = False
                    error_msg = f"{proc_info['name']} streaming query failed to start"
                    await self._update_progress(
                        pipeline_id, business_id,
                        status="failed",
                        current_step="Starting Streaming Queries",
                        progress=cumulative_progress,
                        error_message=error_msg,
                        failed_phase=proc_info["name"],
                        process_ids=process_ids,
                        db_connection=db_connection
                    )
                    return
            
            if all_running:
                # All streaming queries started successfully - report 100%
                await self._update_progress(
                    pipeline_id, business_id,
                    status="completed",
                    current_step="All streaming queries running successfully",
                    progress=100,
                    completed=True,
                    process_ids=process_ids,
                    db_connection=db_connection
                )
                
                print(f"\n{'='*60}")
                print(f"Streaming pipeline {pipeline_id} started successfully!")
                print(f"All queries are now running continuously")
                print(f"{'='*60}")
                
                # Keep monitoring processes in background
                # They will continue running until explicitly stopped
                asyncio.create_task(
                    self._monitor_streaming_processes(
                        pipeline_id, business_id, running_processes, process_ids, db_connection
                    )
                )
            
        except Exception as e:
            print(f"Streaming pipeline execution error: {e}")
            import traceback
            traceback.print_exc()
            
            await self._update_progress(
                pipeline_id, business_id,
                status="failed",
                current_step="Streaming Pipeline Error",
                progress=cumulative_progress,
                error_message=str(e),
                db_connection=db_connection
            )
    
    async def _start_streaming_phase(
        self, 
        phase: Dict[str, Any], 
        business_id: str, 
        pipeline_id: str,
        mode: str
    ) -> tuple[bool, Optional[asyncio.subprocess.Process]]:
        """
        Start a streaming phase (runs continuously).
        
        Returns:
            tuple: (success: bool, process: Optional[Process])
        """
        script_path = os.path.join(self.project_root, phase["script"])
        
        if not os.path.exists(script_path):
            print(f"ERROR: Script not found: {script_path}")
            return False, None
        
        # Build command
        cmd = [
            "python3",
            script_path,
            "--bucket-name", business_id,
            "--mode", mode
        ]
        
        print(f"Executing: {' '.join(cmd)}")
        
        try:
            env = os.environ.copy()
            env['PIPELINE_ID'] = pipeline_id
            env['PIPELINE_PHASE'] = phase['name']
            env['PIPELINE_MODE'] = mode
            
            # Start subprocess - it will run continuously
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.project_root,
                env=env,
                start_new_session=True
            )
            
            # Stream output in background
            asyncio.create_task(
                self._stream_output(process.stdout, f"[{phase['name']}] ", pipeline_id)
            )
            asyncio.create_task(
                self._stream_output(process.stderr, f"[{phase['name']} ERROR] ", pipeline_id)
            )
            
            # Give it a moment to start
            await asyncio.sleep(2)
            
            # Check if it crashed immediately
            if process.returncode is not None:
                print(f"❌ {phase['name']} phase failed to start")
                return False, process
            
            print(f"✅ {phase['name']} streaming phase started (PID: {process.pid})")
            return True, process
                
        except Exception as e:
            print(f"Error starting {phase['name']} streaming phase: {e}")
            import traceback
            traceback.print_exc()
            return False, None
    
    async def _monitor_streaming_processes(
        self,
        pipeline_id: str,
        business_id: str,
        running_processes: list,
        process_ids: Dict,
        db_connection
    ):
        """
        Monitor streaming processes and update status if any fail.
        """
        try:
            while True:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Check if pipeline was cancelled
                status = self._get_pipeline_status(pipeline_id, db_connection=db_connection)
                if status == "cancelled":
                    print(f"Streaming pipeline {pipeline_id} monitoring stopped (cancelled)")
                    return
                
                # Check if any process has died
                for proc_info in running_processes:
                    if proc_info["process"].returncode is not None:
                        error_msg = f"{proc_info['name']} streaming query stopped unexpectedly"
                        await self._update_progress(
                            pipeline_id, business_id,
                            status="failed",
                            current_step="Streaming Query Error",
                            progress=100,
                            error_message=error_msg,
                            failed_phase=proc_info["name"],
                            process_ids=process_ids,
                            db_connection=db_connection
                        )
                        return
                        
        except Exception as e:
            print(f"Error monitoring streaming processes: {e}")
    
    async def _stream_output(self, stream, prefix: str, pipeline_id: str):
        """Stream subprocess output in real-time."""
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
        failed_phase: Optional[str] = None,
        completed: bool = False,
        process_ids: Optional[Dict] = None,
        db_connection=None
    ):
        """Update pipeline progress in database and broadcast via WebSocket."""
        db = db_connection if db_connection is not None else self.db
        
        try:
            update_data = {
                "pipeline_id": pipeline_id,
                "status": status,
                "current_step": current_step,
                "progress_percentage": min(progress, 100),
                "error_message": error_message,
                "failed_phase": failed_phase
            }
            
            if completed:
                update_data["completed_at"] = datetime.now()
            
            if process_ids:
                update_data["process_ids"] = json.dumps(process_ids)
            
            set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys() if k != "pipeline_id"])
            query = f"UPDATE pipeline_status SET {set_clause} WHERE pipeline_id = :pipeline_id"
            
            db.execute(text(query), update_data)
            db.commit()
            
            # Broadcast via WebSocket
            await self._broadcast_progress(business_id, {
                "pipeline_id": pipeline_id,
                "status": status,
                "current_step": current_step,
                "progress": progress,
                "error_message": error_message,
                "failed_phase": failed_phase,
                "type": "streaming"
            })
            
        except Exception as e:
            print(f"Error updating streaming pipeline progress: {e}")
            import traceback
            traceback.print_exc()
    
    async def _broadcast_progress(self, business_id: str, data: Dict[str, Any]):
        """Broadcast progress update via WebSocket."""
        if self.websocket_manager:
            try:
                await self.websocket_manager.broadcast(
                    message=data,
                    business_id=business_id
                )
            except Exception as e:
                print(f"Error broadcasting streaming pipeline progress: {e}")
    
    def _get_pipeline_status(self, pipeline_id: str, db_connection=None) -> Optional[str]:
        """Get current pipeline status from database."""
        db = db_connection if db_connection is not None else self.db
        
        try:
            result = db.execute(
                text("SELECT status FROM pipeline_status WHERE pipeline_id = :pipeline_id"),
                {"pipeline_id": pipeline_id}
            ).fetchone()
            
            return result[0] if result else None
        except Exception as e:
            print(f"Error getting streaming pipeline status: {e}")
            return None
