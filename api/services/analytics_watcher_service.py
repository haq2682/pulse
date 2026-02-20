"""
Analytics Watcher Service for monitoring parquet file changes in MinIO.

This service monitors the analytics directory in MinIO for changes and
broadcasts updates via WebSocket to connected clients for real-time chart updates.
"""

import asyncio
from typing import Dict, Set, Optional
from datetime import datetime
import logging
from minio import Minio
from minio.error import S3Error
from services.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)


class AnalyticsWatcherService:
    """
    Service for monitoring analytics parquet files and broadcasting updates.
    
    Monitors MinIO buckets for changes in the analytics directory and notifies
    connected WebSocket clients when files are updated.
    """
    
    def __init__(self, minio_client: Minio, websocket_manager: WebSocketManager):
        """
        Initialize the analytics watcher service.
        
        Args:
            minio_client: MinIO client instance
            websocket_manager: WebSocket manager for broadcasting updates
        """
        self.minio_client = minio_client
        self.websocket_manager = websocket_manager
        
        # Track file metadata: {business_id: {file_path: (last_modified, size)}}
        self.file_metadata: Dict[str, Dict[str, tuple]] = {}
        
        # Active monitoring tasks
        self.monitoring_tasks: Dict[str, asyncio.Task] = {}
        
        # Polling interval in seconds
        self.poll_interval = 15  # Check every 15 seconds
        
        logger.info("AnalyticsWatcherService initialized")
    
    def start_monitoring(self, business_id: str):
        """
        Start monitoring analytics directory for a business.
        
        Args:
            business_id: Business ID (bucket name)
        """
        if business_id in self.monitoring_tasks:
            logger.info(f"Already monitoring business {business_id}")
            return
        
        # Create monitoring task
        task = asyncio.create_task(self._monitor_business(business_id))
        self.monitoring_tasks[business_id] = task
        logger.info(f"Started monitoring analytics for business {business_id}")
    
    def stop_monitoring(self, business_id: str):
        """
        Stop monitoring analytics directory for a business.
        
        Args:
            business_id: Business ID (bucket name)
        """
        if business_id in self.monitoring_tasks:
            self.monitoring_tasks[business_id].cancel()
            del self.monitoring_tasks[business_id]
            logger.info(f"Stopped monitoring analytics for business {business_id}")
    
    async def _monitor_business(self, business_id: str):
        """
        Monitor analytics directory for a specific business (internal).
        
        Args:
            business_id: Business ID (bucket name)
        """
        try:
            while True:
                await self._check_for_updates(business_id)
                await asyncio.sleep(self.poll_interval)
                
        except asyncio.CancelledError:
            logger.info(f"Monitoring cancelled for business {business_id}")
        except Exception as e:
            logger.error(f"Error monitoring business {business_id}: {e}")
    
    async def _check_for_updates(self, business_id: str):
        """
        Check for file updates in the analytics directory.
        
        Args:
            business_id: Business ID (bucket name)
        """
        try:
            # Check if bucket exists
            if not self.minio_client.bucket_exists(business_id):
                return
            
            # Get current file metadata
            current_files = {}
            analytics_prefix = "analytics/"
            
            objects = self.minio_client.list_objects(
                business_id, 
                prefix=analytics_prefix,
                recursive=True
            )
            
            for obj in objects:
                if obj.object_name.endswith('.parquet'):
                    file_path = obj.object_name
                    last_modified = obj.last_modified
                    size = obj.size
                    current_files[file_path] = (last_modified, size)
            
            # Initialize if first check
            if business_id not in self.file_metadata:
                self.file_metadata[business_id] = current_files
                logger.info(f"Initialized file tracking for {business_id} with {len(current_files)} files")
                return
            
            # Detect changes
            previous_files = self.file_metadata[business_id]
            changed_files = []
            new_files = []
            
            for file_path, (last_modified, size) in current_files.items():
                if file_path not in previous_files:
                    # New file
                    new_files.append(file_path)
                    logger.info(f"New analytics file detected: {file_path}")
                elif previous_files[file_path] != (last_modified, size):
                    # Modified file
                    changed_files.append(file_path)
                    logger.info(f"Analytics file updated: {file_path}")
            
            # Update stored metadata
            self.file_metadata[business_id] = current_files
            
            # Broadcast updates if any changes
            if changed_files or new_files:
                await self._broadcast_update(
                    business_id, 
                    changed_files + new_files,
                    len(changed_files),
                    len(new_files)
                )
                
        except S3Error as e:
            logger.error(f"MinIO error checking updates for {business_id}: {e}")
        except Exception as e:
            logger.error(f"Error checking updates for {business_id}: {e}")
    
    async def _broadcast_update(
        self, 
        business_id: str, 
        files: list,
        changed_count: int,
        new_count: int
    ):
        """
        Broadcast analytics update notification via WebSocket.
        
        Args:
            business_id: Business ID
            files: List of file paths that changed
            changed_count: Number of modified files
            new_count: Number of new files
        """
        # Extract file names (remove analytics/ prefix and .parquet extension)
        file_names = []
        categories = set()
        
        for file_path in files:
            file_name = file_path.replace('analytics/', '').replace('.parquet', '')
            file_names.append(file_name)
            
            # Try to extract category from file name
            # Many analytics follow pattern: category_metric_period
            parts = file_name.split('_')
            if len(parts) >= 2:
                categories.add(parts[0])
        
        message = {
            "event": "analytics_updated",
            "business_id": business_id,
            "files": file_names,
            "categories": list(categories),
            "changed_count": changed_count,
            "new_count": new_count,
            "timestamp": datetime.utcnow().isoformat(),
            "total_files": len(file_names)
        }
        
        # Broadcast to all connections for this business
        await self.websocket_manager.broadcast(message, business_id)
        logger.info(f"Broadcasted analytics update for {business_id}: {len(file_names)} files")
    
    async def manual_trigger_update(self, business_id: str):
        """
        Manually trigger an update check for a business.
        
        Useful for testing or forcing an immediate check.
        
        Args:
            business_id: Business ID (bucket name)
        """
        await self._check_for_updates(business_id)
        logger.info(f"Manual update check triggered for {business_id}")
    
    def get_monitored_businesses(self) -> list:
        """
        Get list of currently monitored businesses.
        
        Returns:
            List of business IDs being monitored
        """
        return list(self.monitoring_tasks.keys())
    
    def is_monitoring(self, business_id: str) -> bool:
        """
        Check if a business is being monitored.
        
        Args:
            business_id: Business ID
            
        Returns:
            True if monitoring is active
        """
        return business_id in self.monitoring_tasks and not self.monitoring_tasks[business_id].done()


# Global analytics watcher instance
analytics_watcher: Optional[AnalyticsWatcherService] = None


def get_analytics_watcher() -> Optional[AnalyticsWatcherService]:
    """Get the global analytics watcher instance."""
    return analytics_watcher


def set_analytics_watcher(watcher: AnalyticsWatcherService):
    """Set the global analytics watcher instance."""
    global analytics_watcher
    analytics_watcher = watcher
