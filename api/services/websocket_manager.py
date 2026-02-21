"""
WebSocket manager for real-time pipeline progress updates.
"""

from typing import Dict, Set
from fastapi import WebSocket


class WebSocketManager:
    """Manager for WebSocket connections and broadcasting."""
    
    def __init__(self):
        # Store active connections by business_id
        # business_id -> Set of WebSocket connections
        self.active_connections: Dict[str, Set[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, business_id: str):
        """
        Accept and register a new WebSocket connection.
        
        Args:
            websocket: WebSocket connection
            business_id: Business ID for the connection
        """
        await websocket.accept()
        
        if business_id not in self.active_connections:
            self.active_connections[business_id] = set()
        
        self.active_connections[business_id].add(websocket)
        print(f"WebSocket connected for business {business_id}. Total connections: {len(self.active_connections[business_id])}")
    
    def disconnect(self, websocket: WebSocket, business_id: str):
        """
        Remove a WebSocket connection.
        
        Args:
            websocket: WebSocket connection
            business_id: Business ID for the connection
        """
        if business_id in self.active_connections:
            self.active_connections[business_id].discard(websocket)
            
            # Clean up empty sets
            if not self.active_connections[business_id]:
                del self.active_connections[business_id]
            
            print(f"WebSocket disconnected for business {business_id}")
    
    async def broadcast(self, message: dict, business_id: str):
        """
        Broadcast a message to all connections for a business.
        
        Args:
            message: Message dict to broadcast
            business_id: Business ID to broadcast to
        """
        if business_id not in self.active_connections:
            return
        
        # Copy set to avoid modification during iteration
        connections = self.active_connections[business_id].copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"Error broadcasting to connection: {e}")
                # Remove failed connection
                self.disconnect(connection, business_id)
    
    def get_connection_count(self, business_id: str) -> int:
        """
        Get number of active connections for a business.
        
        Args:
            business_id: Business ID
            
        Returns:
            Number of active connections
        """
        return len(self.active_connections.get(business_id, set()))
