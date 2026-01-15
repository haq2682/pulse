"""
Session management with Redis - RAW SQL & UUID compatible
"""
import json
import secrets
import redis
from datetime import datetime
from typing import Optional, Dict, Any

from config import get_settings

settings = get_settings()


class SessionService:
    """Manages user sessions stored in Redis"""

    def __init__(self):
        self.redis = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True  # so json.loads returns dict
        )
        self.prefix = "session:"
        self.expire_seconds = settings.session_expire_minutes * 60  # typically 60*24
    
    def create_session(self, user_id: str, email: str, name: str) -> str:
        """
        Create a new session after login or registration.

        Args:
            user_id (str): User's UUID (as string)
            email (str): User's email
            name (str): User's display name or username

        Returns:
            str: session_id (random token to send back as cookie)
        """
        session_id = secrets.token_urlsafe(32)

        session_data = {
            "user_id": user_id,
            "email": email,
            "name": name,
            "created_at": datetime.utcnow().isoformat()
        }

        key = f"{self.prefix}{session_id}"
        self.redis.setex(key, self.expire_seconds, json.dumps(session_data))
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session data by session_id.

        Args:
            session_id (str): random token from cookie

        Returns:
            dict or None: session data if valid, else None
        """
        key = f"{self.prefix}{session_id}"
        data = self.redis.get(key)
        if not data:
            return None
        # refresh expiration (sliding session)
        self.redis.expire(key, self.expire_seconds)
        return json.loads(data)

    def delete_session(self, session_id: str) -> bool:
        """
        Delete a session upon logout.

        Args:
            session_id (str): session from cookie

        Returns:
            bool: True if session deleted, False otherwise
        """
        key = f"{self.prefix}{session_id}"
        return self.redis.delete(key) > 0


# Single instance to use everywhere
session_service = SessionService()