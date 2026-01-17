import httpx
from typing import Optional
from config import get_settings

settings = get_settings()

class GoogleOAuthService: 
    GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
    GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
    
    @staticmethod
    def get_authorization_url(state: str) -> str:
        """Generate Google OAuth authorization URL."""
        params = {
            "client_id": settings.google_client_id,
            "redirect_uri":  settings.google_redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            "access_type": "offline",
            "prompt": "consent"
        }
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{GoogleOAuthService.GOOGLE_AUTH_URL}?{query_string}"
    
    @staticmethod
    async def get_tokens(code: str) -> dict:
        """Exchange authorization code for tokens."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                GoogleOAuthService.GOOGLE_TOKEN_URL,
                data={
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri":  settings.google_redirect_uri
                }
            )
            response.raise_for_status()
            return response.json()
    
    @staticmethod
    async def get_user_info(access_token: str) -> Optional[dict]:
        """Get user info from Google."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                GoogleOAuthService.GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {access_token}"}
            )
            if response.status_code == 200:
                return response.json()
            return None

google_oauth_service = GoogleOAuthService()