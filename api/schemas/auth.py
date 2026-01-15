"""
Request/Response schemas for authentication
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional


# ============ REQUEST SCHEMAS ============

class UserRegister(BaseModel):
    """Data needed to register a new user"""
    email: EmailStr
    username: str = Field(..., min_length=2, max_length=100)
    password: str = Field(..., min_length=8)


class UserLogin(BaseModel):
    """Data needed to login"""
    email: EmailStr
    password: str


class ForgotPasswordRequest(BaseModel):
    """Data for forgot password"""
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    """Data for resetting password"""
    token: str
    new_password: str = Field(..., min_length=8)


# ============ RESPONSE SCHEMAS ============

class UserResponse(BaseModel):
    """User data returned to frontend"""
    id: int
    email: str
    full_name: str
    is_verified: bool
    
    class Config:
        from_attributes = True


class MessageResponse(BaseModel):
    """Simple message response"""
    message: str


class AuthStatusResponse(BaseModel):
    """Authentication status check response"""
    authenticated: bool
    user:  Optional[UserResponse] = None