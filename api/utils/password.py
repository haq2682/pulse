"""
Password hashing utilities
"""
import bcrypt


def hash_password(password: str) -> str:
    """
    Hash a password securely. 
    
    Example:
        hashed = hash_password("mypassword123")
        # Returns something like: $2b$12$ABC123... 
    """
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Check if a password matches its hash. 
    
    Example:
        is_valid = verify_password("mypassword123", stored_hash)
        # Returns True or False
    """
    return bcrypt.checkpw(
        plain_password.encode('utf-8'),
        hashed_password.encode('utf-8')
    )