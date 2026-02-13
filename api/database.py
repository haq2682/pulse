from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import get_settings

settings = get_settings()

# 1. Create the Connection Pool
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True, 
    pool_size=10, 
    max_overflow=20
)

# 2. Create SessionLocal for ORM sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 3. Define the Dependency
def get_db():
    # Open a raw connection
    with engine.connect() as connection:
        yield connection
        # The connection automatically closes here because of the 'with' block