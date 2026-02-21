"""
Incremental Cleaner for efficient data processing.

This module provides functionality to track processed files and only clean new/updated files,
significantly reducing processing time for incremental runs.
"""

import os
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class IncrementalCleaner:
    """
    Manages incremental cleaning by tracking which files have been processed.
    
    This class maintains a state table in PostgreSQL to track processed files,
    enabling the cleaning pipeline to skip files that have already been processed.
    """
    
    def __init__(self):
        """Initialize the incremental cleaner with database connection."""
        # Construct PostgreSQL connection string from environment variables
        postgres_user = os.getenv("POSTGRES_USER", "postgres")
        postgres_password = os.getenv("POSTGRES_PASSWORD", "")
        postgres_server = os.getenv("POSTGRES_SERVER", "localhost")
        postgres_database = os.getenv("POSTGRES_DATABASE_NAME", "postgres")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        
        connection_string = (
            f"postgresql://{postgres_user}:{postgres_password}"
            f"@{postgres_server}:{postgres_port}/{postgres_database}"
        )
        
        self.engine = create_engine(connection_string, echo=False)
        self._ensure_state_table()
    
    def _ensure_state_table(self):
        """Ensure the state tracking table exists."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cleaning_state (
            file_path VARCHAR(500) PRIMARY KEY,
            processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            file_size BIGINT,
            record_count BIGINT,
            checksum VARCHAR(64),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_cleaning_state_processed_at 
        ON cleaning_state(processed_at DESC);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                print("✅ State tracking table ready")
        except SQLAlchemyError as e:
            print(f"⚠️  Warning: Could not create state table: {e}")
    
    def get_processed_files(self):
        """
        Get set of already processed file paths.
        
        Returns:
            set: Set of file paths that have been processed
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT file_path FROM cleaning_state"))
                processed = {row[0] for row in result}
                return processed
        except SQLAlchemyError as e:
            print(f"⚠️  Warning: Could not query state table: {e}")
            return set()
    
    def get_unprocessed_files(self, all_file_paths):
        """
        Filter file paths to only include unprocessed files.
        
        Args:
            all_file_paths (list): List of all available file paths
            
        Returns:
            list: List of file paths that haven't been processed yet
        """
        processed_files = self.get_processed_files()
        unprocessed = [f for f in all_file_paths if f not in processed_files]
        
        if not unprocessed:
            print("✅ All files have been processed - no new files to clean")
        else:
            print(f"📦 Found {len(unprocessed)} new files to clean (out of {len(all_file_paths)} total)")
        
        return unprocessed
    
    def mark_processed(self, file_path, file_size=None, record_count=None, checksum=None):
        """
        Mark a file as processed in the state table.
        
        Args:
            file_path (str): Path to the file
            file_size (int): Size of the file in bytes
            record_count (int): Number of records in the file
            checksum (str): MD5 checksum of the file
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO cleaning_state 
                    (file_path, processed_at, file_size, record_count, checksum)
                    VALUES (:path, :ts, :size, :count, :checksum)
                    ON CONFLICT (file_path) DO UPDATE 
                    SET processed_at = :ts, 
                        file_size = :size, 
                        record_count = :count,
                        checksum = :checksum,
                        updated_at = :ts
                """), {
                    "path": file_path,
                    "ts": datetime.utcnow(),
                    "size": file_size,
                    "count": record_count,
                    "checksum": checksum
                })
                conn.commit()
                print(f"  ✓ Marked {file_path} as processed")
        except SQLAlchemyError as e:
            print(f"  ⚠️  Warning: Could not mark {file_path} as processed: {e}")
    
    def mark_multiple_processed(self, file_records):
        """
        Mark multiple files as processed in a single transaction.
        
        Args:
            file_records (dict): Dict mapping file_path to dict of metadata
                                 (file_size, record_count, checksum)
        """
        if not file_records:
            return
        
        try:
            with self.engine.connect() as conn:
                for file_path, metadata in file_records.items():
                    conn.execute(text("""
                        INSERT INTO cleaning_state 
                        (file_path, processed_at, file_size, record_count, checksum)
                        VALUES (:path, :ts, :size, :count, :checksum)
                        ON CONFLICT (file_path) DO UPDATE 
                        SET processed_at = :ts, 
                            file_size = :size, 
                            record_count = :count,
                            checksum = :checksum,
                            updated_at = :ts
                    """), {
                        "path": file_path,
                        "ts": datetime.utcnow(),
                        "size": metadata.get('file_size'),
                        "count": metadata.get('record_count'),
                        "checksum": metadata.get('checksum')
                    })
                conn.commit()
                print(f"✅ Marked {len(file_records)} files as processed")
        except SQLAlchemyError as e:
            print(f"⚠️  Warning: Could not mark files as processed: {e}")
    
    def reset_state(self):
        """
        Reset the state table (remove all records).
        Use with caution - this will cause all files to be reprocessed.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("DELETE FROM cleaning_state"))
                conn.commit()
                print("✅ State table reset - all files will be reprocessed on next run")
        except SQLAlchemyError as e:
            print(f"❌ Error resetting state table: {e}")
    
    def get_state_summary(self):
        """
        Get summary statistics about the state table.
        
        Returns:
            dict: Summary statistics
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_files,
                        SUM(record_count) as total_records,
                        MAX(processed_at) as last_processed,
                        MIN(processed_at) as first_processed
                    FROM cleaning_state
                """))
                row = result.fetchone()
                if row:
                    return {
                        'total_files': row[0],
                        'total_records': row[1],
                        'last_processed': row[2],
                        'first_processed': row[3]
                    }
                return {}
        except SQLAlchemyError as e:
            print(f"⚠️  Warning: Could not get state summary: {e}")
            return {}
