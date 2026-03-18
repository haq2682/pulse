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

    Each instance is scoped to a single ``bucket_name`` (business_id) so that
    multiple tenants running concurrently never see each other's watermarks.
    The ``cleaning_state`` table uses ``(bucket_name, file_path)`` as its
    composite primary key.
    """

    def __init__(self, bucket_name: str = ""):
        """Initialize the incremental cleaner with database connection.

        Args:
            bucket_name: MinIO bucket name (business_id).  Used to scope all
                state queries so that different tenants do not share watermarks.
        """
        self.bucket_name = bucket_name or ""
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
        """Ensure the state tracking table exists with the correct schema.

        Handles two cases:
        1. First-time creation  — creates the table with the composite PK
           ``(bucket_name, file_path)`` from the start.
        2. Migration from the old single-column PK schema — adds the
           ``bucket_name`` column (defaulting to '') and promotes the PK to
           the composite form so existing rows are preserved.
        """
        try:
            with self.engine.connect() as conn:
                # ── 1. Create table (new installations) ───────────────────────
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS cleaning_state (
                        bucket_name  VARCHAR(255) NOT NULL DEFAULT '',
                        file_path    VARCHAR(500) NOT NULL,
                        processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        file_size    BIGINT,
                        record_count BIGINT,
                        checksum     VARCHAR(64),
                        created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (bucket_name, file_path)
                    )
                """))

                # ── 2. Migration: add bucket_name if the old schema is present ─
                conn.execute(text("""
                    ALTER TABLE cleaning_state
                        ADD COLUMN IF NOT EXISTS bucket_name VARCHAR(255) NOT NULL DEFAULT ''
                """))

                # ── 3. Migration: promote PK to composite form if still single ─
                #    Uses a DO block so Airflow/psycopg2 see a single statement.
                conn.execute(text("""
                    DO $$
                    BEGIN
                        -- Only act when the current PK covers exactly one column
                        IF EXISTS (
                            SELECT 1
                            FROM   pg_constraint c
                            JOIN   pg_class      t ON t.oid = c.conrelid
                            WHERE  t.relname          = 'cleaning_state'
                              AND  c.contype           = 'p'
                              AND  array_length(c.conkey, 1) = 1
                        ) THEN
                            ALTER TABLE cleaning_state DROP CONSTRAINT cleaning_state_pkey;
                            ALTER TABLE cleaning_state ADD PRIMARY KEY (bucket_name, file_path);
                        END IF;
                    END
                    $$
                """))

                # ── 4. Ensure supporting indexes exist ─────────────────────────
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_cleaning_state_processed_at
                        ON cleaning_state(processed_at DESC)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_cleaning_state_bucket
                        ON cleaning_state(bucket_name)
                """))

                conn.commit()
                print("✅ State tracking table ready")
        except SQLAlchemyError as e:
            print(f"⚠️  Warning: Could not create/migrate state table: {e}")
    
    def get_processed_files(self):
        """
        Get set of already-processed file paths **for this tenant bucket**.

        Returns:
            set: Set of file paths (MinIO object keys) that have been processed
                 for ``self.bucket_name``.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT file_path FROM cleaning_state WHERE bucket_name = :bn"),
                    {"bn": self.bucket_name},
                )
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
                    (bucket_name, file_path, processed_at, file_size, record_count, checksum)
                    VALUES (:bn, :path, :ts, :size, :count, :checksum)
                    ON CONFLICT (bucket_name, file_path) DO UPDATE
                    SET processed_at  = :ts,
                        file_size     = :size,
                        record_count  = :count,
                        checksum      = :checksum,
                        updated_at    = :ts
                """), {
                    "bn":    self.bucket_name,
                    "path":  file_path,
                    "ts":    datetime.utcnow(),
                    "size":  file_size,
                    "count": record_count,
                    "checksum": checksum,
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
                        (bucket_name, file_path, processed_at, file_size, record_count, checksum)
                        VALUES (:bn, :path, :ts, :size, :count, :checksum)
                        ON CONFLICT (bucket_name, file_path) DO UPDATE
                        SET processed_at  = :ts,
                            file_size     = :size,
                            record_count  = :count,
                            checksum      = :checksum,
                            updated_at    = :ts
                    """), {
                        "bn":    self.bucket_name,
                        "path":  file_path,
                        "ts":    datetime.utcnow(),
                        "size":  metadata.get("file_size"),
                        "count": metadata.get("record_count"),
                        "checksum": metadata.get("checksum"),
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
                conn.execute(
                    text("DELETE FROM cleaning_state WHERE bucket_name = :bn"),
                    {"bn": self.bucket_name},
                )
                conn.commit()
                print(f"✅ State table reset for bucket '{self.bucket_name}' - all files will be reprocessed on next run")
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
                        COUNT(*)         AS total_files,
                        SUM(record_count) AS total_records,
                        MAX(processed_at) AS last_processed,
                        MIN(processed_at) AS first_processed
                    FROM cleaning_state
                    WHERE bucket_name = :bn
                """), {"bn": self.bucket_name})
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
