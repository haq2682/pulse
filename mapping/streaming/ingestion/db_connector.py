"""
Database connector with auto-detection from URI.
Functional approach - simple functions for all DB types.
"""

from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple
import os


def detect_db_type(uri: str) -> str:
    """Detect database type from URI scheme."""
    scheme = urlparse(uri).scheme.lower()
    
    db_map = {
        'postgresql': 'postgres',
        'postgres': 'postgres',
        'mongodb': 'mongo',
        'mongodb+srv': 'mongo',
        'mysql': 'mysql',
        'mssql': 'mssql',
        'sqlserver': 'mssql'
    }
    
    db_type = db_map.get(scheme)
    if not db_type:
        raise ValueError(f"Unsupported database type: {scheme}")
    
    return db_type


def connect_postgres(uri: str) -> Any:
    """Connect to PostgreSQL."""
    import psycopg2
    return psycopg2.connect(uri)


def connect_mysql(uri: str) -> Any:
    """Connect to MySQL."""
    import mysql.connector
    from mysql.connector import connect
    parsed = urlparse(uri)
    return connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/')
    )


def connect_mongo(uri: str) -> Any:
    """Connect to MongoDB."""
    from pymongo import MongoClient
    return MongoClient(uri)


def connect_mssql(uri: str) -> Any:
    """Connect to MSSQL."""
    import pyodbc
    return pyodbc.connect(uri)


def get_connection(uri: str) -> Tuple[Any, str]:
    """
    Auto-detect DB type and return connection.
    Returns: (connection, db_type)
    """
    db_type = detect_db_type(uri)
    
    connectors = {
        'postgres': connect_postgres,
        'mysql': connect_mysql,
        'mongo': connect_mongo,
        'mssql': connect_mssql
    }
    
    conn = connectors[db_type](uri)
    return conn, db_type


def discover_tables(conn: Any, db_type: str) -> List[str]:
    """
    Auto-discover all tables/collections in database.
    Returns list of table/collection names.
    """
    if db_type == 'mongo':
        db = conn.get_default_database()
        return db.list_collection_names()
    
    elif db_type == 'postgres':
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    elif db_type == 'mysql':
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    elif db_type == 'mssql':
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    return []


def fetch_new_records(conn: Any, db_type: str, table: str, last_timestamp: str = None) -> List[Dict]:
    """
    Fetch new records from database.
    Works for SQL databases. MongoDB needs different logic.
    """
    if db_type == 'mongo':
        return fetch_mongo_records(conn, table, last_timestamp)
    else:
        return fetch_sql_records(conn, db_type, table, last_timestamp)


def fetch_sql_records(conn: Any, db_type: str, table: str, last_timestamp: str = None) -> List[Dict]:
    """Fetch records from SQL databases."""
    cursor = conn.cursor()
    
    if last_timestamp:
        query = f"SELECT * FROM {table} WHERE updated_at > %s OR created_at > %s ORDER BY COALESCE(updated_at, created_at) ASC"
        cursor.execute(query, (last_timestamp, last_timestamp))
    else:
        query = f"SELECT * FROM {table} ORDER BY COALESCE(updated_at, created_at) ASC"
        cursor.execute(query)
    
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    
    return [dict(zip(columns, row)) for row in rows]


def fetch_mongo_records(conn: Any, collection: str, last_timestamp: str = None) -> List[Dict]:
    """Fetch records from MongoDB."""
    db = conn.get_default_database()
    coll = db[collection]
    
    if last_timestamp:
        query = {"$or": [{"updated_at": {"$gt": last_timestamp}}, {"created_at": {"$gt": last_timestamp}}]}
        cursor = coll.find(query).sort([("updated_at", 1), ("created_at", 1)])
    else:
        cursor = coll.find().sort([("updated_at", 1), ("created_at", 1)])
    
    records = []
    for doc in cursor:
        doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
        records.append(doc)
    
    return records


def get_last_timestamp(records: List[Dict]) -> str:
    """Extract last timestamp from records."""
    if not records:
        return None
    
    last_record = records[-1]
    return last_record.get('updated_at') or last_record.get('created_at')
