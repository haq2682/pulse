"""
Database connector with auto-detection from URI.
Functional approach - simple functions for all DB types.

IMPORTANT: Database connections require proper user permissions.
See mapping/README.md for database administrator prerequisites
including user creation, role assignments, and replication setup.
"""

from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple
import os


def detect_db_type(uri: str) -> str:
    """
    Detect database type from URI scheme.
    
    Supported databases: PostgreSQL, MySQL, MongoDB, SQL Server, Oracle, IBM Db2, Vitess.
    Note: Cassandra and Spanner use configuration-based connections, not URIs.
    
    Vitess: Can use either 'mysql://' or 'vitess://' scheme (both use MySQL protocol).
    See mapping/README.md for detailed setup instructions.
    """
    scheme = urlparse(uri).scheme.lower()
    
    db_map = {
        'postgresql': 'postgres',
        'postgres': 'postgres',
        'mongodb': 'mongo',
        'mongodb+srv': 'mongo',
        'mysql': 'mysql',
        'mssql': 'mssql',
        'sqlserver': 'mssql',
        'oracle': 'oracle',
        'db2': 'db2',
        'vitess': 'vitess'  # Vitess uses MySQL protocol but can be specified explicitly
    }
    
    db_type = db_map.get(scheme)
    if not db_type:
        raise ValueError(
            f"Unsupported database type: {scheme}. "
            f"Supported types: {', '.join(set(db_map.values()))}. "
            f"See mapping/README.md for setup instructions."
        )
    
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


def connect_oracle(uri: str) -> Any:
    """
    Connect to Oracle Database.
    Requires cx_Oracle or oracledb package.
    URI format: oracle://user:pass@host:port/service_name
    """
    try:
        import oracledb
        parsed = urlparse(uri)
        dsn = oracledb.makedsn(
            parsed.hostname,
            parsed.port or 1521,
            service_name=parsed.path.lstrip('/')
        )
        return oracledb.connect(
            user=parsed.username,
            password=parsed.password,
            dsn=dsn
        )
    except ImportError:
        try:
            import cx_Oracle
            parsed = urlparse(uri)
            dsn = cx_Oracle.makedsn(
                parsed.hostname,
                parsed.port or 1521,
                service_name=parsed.path.lstrip('/')
            )
            return cx_Oracle.connect(
                user=parsed.username,
                password=parsed.password,
                dsn=dsn
            )
        except ImportError:
            raise ImportError(
                "Oracle database support requires either 'oracledb' or 'cx_Oracle' package. "
                "Install with: pip install oracledb  (recommended) or pip install cx_Oracle"
            )


def connect_db2(uri: str) -> Any:
    """
    Connect to IBM Db2.
    Requires ibm_db or ibm_db_dbi package.
    URI format: db2://user:pass@host:port/database
    """
    try:
        import ibm_db
        parsed = urlparse(uri)
        conn_str = (
            f"DATABASE={parsed.path.lstrip('/')};"
            f"HOSTNAME={parsed.hostname};"
            f"PORT={parsed.port or 50000};"
            f"PROTOCOL=TCPIP;"
            f"UID={parsed.username};"
            f"PWD={parsed.password};"
        )
        return ibm_db.connect(conn_str, "", "")
    except ImportError:
        raise ImportError(
            "IBM Db2 database support requires 'ibm_db' package. "
            "Install with: pip install ibm_db"
        )


def connect_vitess(uri: str) -> Any:
    """
    Connect to Vitess (uses MySQL protocol).
    Vitess is MySQL-compatible, so we use MySQL connector.
    URI format: mysql://user:pass@vtgate-host:port/keyspace
              or vitess://user:pass@vtgate-host:port/keyspace
    
    Both schemes are supported; Vitess uses MySQL protocol internally.
    """
    return connect_mysql(uri)


def get_connection(uri: str) -> Tuple[Any, str]:
    """
    Auto-detect DB type and return connection.
    Returns: (connection, db_type)
    
    Supported databases: PostgreSQL, MySQL, MongoDB, SQL Server, 
    Oracle, IBM Db2, Vitess.
    
    Note: Cassandra and Spanner require configuration-based setup
    rather than URI connections. See mapping/README.md for details.
    """
    db_type = detect_db_type(uri)
    
    connectors = {
        'postgres': connect_postgres,
        'mysql': connect_mysql,
        'mongo': connect_mongo,
        'mssql': connect_mssql,
        'oracle': connect_oracle,
        'db2': connect_db2,
        'vitess': connect_vitess
    }
    
    conn = connectors[db_type](uri)
    return conn, db_type


def discover_tables(conn: Any, db_type: str) -> List[str]:
    """
    Auto-discover all tables/collections in database.
    Returns list of table/collection names.
    
    Supported for: PostgreSQL, MySQL, MongoDB, SQL Server, Oracle, Db2, Vitess.
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
    
    elif db_type == 'oracle':
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM user_tables
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    elif db_type == 'db2':
        try:
            import ibm_db_dbi
            # Convert ibm_db connection to DBI connection for cursor operations
            cursor = ibm_db_dbi.Connection(conn).cursor()
            cursor.execute("""
                SELECT TABNAME FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = CURRENT SCHEMA AND TYPE = 'T'
                ORDER BY TABNAME
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
        except ImportError:
            raise ImportError(
                "IBM Db2 table discovery requires 'ibm_db_dbi' package. "
                "Install with: pip install ibm_db"
            )
    
    elif db_type == 'vitess':
        # Vitess uses MySQL protocol
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    return []


def fetch_new_records(conn: Any, db_type: str, table: str, last_timestamp: str = None) -> List[Dict]:
    """
    Fetch new records from database.
    Works for SQL databases (PostgreSQL, MySQL, SQL Server, Oracle, Db2, Vitess).
    MongoDB needs different logic.
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
