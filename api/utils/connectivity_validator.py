"""
Connectivity validation utilities for database and API endpoints.
Used to test connections before starting mapping pipelines.

Supports all Debezium-compatible databases:
- PostgreSQL
- MySQL/MariaDB
- MongoDB
- SQL Server
- Oracle
- DB2
- Cassandra
- Vitess
- Spanner
- Informix
"""

import socket
import ipaddress
from urllib.parse import urlparse
from typing import Tuple

# Core dependencies (should always be available)
import psycopg2
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure, ConfigurationError as MongoConfigurationError

# Optional dependencies - import conditionally
try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore[assignment]

try:
    import pymssql
except ImportError:
    pymssql = None  # type: ignore[assignment]

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]


def validate_database_connection(db_uri: str, timeout: int = 10) -> Tuple[bool, str]:
    """
    Test database connectivity and return status with error message.
    
    Supports all Debezium-compatible databases with appropriate connection testing.
    For databases without direct Python drivers, returns guidance message.
    
    Args:
        db_uri: Database connection URI
        timeout: Connection timeout in seconds
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        parsed = urlparse(db_uri)
        db_type = parsed.scheme.lower()
        
        # Extract connection parameters
        username = parsed.username
        password = parsed.password
        hostname = parsed.hostname
        port = parsed.port
        database = parsed.path.lstrip('/') if parsed.path else None
        
        if not hostname:
            return False, "Invalid database URI: hostname not found"
        
        # PostgreSQL
        if db_type in ['postgresql', 'postgres']:
            port = port or 5432
            if not database:
                return False, "Invalid PostgreSQL URI: database name not found"
            try:
                conn = psycopg2.connect(
                    host=hostname,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    connect_timeout=timeout
                )
                conn.close()
                return True, f"Successfully connected to PostgreSQL database at {hostname}:{port}"
            except psycopg2.OperationalError as e:
                error_msg = str(e)
                if "authentication failed" in error_msg.lower():
                    return False, f"Authentication failed for PostgreSQL at {hostname}:{port}. Check username and password."
                elif "connection refused" in error_msg.lower() or "could not connect" in error_msg.lower():
                    return False, f"Cannot connect to PostgreSQL at {hostname}:{port}. Server may be down or unreachable."
                elif "does not exist" in error_msg.lower():
                    return False, f"Database '{database}' does not exist on PostgreSQL server at {hostname}:{port}."
                else:
                    return False, f"PostgreSQL connection error: {error_msg}"
                    
        # MySQL
        elif db_type == 'mysql':
            if pymysql is None:
                return True, f"MySQL database at {hostname} will be validated by Debezium connector. Install pymysql library for pre-validation."
            port = port or 3306
            if not database:
                return False, "Invalid MySQL URI: database name not found"
            try:
                conn = pymysql.connect(
                    host=hostname,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    connect_timeout=timeout
                )
                conn.close()
                return True, f"Successfully connected to MySQL database at {hostname}:{port}"
            except pymysql.OperationalError as e:
                error_msg = str(e)
                if "access denied" in error_msg.lower():
                    return False, f"Authentication failed for MySQL at {hostname}:{port}. Check username and password."
                elif "can't connect" in error_msg.lower() or "connection refused" in error_msg.lower():
                    return False, f"Cannot connect to MySQL at {hostname}:{port}. Server may be down or unreachable."
                elif "unknown database" in error_msg.lower():
                    return False, f"Database '{database}' does not exist on MySQL server at {hostname}:{port}."
                else:
                    return False, f"MySQL connection error: {error_msg}"
        
        # MariaDB (similar to MySQL but distinct)
        elif db_type == 'mariadb':
            if pymysql is None:
                return True, f"MariaDB database at {hostname} will be validated by Debezium connector. Install pymysql library for pre-validation."
            port = port or 3306
            if not database:
                return False, "Invalid MariaDB URI: database name not found"
            try:
                conn = pymysql.connect(
                    host=hostname,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    connect_timeout=timeout
                )
                conn.close()
                return True, f"Successfully connected to MariaDB database at {hostname}:{port}"
            except pymysql.OperationalError as e:
                error_msg = str(e)
                if "access denied" in error_msg.lower():
                    return False, f"Authentication failed for MariaDB at {hostname}:{port}. Check username and password."
                elif "can't connect" in error_msg.lower() or "connection refused" in error_msg.lower():
                    return False, f"Cannot connect to MariaDB at {hostname}:{port}. Server may be down or unreachable."
                elif "unknown database" in error_msg.lower():
                    return False, f"Database '{database}' does not exist on MariaDB server at {hostname}:{port}."
                else:
                    return False, f"MariaDB connection error: {error_msg}"
                    
        # MongoDB
        elif db_type in ['mongodb', 'mongodb+srv']:
            port = port or 27017
            # Human-readable location for error messages
            location = hostname if db_type == 'mongodb+srv' else f"{hostname}:{port}"

            # Stage 1: For standard (non-SRV) URIs, verify raw TCP reachability first.
            # This lets us distinguish a true "server down" from an auth failure,
            # regardless of what text pymongo puts in its exception messages.
            if db_type == 'mongodb':
                try:
                    with socket.create_connection((hostname, port), timeout=timeout):
                        pass
                except OSError:
                    return False, f"Cannot connect to MongoDB at {location}. Server may be down or unreachable."

            # Stage 2: Test authentication.  'ping' is auth-free in all MongoDB
            # versions (it is an alias for the 'hello' SDAM command).  Use
            # list_collection_names() on the target database instead — this is a
            # real application-level command that requires an authenticated
            # connection.
            #
            # IMPORTANT: pymongo defaults the authSource to the database name in
            # the URI path (e.g. /pulse-test → authSource=pulse-test).  Most
            # MongoDB users (root, debezium_user) are created in the 'admin'
            # database and therefore require authSource=admin.  When no explicit
            # authSource is present in the URI we therefore retry with
            # authSource=admin on the first auth failure before giving up.
            query_str = urlparse(db_uri).query.lower()
            has_explicit_authsource = 'authsource' in query_str

            target_db = database if database else 'admin'
            # Build the list of URIs to attempt: original first, then with
            # authSource=admin as a fallback (only when no authSource was given
            # and we know the server is reachable, i.e. non-SRV after TCP check).
            uris_to_try = [db_uri]
            if not has_explicit_authsource and db_type == 'mongodb':
                sep = '&' if query_str else '?'
                uris_to_try.append(f"{db_uri}{sep}authSource=admin")

            # Use directConnection=True for standard mongodb:// URIs so that
            # pymongo connects straight to the specified host without attempting
            # replica-set member discovery.  RS members' internal hostnames are
            # often unreachable from the application network, which would
            # otherwise cause ServerSelectionTimeoutError even when the target
            # host is reachable and credentials are correct.
            # SRV URIs handle topology discovery differently, so directConnection
            # is only applied to non-SRV connections.
            direct = db_type == 'mongodb'

            last_failure = None
            for uri in uris_to_try:
                try:
                    client = MongoClient(
                        uri,
                        serverSelectionTimeoutMS=timeout * 1000,
                        directConnection=direct,
                    )
                    client[target_db].list_collection_names()
                    client.close()
                    return True, f"Successfully connected to MongoDB database at {location}"
                except OperationFailure:
                    last_failure = 'auth'
                    continue
                except ServerSelectionTimeoutError:
                    # With directConnection=True the TCP check already confirmed
                    # reachability, so SSTE here means auth/TLS rejected the
                    # connection.  For SRV URIs the cause is ambiguous.
                    last_failure = 'auth' if db_type == 'mongodb' else 'connect'
                    continue
                except MongoConfigurationError as e:
                    return False, f"MongoDB configuration error: {str(e)}"
                except Exception as e:
                    error_msg = str(e)
                    if "authentication" in error_msg.lower() or "auth" in error_msg.lower():
                        last_failure = 'auth'
                        continue
                    return False, f"MongoDB connection error: {error_msg}"

            if last_failure == 'auth':
                return False, f"Authentication failed for MongoDB at {location}. Check username and password."
            return False, f"Cannot connect to MongoDB at {location}. Server may be down or unreachable."
                    
        # SQL Server
        elif db_type in ['mssql', 'sqlserver']:
            if pymssql is None:
                return True, f"SQL Server database at {hostname} will be validated by Debezium connector. Install pymssql library for pre-validation."
            port = port or 1433
            if not database:
                return False, "Invalid SQL Server URI: database name not found"
            try:
                conn = pymssql.connect(
                    server=hostname,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    timeout=timeout
                )
                conn.close()
                return True, f"Successfully connected to SQL Server database at {hostname}:{port}"
            except pymssql.OperationalError as e:
                error_msg = str(e)
                if "login failed" in error_msg.lower():
                    return False, f"Authentication failed for SQL Server at {hostname}:{port}. Check username and password."
                elif "cannot open" in error_msg.lower() or "timeout" in error_msg.lower():
                    return False, f"Cannot connect to SQL Server at {hostname}:{port}. Server may be down or unreachable."
                else:
                    return False, f"SQL Server connection error: {error_msg}"
            except pymssql.InterfaceError:
                return False, f"Cannot connect to SQL Server at {hostname}:{port}. Server may be down or unreachable."
        
        # Oracle
        elif db_type == 'oracle':
            port = port or 1521
            # Oracle requires cx_Oracle or oracledb library
            # We'll provide a helpful message since it requires specific setup
            try:
                import cx_Oracle
                if not database:
                    return False, "Invalid Oracle URI: service name or SID not found"
                try:
                    dsn = cx_Oracle.makedsn(hostname, port, service_name=database)
                    conn = cx_Oracle.connect(user=username, password=password, dsn=dsn)
                    conn.close()
                    return True, f"Successfully connected to Oracle database at {hostname}:{port}"
                except cx_Oracle.DatabaseError as e:
                    error_obj, = e.args
                    if "ORA-01017" in str(error_obj):
                        return False, f"Authentication failed for Oracle at {hostname}:{port}. Check username and password."
                    elif "ORA-12541" in str(error_obj) or "ORA-12170" in str(error_obj):
                        return False, f"Cannot connect to Oracle at {hostname}:{port}. Server may be down or unreachable."
                    else:
                        return False, f"Oracle connection error: {error_obj}"
            except ImportError:
                # cx_Oracle not installed, provide helpful message
                return True, f"Oracle database at {hostname}:{port} will be validated by Debezium connector. Install cx_Oracle library for pre-validation."
        
        # DB2
        elif db_type == 'db2':
            port = port or 50000
            # DB2 requires ibm_db library
            try:
                import ibm_db
                if not database:
                    return False, "Invalid DB2 URI: database name not found"
                try:
                    conn_str = f"DATABASE={database};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;UID={username};PWD={password};"
                    conn = ibm_db.connect(conn_str, "", "")
                    ibm_db.close(conn)
                    return True, f"Successfully connected to DB2 database at {hostname}:{port}"
                except Exception as e:
                    error_msg = str(e)
                    if "authorization" in error_msg.lower() or "authentication" in error_msg.lower():
                        return False, f"Authentication failed for DB2 at {hostname}:{port}. Check username and password."
                    elif "communication" in error_msg.lower() or "connection refused" in error_msg.lower():
                        return False, f"Cannot connect to DB2 at {hostname}:{port}. Server may be down or unreachable."
                    else:
                        return False, f"DB2 connection error: {error_msg}"
            except ImportError:
                # ibm_db not installed, provide helpful message
                return True, f"DB2 database at {hostname}:{port} will be validated by Debezium connector. Install ibm_db library for pre-validation."
        
        # Cassandra
        elif db_type == 'cassandra':
            port = port or 9042
            # Cassandra requires cassandra-driver library
            try:
                from cassandra.cluster import Cluster, NoHostAvailable
                from cassandra.auth import PlainTextAuthProvider
                from cassandra import AuthenticationFailed as CassandraAuthFailed
                try:
                    if username and password:
                        auth_provider = PlainTextAuthProvider(username=username, password=password)
                        cluster = Cluster([hostname], port=port, auth_provider=auth_provider, connect_timeout=timeout)
                    else:
                        cluster = Cluster([hostname], port=port, connect_timeout=timeout)
                    cluster.connect()  # Test connection
                    cluster.shutdown()
                    return True, f"Successfully connected to Cassandra database at {hostname}:{port}"
                except CassandraAuthFailed:
                    return False, f"Authentication failed for Cassandra at {hostname}:{port}. Check username and password."
                except NoHostAvailable as e:
                    # NoHostAvailable wraps per-host errors in e.errors (dict of host→exception).
                    # Inspect the actual exception types to distinguish auth from connectivity.
                    host_errors = getattr(e, 'errors', {})
                    if any(isinstance(exc, CassandraAuthFailed) for exc in host_errors.values()):
                        return False, f"Authentication failed for Cassandra at {hostname}:{port}. Check username and password."
                    return False, f"Cannot connect to Cassandra at {hostname}:{port}. Server may be down or unreachable."
                except Exception as e:
                    return False, f"Cassandra connection error: {str(e)}"
            except ImportError:
                # cassandra-driver not installed, provide helpful message
                return True, f"Cassandra database at {hostname}:{port} will be validated by Debezium connector. Install cassandra-driver library for pre-validation."
        
        # Vitess (MySQL-compatible)
        elif db_type == 'vitess':
            port = port or 15991
            return True, f"Vitess database at {hostname}:{port} will be validated by Debezium connector during deployment."
        
        # Google Cloud Spanner
        elif db_type == 'spanner':
            return True, f"Google Cloud Spanner database will be validated by Debezium connector during deployment. Ensure GCP credentials are configured."
        
        # Informix
        elif db_type == 'informix':
            port = port or 9088
            # Informix requires specific ODBC setup
            return True, f"Informix database at {hostname}:{port} will be validated by Debezium connector. Ensure Informix ODBC driver is installed."
        
        else:
            # Unknown database type
            return False, f"Unsupported database type '{db_type}'. Supported types: postgresql, mysql, mariadb, mongodb, sqlserver, oracle, db2, cassandra, vitess, spanner, informix"
            
    except Exception as e:
        return False, f"Error validating database connection: {str(e)}"


def _is_safe_url(url: str) -> Tuple[bool, str]:
    """
    Check if URL is safe from SSRF attacks.
    Blocks private, loopback, and link-local IP addresses.
    
    Args:
        url: URL to validate
        
    Returns:
        Tuple of (is_safe: bool, error_message: str)
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        
        if not hostname:
            return False, "Invalid URL: no hostname found"
        
        # Try to resolve hostname to IP address
        try:
            ip_address = socket.gethostbyname(hostname)
            ip_obj = ipaddress.ip_address(ip_address)
            
            # Block private IP ranges (RFC 1918)
            if ip_obj.is_private:
                return False, f"Access to private IP addresses is not allowed for security reasons"
            
            # Block loopback addresses
            if ip_obj.is_loopback:
                return False, f"Access to loopback addresses is not allowed for security reasons"
            
            # Block link-local addresses  
            if ip_obj.is_link_local:
                return False, f"Access to link-local addresses is not allowed for security reasons"
            
            # Block multicast addresses
            if ip_obj.is_multicast:
                return False, f"Access to multicast addresses is not allowed for security reasons"
            
            # Block reserved addresses
            if ip_obj.is_reserved:
                return False, f"Access to reserved IP addresses is not allowed for security reasons"
                
        except socket.gaierror:
            # Hostname resolution failed - this is okay, let the request fail naturally
            pass
        except ValueError:
            # Invalid IP address format - this is okay
            pass
            
        return True, ""
        
    except Exception as e:
        return False, f"Error validating URL safety: {str(e)}"


def validate_api_endpoint(api_url: str, timeout: int = 10) -> Tuple[bool, str]:
    """
    Test API endpoint connectivity and validate response format.
    Includes SSRF protection to prevent access to internal services.
    
    The API must return data in this format:
    {
        "tables": [
            {
                "table_name": "users",
                "data": [{"id": 1, "name": "Alice"}]
            }
        ]
    }
    
    Args:
        api_url: API endpoint URL
        timeout: Request timeout in seconds
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    if requests is None:
        return False, "requests library is not installed. Install it to validate API endpoints."
    
    try:
        # Validate URL format
        parsed = urlparse(api_url)
        if not parsed.scheme in ['http', 'https']:
            return False, "Invalid API URL: must start with http:// or https://"
        
        if not parsed.netloc:
            return False, "Invalid API URL: hostname not found"
        
        # SSRF Protection: Check if URL is safe before making request
        is_safe, safety_error = _is_safe_url(api_url)
        if not is_safe:
            return False, f"Security error: {safety_error}"
        
        # Make test request with redirects disabled for SSRF protection
        try:
            response = requests.get(api_url, timeout=timeout, allow_redirects=False)
            
            if response.status_code == 200:
                # Validate response format
                try:
                    data = response.json()
                    
                    # Check if 'tables' key exists
                    if 'tables' not in data:
                        return False, (
                            f"API endpoint response format error: Missing 'tables' key. "
                            f"Expected format: {{'tables': [{{'table_name': 'users', 'data': [...]}}]}}"
                        )
                    
                    # Check if 'tables' is a list
                    if not isinstance(data['tables'], list):
                        return False, (
                            f"API endpoint response format error: 'tables' must be an array. "
                            f"Expected format: {{'tables': [{{'table_name': 'users', 'data': [...]}}]}}"
                        )
                    
                    # Check if at least one table exists
                    if len(data['tables']) == 0:
                        return False, (
                            f"API endpoint response format error: 'tables' array is empty. "
                            f"At least one table must be present."
                        )
                    
                    # Validate each table structure
                    for idx, table in enumerate(data['tables']):
                        if not isinstance(table, dict):
                            return False, f"API endpoint response format error: tables[{idx}] must be an object with 'table_name' and 'data' fields."
                        
                        if 'table_name' not in table:
                            return False, f"API endpoint response format error: tables[{idx}] is missing 'table_name' field."
                        
                        if 'data' not in table:
                            return False, f"API endpoint response format error: tables[{idx}] is missing 'data' field."
                        
                        if not isinstance(table['data'], list):
                            return False, f"API endpoint response format error: tables[{idx}].data must be an array of records."
                        
                        # Validate table_name is not empty
                        if not table['table_name'] or not str(table['table_name']).strip():
                            return False, f"API endpoint response format error: tables[{idx}].table_name cannot be empty."
                    
                    return True, f"Successfully connected to API endpoint at {api_url} and validated response format."
                    
                except ValueError as e:
                    return False, f"API endpoint response error: Response is not valid JSON. {str(e)}"
                    
            elif response.status_code == 401:
                return False, f"API endpoint requires authentication (401 Unauthorized). Please provide valid credentials."
            elif response.status_code == 403:
                return False, f"Access to API endpoint is forbidden (403 Forbidden). Check permissions."
            elif response.status_code == 404:
                return False, f"API endpoint not found (404). Please verify the URL: {api_url}"
            elif response.status_code >= 500:
                return False, f"API server error ({response.status_code}). The server may be experiencing issues."
            else:
                return False, f"API endpoint returned unexpected status code: {response.status_code}"
                
        except requests.exceptions.Timeout:
            return False, f"Connection to API endpoint timed out after {timeout} seconds. The server may be slow or unreachable."
        except requests.exceptions.ConnectionError:
            return False, f"Cannot connect to API endpoint at {api_url}. Server may be down or URL is incorrect."
        except requests.exceptions.TooManyRedirects:
            return False, f"Too many redirects when accessing {api_url}. Check the URL."
        except requests.exceptions.RequestException as e:
            return False, f"Error connecting to API endpoint: {str(e)}"
            
    except Exception as e:
        return False, f"Error validating API endpoint: {str(e)}"
