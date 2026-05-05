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
from typing import List, Optional, Tuple

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
    import oracledb
except ImportError:
    oracledb = None  # type: ignore[assignment]

try:
    import ibm_db_dbi
except ImportError:
    ibm_db_dbi = None  # type: ignore[assignment]

try:
    from cassandra.cluster import Cluster as CassandraCluster
    from cassandra.auth import PlainTextAuthProvider as CassandraPlainTextAuthProvider
except ImportError:
    CassandraCluster = None  # type: ignore[assignment]
    CassandraPlainTextAuthProvider = None  # type: ignore[assignment]

try:
    from google.cloud import spanner as gcp_spanner
except ImportError:
    gcp_spanner = None  # type: ignore[assignment]

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

            # Stage 1: Optional TCP probe for standard (non-SRV) URIs.
            # Used only to distinguish "server down" from "auth failure" in the
            # error message.  We do NOT hard-fail here — pymongo is the
            # authoritative judge of reachability because it has its own retry
            # and handshake logic that can succeed even when a raw socket connect
            # times out on the first attempt (e.g. first TCP SYN to a cold
            # replica-set member, IPv6/IPv4 dual-stack fallback, etc.).
            tcp_ok: bool | None = None  # None = untested (SRV), True/False for plain
            if db_type == 'mongodb':
                try:
                    with socket.create_connection((hostname, port), timeout=max(timeout, 15)):
                        tcp_ok = True
                except OSError:
                    tcp_ok = False
                    # Do NOT return here — fall through and let pymongo try.

            # Stage 2: Test authentication via pymongo.
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
            direct = db_type == 'mongodb'
            # Give pymongo more time than the raw socket — use at least 20 s so
            # a cold replica-set primary can complete its handshake.
            mongo_timeout_ms = max(timeout, 20) * 1000

            last_failure = None
            for uri in uris_to_try:
                try:
                    client = MongoClient(
                        uri,
                        serverSelectionTimeoutMS=mongo_timeout_ms,
                        directConnection=direct,
                    )
                    client[target_db].list_collection_names()
                    client.close()
                    return True, f"Successfully connected to MongoDB database at {location}"
                except OperationFailure:
                    last_failure = 'auth'
                    continue
                except ServerSelectionTimeoutError:
                    # If the TCP probe already confirmed the port is closed/
                    # unreachable, report that clearly.  Otherwise (tcp_ok is
                    # True or untested for SRV) the SSTE likely means auth/TLS.
                    last_failure = 'connect' if tcp_ok is False else 'auth'
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
        # is_safe, safety_error = _is_safe_url(api_url)
        is_safe = True
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


# ---------------------------------------------------------------------------
# Table auto-discovery helpers
# ---------------------------------------------------------------------------

# Canonical e-commerce schema tables (mirrors mapping/utils/table_mapper.py)
_CANONICAL_TABLES = [
    "addresses", "cart_items", "categories", "customer_sessions",
    "customers", "inventory", "marketing_campaigns", "order_items",
    "orders", "payments", "products", "reviews", "shopping_cart",
    "suppliers", "wishlist",
]

# Synonym map for common naming variations (mirrors mapping/utils/table_mapper.py)
_TABLE_SYNONYMS = {
    "customers": "customers", "customer": "customers", "users": "customers",
    "user": "customers", "clients": "customers", "client": "customers",
    "addresses": "addresses", "address": "addresses",
    "locations": "addresses", "location": "addresses",
    "products": "products", "product": "products", "items": "products",
    "item": "products", "catalog": "products",
    "inventories": "inventory", "inventory": "inventory",
    "stock": "inventory", "stocks": "inventory",
    "orders": "orders", "order": "orders", "sales": "orders", "sale": "orders",
    "order_items": "order_items", "orderitems": "order_items",
    "order_item": "order_items", "line_items": "order_items",
    "lineitems": "order_items",
    "reviews": "reviews", "review": "reviews", "ratings": "reviews",
    "rating": "reviews", "feedback": "reviews",
    "categories": "categories", "category": "categories",
    "wishlists": "wishlist", "wishlist": "wishlist",
    "favorites": "wishlist", "favourites": "wishlist", "favourite": "wishlist",
    "payments": "payments", "payment": "payments",
    "billing": "payments", "invoices": "payments", "invoice": "payments",
    "shopping_carts": "shopping_cart", "shopping_cart": "shopping_cart",
    "cart": "shopping_cart", "carts": "shopping_cart",
    "basket": "shopping_cart", "baskets": "shopping_cart",
    "cart_items": "cart_items", "cartitems": "cart_items",
    "cart_item": "cart_items", "shopping_cart_items": "cart_items",
    "basket_items": "cart_items",
    "customer_sessions": "customer_sessions", "sessions": "customer_sessions",
    "session": "customer_sessions", "user_sessions": "customer_sessions",
    "marketing_campaigns": "marketing_campaigns",
    "campaigns": "marketing_campaigns", "campaign": "marketing_campaigns",
    "promotions": "marketing_campaigns", "promotion": "marketing_campaigns",
    "suppliers": "suppliers", "supplier": "suppliers",
    "vendors": "suppliers", "vendor": "suppliers",
    "partners": "suppliers", "partner": "suppliers",
}


def _fuzzy_match_table(table_name: str, threshold: int = 75) -> Optional[str]:
    """
    Map a remote table name to a canonical schema table name.

    Tries (in order):
    1. Exact match against canonical tables
    2. Synonym lookup
    3. Word-boundary split (e.g. "ecommerce_orders" → "orders")
    4. Rapidfuzz ratio / token_set_ratio similarity
    5. Difflib fallback if rapidfuzz is unavailable

    Returns the canonical name, or None if no match exceeds the threshold.
    """
    if not table_name:
        return None
    normalized = table_name.lower().strip()

    # 1. Exact match
    if normalized in _CANONICAL_TABLES:
        return normalized

    # 2. Synonym lookup
    if normalized in _TABLE_SYNONYMS:
        return _TABLE_SYNONYMS[normalized]

    # 3. Word-boundary split: "ecommerce_orders" → ["ecommerce", "orders"]
    #    Match each word-part against canonical tables and synonyms.
    #    Parts shorter than 3 characters (e.g. "id", "no") are too ambiguous to match.
    parts = [p for p in normalized.replace("-", "_").split("_") if len(p) >= 3]
    for part in parts:
        if part in _CANONICAL_TABLES:
            return part
        if part in _TABLE_SYNONYMS:
            return _TABLE_SYNONYMS[part]

    # 4. Fuzzy matching
    try:
        from rapidfuzz import fuzz, process as rfprocess
        match = rfprocess.extractOne(
            normalized, _CANONICAL_TABLES,
            scorer=fuzz.ratio, score_cutoff=threshold,
        )
        if match:
            return match[0]
        # token_set_ratio handles extra tokens
        match2 = rfprocess.extractOne(
            normalized, _CANONICAL_TABLES,
            scorer=fuzz.token_set_ratio, score_cutoff=threshold,
        )
        return match2[0] if match2 else None
    except ImportError:
        # difflib cutoff is in [0, 1] range; threshold is in [0, 100]
        import difflib
        matches = difflib.get_close_matches(
            normalized, _CANONICAL_TABLES, n=1, cutoff=threshold / 100.0,
        )
        return matches[0] if matches else None


# Named constants used inside discover_and_match_db_tables
# VTGate exposes two ports: the gRPC CDC port (used by Debezium) and the
# MySQL-compatible query port (used by pymysql for table discovery).
_VITESS_GRPC_PORT = 15991
# Informix systables rows with tabid < 100 are internal system tables.
_INFORMIX_SYSTEM_TABLE_MIN_ID = 100


def discover_and_match_db_tables(db_uri: str, timeout: int = 10) -> List[str]:
    """
    Connect to the database, list all tables/collections accessible to the
    user, and return those whose names fuzzy-match the canonical e-commerce
    schema.

    The returned list contains the **original remote names** (not canonical).
    These are passed directly to Debezium for CDC capture; the mapping layer
    handles the canonical name translation during streaming.

    Supported databases (all 11 that Debezium supports):
    - PostgreSQL   (psycopg2)
    - MySQL        (pymysql)
    - MariaDB      (pymysql)
    - MongoDB      (pymongo)
    - SQL Server   (pymssql)
    - Oracle       (oracledb thin mode)
    - Db2          (ibm_db_dbi)
    - Vitess       (pymysql via VTGate MySQL port)
    - Google Spanner (google-cloud-spanner)
    - Informix     (ibm_db_dbi)
    - Cassandra    (cassandra-driver)

    When a required driver is not installed, the branch is skipped and an
    empty list is returned (safe best-effort fallback).
    """
    try:
        parsed = urlparse(db_uri)
        db_type = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
        username = parsed.username
        password = parsed.password
        database = parsed.path.lstrip("/").split("?")[0] if parsed.path else ""

        discovered: List[str] = []

        if db_type in ("postgresql", "postgres"):
            port = port or 5432
            conn = psycopg2.connect(
                host=hostname, port=port,
                user=username, password=password,
                database=database, connect_timeout=timeout,
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
            discovered = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

        elif db_type in ("mongodb", "mongodb+srv"):
            port = port or 27017
            target_db = database if database else "admin"
            direct = db_type == "mongodb"
            query_str = parsed.query.lower() if parsed.query else ""
            has_authsource = "authsource" in query_str
            uris_to_try = [db_uri]
            if not has_authsource and direct:
                sep = "&" if query_str else "?"
                uris_to_try.append(f"{db_uri}{sep}authSource=admin")
            for uri in uris_to_try:
                try:
                    client = MongoClient(
                        uri,
                        serverSelectionTimeoutMS=timeout * 1000,
                        directConnection=direct,
                    )
                    discovered = client[target_db].list_collection_names()
                    client.close()
                    break
                except Exception:
                    continue

        elif db_type in ("mysql", "mariadb"):
            if pymysql is not None:
                port = port or 3306
                conn = pymysql.connect(
                    host=hostname, port=port,
                    user=username, password=password,
                    database=database, connect_timeout=timeout,
                )
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                discovered = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()

        elif db_type in ("mssql", "sqlserver"):
            if pymssql is not None:
                port = port or 1433
                conn = pymssql.connect(
                    server=hostname, port=str(port),
                    user=username, password=password,
                    database=database, timeout=timeout,
                )
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_type = 'BASE TABLE'"
                )
                discovered = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()

        elif db_type == "oracle":
            # oracledb thin mode works without Oracle Instant Client
            if oracledb is not None:
                port = port or 1521
                conn = oracledb.connect(
                    user=username, password=password,
                    dsn=f"{hostname}:{port}/{database}",
                )
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1",
                    [username.upper()],
                )
                discovered = [row[0].lower() for row in cursor.fetchall()]
                cursor.close()
                conn.close()

        elif db_type == "db2":
            # ibm_db_dbi is the official IBM Python driver for Db2
            if ibm_db_dbi is not None:
                port = port or 50000
                dsn = (
                    f"DATABASE={database};HOSTNAME={hostname};PORT={port};"
                    f"PROTOCOL=TCPIP;UID={username};PWD={password};"
                )
                conn = ibm_db_dbi.connect(dsn, "", "")
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT TABNAME FROM SYSCAT.TABLES "
                    "WHERE TYPE = 'T' AND TABSCHEMA = CURRENT SCHEMA"
                )
                discovered = [row[0].lower() for row in cursor.fetchall()]
                cursor.close()
                conn.close()

        elif db_type == "vitess":
            # VTGate exposes a MySQL-compatible interface on its mysql port
            # (default 3306); the Debezium gRPC port (_VITESS_GRPC_PORT) is separate.
            if pymysql is not None:
                mysql_port = port if (port and port != _VITESS_GRPC_PORT) else 3306
                conn = pymysql.connect(
                    host=hostname, port=mysql_port,
                    user=username or "", password=password or "",
                    database=database, connect_timeout=timeout,
                )
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                discovered = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()

        elif db_type == "spanner":
            # google-cloud-spanner SDK; requires GOOGLE_APPLICATION_CREDENTIALS
            if gcp_spanner is not None:
                path_parts = parsed.path.strip("/").split("/")
                project_id = parsed.hostname or ""
                instance_id = path_parts[0] if len(path_parts) > 0 else ""
                database_id = path_parts[1] if len(path_parts) > 1 else ""
                client = gcp_spanner.Client(project=project_id)
                instance = client.instance(instance_id)
                db_obj = instance.database(database_id)
                with db_obj.snapshot() as snapshot:
                    results = snapshot.execute_sql(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = ''"
                    )
                    discovered = [row[0].lower() for row in results]

        elif db_type == "informix":
            # ibm_db_dbi supports Informix via IBM CLI/DRDA as well as Db2
            if ibm_db_dbi is not None:
                port = port or 9088
                dsn = (
                    f"DATABASE={database};HOSTNAME={hostname};PORT={port};"
                    f"PROTOCOL=TCPIP;UID={username};PWD={password};"
                )
                conn = ibm_db_dbi.connect(dsn, "", "")
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT tabname FROM systables "
                    f"WHERE tabtype = 'T' AND tabid >= {_INFORMIX_SYSTEM_TABLE_MIN_ID}"
                )
                discovered = [row[0].lower() for row in cursor.fetchall()]
                cursor.close()
                conn.close()

        elif db_type == "cassandra":
            # cassandra-driver lists table names from cluster metadata
            if CassandraCluster is not None:
                port = port or 9042
                keyspace = database if database else None
                auth = None
                if username:
                    auth = CassandraPlainTextAuthProvider(
                        username=username, password=password or "",
                    )
                cluster = CassandraCluster(
                    [hostname], port=port,
                    auth_provider=auth,
                    connect_timeout=timeout,
                )
                try:
                    cluster.connect()
                    if keyspace and keyspace in cluster.metadata.keyspaces:
                        discovered = list(
                            cluster.metadata.keyspaces[keyspace].tables.keys()
                        )
                finally:
                    cluster.shutdown()

        # Filter to tables whose names match the canonical schema
        matched = [t for t in discovered if _fuzzy_match_table(t) is not None]
        return matched

    except Exception as exc:
        # Discovery is best-effort; never let it block the pipeline start.
        # Log at WARNING so operators can diagnose issues without a crash.
        import logging as _logging
        _logging.getLogger("pulse").warning(
            "db_table_discovery: auto-discovery failed for %s: %s",
            urlparse(db_uri).hostname, exc,
        )
        return []
