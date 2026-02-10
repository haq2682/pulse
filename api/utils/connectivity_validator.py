"""
Connectivity validation utilities for database and API endpoints.
Used to test connections before starting mapping pipelines.
"""

import requests
from urllib.parse import urlparse
from typing import Dict, Tuple
import pymysql
import psycopg2
from pymongo import MongoClient
import pymssql


def validate_database_connection(db_uri: str, timeout: int = 10) -> Tuple[bool, str]:
    """
    Test database connectivity and return status with error message.
    
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
        
        if not database:
            return False, "Invalid database URI: database name not found"
        
        # Test connection based on database type
        if db_type in ['postgresql', 'postgres']:
            port = port or 5432
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
                    
        elif db_type in ['mysql', 'mariadb']:
            port = port or 3306
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
                return True, f"Successfully connected to MySQL/MariaDB database at {hostname}:{port}"
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
                    
        elif db_type in ['mongodb', 'mongodb+srv']:
            port = port or 27017
            try:
                # For MongoDB, construct connection string differently
                if username and password:
                    mongo_uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"
                else:
                    mongo_uri = f"mongodb://{hostname}:{port}/{database}"
                    
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=timeout * 1000)
                # Test connection by listing databases
                client.server_info()
                client.close()
                return True, f"Successfully connected to MongoDB database at {hostname}:{port}"
            except Exception as e:
                error_msg = str(e)
                if "authentication failed" in error_msg.lower():
                    return False, f"Authentication failed for MongoDB at {hostname}:{port}. Check username and password."
                elif "connection refused" in error_msg.lower() or "timeout" in error_msg.lower():
                    return False, f"Cannot connect to MongoDB at {hostname}:{port}. Server may be down or unreachable."
                else:
                    return False, f"MongoDB connection error: {error_msg}"
                    
        elif db_type in ['mssql', 'sqlserver']:
            port = port or 1433
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
        else:
            # For other database types, just return a generic message
            return True, f"Database type '{db_type}' will be validated by Debezium connector"
            
    except Exception as e:
        return False, f"Error validating database connection: {str(e)}"


def validate_api_endpoint(api_url: str, timeout: int = 10) -> Tuple[bool, str]:
    """
    Test API endpoint connectivity and return status with error message.
    
    Args:
        api_url: API endpoint URL
        timeout: Request timeout in seconds
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Validate URL format
        parsed = urlparse(api_url)
        if not parsed.scheme in ['http', 'https']:
            return False, "Invalid API URL: must start with http:// or https://"
        
        if not parsed.netloc:
            return False, "Invalid API URL: hostname not found"
        
        # Make test request
        try:
            response = requests.get(api_url, timeout=timeout)
            
            if response.status_code == 200:
                return True, f"Successfully connected to API endpoint at {api_url}"
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
