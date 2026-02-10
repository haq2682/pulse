"""
Debezium Connector Manager: Deploy and manage CDC connectors via Kafka Connect REST API.

Supports all Debezium source connectors:
- PostgreSQL, MySQL, MariaDB, MongoDB, SQL Server, Oracle, Db2,
  Vitess, Spanner, Informix, Cassandra

The user provides a database URI. This module auto-detects the database type,
builds the correct Debezium connector configuration, and deploys it to
Kafka Connect via its REST API.

Usage:
    from streaming.ingestion.debezium_connector_manager import DebeziumConnectorManager

    manager = DebeziumConnectorManager()
    config = manager.create_connector_config(
        db_uri="postgresql://debezium_user:pass@host:5432/mydb",
        tables=["orders", "payments"],
    )
    manager.deploy_connector(config)
"""

import time
import json
import requests
from urllib.parse import urlparse, parse_qs
from typing import List, Dict, Any, Optional

# Kafka bootstrap inside the docker network
KAFKA_BOOTSTRAP_INTERNAL = "10.5.0.7:9092"

# URI scheme -> internal db type key
URI_SCHEME_MAP = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mongodb": "mongodb",
    "mongodb+srv": "mongodb",
    "mssql": "sqlserver",
    "sqlserver": "sqlserver",
    "oracle": "oracle",
    "db2": "db2",
    "vitess": "vitess",
    "spanner": "spanner",
    "informix": "informix",
    "cassandra": "cassandra",
}

# Default ports per database type
DEFAULT_PORTS = {
    "postgres": 5432,
    "mysql": 3306,
    "mariadb": 3306,
    "mongodb": 27017,
    "sqlserver": 1433,
    "oracle": 1521,
    "db2": 50000,
    "vitess": 15991,
    "informix": 9088,
    "cassandra": 9042,
}

# Debezium connector classes
CONNECTOR_CLASSES = {
    "postgres": "io.debezium.connector.postgresql.PostgresConnector",
    "mysql": "io.debezium.connector.mysql.MySqlConnector",
    "mariadb": "io.debezium.connector.mariadb.MariaDbConnector",
    "mongodb": "io.debezium.connector.mongodb.MongoDbConnector",
    "sqlserver": "io.debezium.connector.sqlserver.SqlServerConnector",
    "oracle": "io.debezium.connector.oracle.OracleConnector",
    "db2": "io.debezium.connector.db2.Db2Connector",
    "vitess": "io.debezium.connector.vitess.VitessConnector",
    "spanner": "io.debezium.connector.spanner.SpannerConnector",
    "informix": "io.debezium.connector.informix.InformixConnector",
    "cassandra": "io.debezium.connector.cassandra.CassandraConnector",
}

# Databases that need schema.history.internal Kafka topics for DDL tracking
NEEDS_SCHEMA_HISTORY = {"mysql", "mariadb", "sqlserver", "oracle", "db2", "informix"}


def parse_db_uri(uri: str) -> Dict[str, Any]:
    """
    Parse a database URI into its components and detect the database type.

    Supported URI formats:
        postgresql://user:pass@host:5432/dbname
        mysql://user:pass@host:3306/dbname
        mariadb://user:pass@host:3306/dbname
        mongodb://user:pass@host:27017/dbname
        mssql://user:pass@host:1433/dbname
        oracle://user:pass@host:1521/service_name
        db2://user:pass@host:50000/dbname
        vitess://user:pass@host:15991/keyspace
        spanner://project_id/instance_id/database_id
        informix://user:pass@host:9088/dbname
        cassandra://user:pass@host:9042/keyspace

    Returns:
        Dictionary with keys: db_type, host, port, user, password, database, raw_uri
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    db_type = URI_SCHEME_MAP.get(scheme)
    if not db_type:
        supported = sorted(set(URI_SCHEME_MAP.values()))
        raise ValueError(
            f"Unsupported database URI scheme: '{scheme}'. "
            f"Supported types: {', '.join(supported)}"
        )

    # Spanner uses a special URI format: spanner://project/instance/database
    if db_type == "spanner":
        path_parts = parsed.path.strip("/").split("/")
        project_id = parsed.hostname or (path_parts[0] if len(path_parts) > 0 else "")
        instance_id = path_parts[1] if len(path_parts) > 1 else (path_parts[0] if parsed.hostname else "")
        database_id = path_parts[2] if len(path_parts) > 2 else (path_parts[1] if parsed.hostname else "")

        # Handle spanner://project/instance/database format
        if parsed.hostname:
            project_id = parsed.hostname
            path_parts = parsed.path.strip("/").split("/")
            instance_id = path_parts[0] if len(path_parts) > 0 else ""
            database_id = path_parts[1] if len(path_parts) > 1 else ""

        return {
            "db_type": "spanner",
            "host": None,
            "port": None,
            "user": None,
            "password": None,
            "database": database_id,
            "spanner_project_id": project_id,
            "spanner_instance_id": instance_id,
            "spanner_database_id": database_id,
            "raw_uri": uri,
        }

    default_port = DEFAULT_PORTS.get(db_type)

    return {
        "db_type": db_type,
        "host": parsed.hostname or "localhost",
        "port": parsed.port or default_port,
        "user": parsed.username or "",
        "password": parsed.password or "",
        "database": parsed.path.lstrip("/").split("?")[0] if parsed.path else "",
        "raw_uri": uri,
    }


def _common_converter_config() -> Dict[str, str]:
    """Common Kafka converter settings shared by all connectors."""
    return {
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
    }


def _schema_history_config(db_name: str) -> Dict[str, str]:
    """Schema history config for databases that track DDL changes."""
    return {
        "schema.history.internal.kafka.bootstrap.servers": KAFKA_BOOTSTRAP_INTERNAL,
        "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
    }


def _build_table_include_list(
    db_type: str, tables: List[str], db_name: str, schema: str = "public"
) -> str:
    """
    Build the table.include.list value for a given database type.

    Different databases use different qualification:
        postgres:   public.table
        mysql:      dbname.table
        mariadb:    dbname.table
        sqlserver:  dbo.table
        oracle:     SCHEMA.TABLE
        db2:        SCHEMA.TABLE
        vitess:     keyspace.table
        informix:   informix.table
    """
    prefix_map = {
        "postgres": schema,
        "mysql": db_name,
        "mariadb": db_name,
        "sqlserver": "dbo",
        "oracle": schema.upper(),
        "db2": schema.upper(),
        "vitess": db_name,
        "informix": f"{db_name}.{schema}",
    }
    prefix = prefix_map.get(db_type, db_name)
    return ",".join(f"{prefix}.{t}" for t in tables)


# ── Per-database config builders ────────────────────────────────────────────


def _postgres_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["postgres"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "postgres", tables, parsed["database"]
        ),
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    return config


def _mysql_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["mysql"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.server.id": "184054",
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "mysql", tables, parsed["database"]
        ),
        "database.include.list": parsed["database"],
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _mariadb_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["mariadb"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.server.id": "184055",
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "mariadb", tables, parsed["database"]
        ),
        "database.include.list": parsed["database"],
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _mongodb_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    """
    MongoDB uses collection.include.list instead of table.include.list.
    The raw URI is passed as mongodb.connection.string.
    """
    collection_list = ",".join(f"{parsed['database']}.{t}" for t in tables)
    config = {
        "connector.class": CONNECTOR_CLASSES["mongodb"],
        "mongodb.connection.string": parsed["raw_uri"],
        "topic.prefix": topic_prefix,
        "collection.include.list": collection_list,
        "capture.mode": "change_streams_update_full",
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    return config


def _sqlserver_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["sqlserver"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.names": parsed["database"],
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "sqlserver", tables, parsed["database"]
        ),
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _oracle_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["oracle"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "oracle", tables, parsed["database"], schema=parsed["user"]
        ),
        "database.connection.adapter": "logminer",
        "log.mining.strategy": "online_catalog",
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _db2_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["db2"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "db2", tables, parsed["database"], schema=parsed["user"]
        ),
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _vitess_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    """
    Vitess connects to VTGate via gRPC. No schema history needed.
    """
    config = {
        "connector.class": CONNECTOR_CLASSES["vitess"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "vitess.keyspace": parsed["database"],
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "vitess", tables, parsed["database"]
        ),
        "vitess.tablet.type": "MASTER",
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    return config


def _spanner_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    """
    Spanner uses GCP credentials, not host/user/password.
    URI format: spanner://project_id/instance_id/database_id
    The user must set GOOGLE_APPLICATION_CREDENTIALS env var or provide
    gcp.spanner.credentials.path / gcp.spanner.credentials.json.
    """
    config = {
        "connector.class": CONNECTOR_CLASSES["spanner"],
        "gcp.spanner.project.id": parsed.get("spanner_project_id", ""),
        "gcp.spanner.instance.id": parsed.get("spanner_instance_id", ""),
        "gcp.spanner.database.id": parsed.get("spanner_database_id", ""),
        "gcp.spanner.change.stream": "pulse_change_stream",
        "topic.prefix": topic_prefix,
    }
    config.update(_common_converter_config())
    return config


def _informix_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    config = {
        "connector.class": CONNECTOR_CLASSES["informix"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "table.include.list": _build_table_include_list(
            "informix", tables, parsed["database"]
        ),
        "snapshot.mode": "initial",
    }
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _cassandra_config(parsed: Dict, tables: List[str], topic_prefix: str) -> Dict:
    """
    Cassandra CDC connector. Note: the Cassandra connector runs as a
    standalone agent (NOT inside Kafka Connect) in production. This config
    is for the Kafka Connect-compatible wrapper / Debezium Server mode.
    CDC must be enabled per-table in Cassandra (ALTER TABLE ... WITH cdc = true).
    """
    config = {
        "connector.class": CONNECTOR_CLASSES["cassandra"],
        "cassandra.hosts": parsed["host"],
        "cassandra.port": str(parsed["port"]),
        "cassandra.user": parsed["user"],
        "cassandra.password": parsed["password"],
        "cassandra.keyspace": parsed["database"],
        "topic.prefix": topic_prefix,
        "snapshot.mode": "INITIAL",
        "kafka.producer.bootstrap.servers": KAFKA_BOOTSTRAP_INTERNAL,
    }
    config.update(_common_converter_config())
    return config


# Dispatch map
_CONFIG_BUILDERS = {
    "postgres": _postgres_config,
    "mysql": _mysql_config,
    "mariadb": _mariadb_config,
    "mongodb": _mongodb_config,
    "sqlserver": _sqlserver_config,
    "oracle": _oracle_config,
    "db2": _db2_config,
    "vitess": _vitess_config,
    "spanner": _spanner_config,
    "informix": _informix_config,
    "cassandra": _cassandra_config,
}


class DebeziumConnectorManager:
    """Manages Debezium CDC connectors via Kafka Connect REST API."""

    def __init__(self, connect_url: str = "http://10.5.0.10:8083"):
        """
        Initialize connector manager.

        Args:
            connect_url: Kafka Connect REST API URL (default: Debezium container IP)
        """
        self.connect_url = connect_url.rstrip("/")

    def wait_for_connect(self, timeout: int = 120, interval: int = 5) -> bool:
        """
        Wait for Kafka Connect to be available.

        Args:
            timeout: Maximum seconds to wait
            interval: Seconds between retries

        Returns:
            True if Connect is available, False if timeout reached
        """
        elapsed = 0
        while elapsed < timeout:
            try:
                resp = requests.get(f"{self.connect_url}/connectors", timeout=10)
                if resp.status_code == 200:
                    print(f"Kafka Connect is ready at {self.connect_url}")
                    return True
            except requests.ConnectionError:
                pass
            except requests.Timeout:
                pass

            print(f"Waiting for Kafka Connect... ({elapsed}s/{timeout}s)")
            time.sleep(interval)
            elapsed += interval

        print(f"Kafka Connect not available after {timeout}s")
        return False

    def create_connector_config(
        self,
        db_uri: str,
        tables: List[str],
        connector_name: str = "pulse-cdc-connector",
        topic_prefix: str = "ecom",
    ) -> Dict[str, Any]:
        """
        Auto-detect database type from URI and build the Debezium connector config.

        Args:
            db_uri: Database connection URI provided by the user
            tables: List of table/collection names to capture
            connector_name: Unique name for the connector instance
            topic_prefix: Kafka topic prefix (topics: {prefix}.{schema}.{table})

        Returns:
            Connector configuration dict ready for deploy_connector()

        Raises:
            ValueError: If URI scheme is unsupported
        """
        parsed = parse_db_uri(db_uri)
        db_type = parsed["db_type"]

        builder = _CONFIG_BUILDERS.get(db_type)
        if not builder:
            raise ValueError(f"No Debezium config builder for database type: {db_type}")

        config = builder(parsed, tables, topic_prefix)

        print(f"  Detected database type: {db_type}")
        print(f"  Connector class: {config['connector.class']}")
        if parsed.get("host"):
            print(f"  Host: {parsed['host']}:{parsed['port']}")
        print(f"  Database: {parsed.get('database', 'N/A')}")
        print(f"  Tables: {tables}")
        print(f"  Topic prefix: {topic_prefix}")

        return {"name": connector_name, "config": config}

    def deploy_connector(self, config: Dict[str, Any]) -> bool:
        """
        Deploy a connector to Kafka Connect.

        If a connector with the same name exists, it will be updated.

        Args:
            config: Connector configuration dictionary

        Returns:
            True if deployment succeeded, False otherwise
        """
        connector_name = config["name"]

        try:
            # Check if connector already exists
            resp = requests.get(
                f"{self.connect_url}/connectors/{connector_name}", timeout=10
            )

            if resp.status_code == 200:
                # Update existing connector
                print(f"Updating existing connector: {connector_name}")
                resp = requests.put(
                    f"{self.connect_url}/connectors/{connector_name}/config",
                    json=config["config"],
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
            else:
                # Create new connector
                print(f"Creating new connector: {connector_name}")
                resp = requests.post(
                    f"{self.connect_url}/connectors",
                    json=config,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )

            if resp.status_code in (200, 201):
                print(f"Connector '{connector_name}' deployed successfully")
                return True
            else:
                print(
                    f"Failed to deploy connector: {resp.status_code} - {resp.text}"
                )
                return False

        except requests.RequestException as e:
            print(f"Error deploying connector: {e}")
            return False

    def get_connector_status(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a deployed connector.

        Args:
            connector_name: Name of the connector

        Returns:
            Status dictionary or None if connector not found
        """
        try:
            resp = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/status", timeout=10
            )
            if resp.status_code == 200:
                return resp.json()
            return None
        except requests.RequestException:
            return None

    def delete_connector(self, connector_name: str) -> bool:
        """
        Delete a connector.

        Args:
            connector_name: Name of the connector to delete

        Returns:
            True if deleted, False otherwise
        """
        try:
            resp = requests.delete(
                f"{self.connect_url}/connectors/{connector_name}", timeout=10
            )
            return resp.status_code in (200, 204)
        except requests.RequestException:
            return False

    def list_connectors(self) -> List[str]:
        """
        List all deployed connectors.

        Returns:
            List of connector names
        """
        try:
            resp = requests.get(f"{self.connect_url}/connectors", timeout=10)
            if resp.status_code == 200:
                return resp.json()
            return []
        except requests.RequestException:
            return []
