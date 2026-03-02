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
import hashlib
import os
import requests
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import List, Dict, Any, Optional

try:
    from pymongo import MongoClient as _MongoClient
    from pymongo.errors import PyMongoError as _PyMongoError
except ImportError:  # pragma: no cover
    _MongoClient = None  # type: ignore[assignment,misc]
    _PyMongoError = Exception  # type: ignore[assignment,misc]

# MySQL/MariaDB server ID derivation constants.
# The server ID must be in [_MYSQL_SERVER_ID_BASE, _MYSQL_SERVER_ID_BASE + _MYSQL_SERVER_ID_RANGE).
_MYSQL_SERVER_ID_BASE = 100000
_MYSQL_SERVER_ID_RANGE = 900000

# Kafka Connect deploy timeout: (connect_timeout, read_timeout) in seconds.
# The POST/PUT deploy calls are synchronous — Kafka Connect validates the
# connector config (including a live DB connection) before returning, so
# read timeouts must be generous.  The connect timeout stays short.
_DEPLOY_CONNECT_TIMEOUT = 10
_DEPLOY_READ_TIMEOUT = 120

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


def _postgres_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    # Derive a slot name that is unique per connector while being a valid PostgreSQL
    # identifier (≤63 chars, alphanumeric + underscores only).
    safe_name = connector_name.replace("-", "_").replace(".", "_")
    slot_name = f"dz_{safe_name}"[:63]
    table_include = _build_table_include_list("postgres", tables, parsed["database"])
    config = {
        "connector.class": CONNECTOR_CLASSES["postgres"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "plugin.name": "pgoutput",
        "slot.name": slot_name,
        "publication.autocreate.mode": "filtered" if table_include else "all_tables",
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    return config


def _mysql_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    # MySQL server ID must be unique across the replication topology.
    # Derive a stable numeric ID from the connector name so that the same
    # connector always produces the same ID (idempotent restarts).
    server_id = int(hashlib.md5(connector_name.encode()).hexdigest()[:8], 16) % _MYSQL_SERVER_ID_RANGE + _MYSQL_SERVER_ID_BASE
    table_include = _build_table_include_list("mysql", tables, parsed["database"])
    config = {
        "connector.class": CONNECTOR_CLASSES["mysql"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.server.id": str(server_id),
        "topic.prefix": topic_prefix,
        "database.include.list": parsed["database"],
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _mariadb_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    # Derive a unique server ID for MariaDB (same approach as MySQL).
    server_id = int(hashlib.md5(connector_name.encode()).hexdigest()[:8], 16) % _MYSQL_SERVER_ID_RANGE + _MYSQL_SERVER_ID_BASE
    table_include = _build_table_include_list("mariadb", tables, parsed["database"])
    config = {
        "connector.class": CONNECTOR_CLASSES["mariadb"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.server.id": str(server_id),
        "topic.prefix": topic_prefix,
        "database.include.list": parsed["database"],
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _get_mongodb_replica_set_name(host: str, port: int, auth_uri: str) -> Optional[str]:
    """
    Connect to the MongoDB node at host:port and return the replica-set name
    (``setName``) if the server is part of a replica set, otherwise None.

    Uses directConnection=True so that pymongo does not attempt replica-set
    member discovery (which could fail if internal hostnames are unreachable).
    Runs the lightweight ``hello`` admin command — available in all MongoDB
    versions ≥ 5.0 (older servers answer the legacy ``isMaster`` alias).

    Returns None on any error so that callers can degrade gracefully.
    """
    if _MongoClient is None:
        return None
    try:
        client = _MongoClient(
            auth_uri,
            directConnection=True,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
        )
        result = client.admin.command("hello")
        client.close()
        return result.get("setName") or None
    except Exception:  # noqa: BLE001 – any error means we cannot determine RS name
        return None


def _mongodb_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    """
    MongoDB uses collection.include.list instead of table.include.list.
    The raw URI is passed as mongodb.connection.string.
    When tables is empty, collection.include.list is omitted so Debezium
    captures all collections (safe fallback when auto-discovery is unavailable).

    authSource handling: MongoDB users (e.g. debezium_user) are typically
    created in the 'admin' database.  If the URI path database (e.g. /pulse-test)
    is used as the authSource, authentication will fail.  When no explicit
    authSource is present in the URI we therefore append authSource=admin so
    that Debezium authenticates against the admin database regardless of which
    application database is being captured.

    replicaSet handling: Debezium's MongoDB connector rejects the config with
    "Replica set not specified" when the cluster topology is REPLICA_SET but
    ``replicaSet`` is absent from the connection string.  We auto-discover the
    replica-set name via the ``hello`` admin command and inject it when needed.

    directConnection handling: When a replica set is involved the Java MongoDB
    driver inside Kafka Connect performs topology discovery.  RS members
    commonly advertise an internal hostname (e.g. ``localhost:27017``) that is
    unreachable from the Kafka Connect container.  Adding ``directConnection=true``
    tells the driver to connect *only* to the explicitly specified host and skip
    topology-driven host substitution, while still allowing Debezium to run
    change streams (which only require the server to be part of a replica set,
    not that the driver resolves every member).
    """
    raw_uri = parsed["raw_uri"]
    parsed_uri = urlparse(raw_uri)
    query_str = parsed_uri.query.lower()

    # 1. Ensure authSource=admin so Debezium can authenticate.
    if "authsource" not in query_str:
        sep = "&" if parsed_uri.query else "?"
        raw_uri = f"{raw_uri}{sep}authSource=admin"
        # Re-parse so query_str reflects the addition.
        parsed_uri = urlparse(raw_uri)
        query_str = parsed_uri.query.lower()

    # 2. Inject replicaSet= when the server is part of a replica set and the
    #    caller has not already specified it in the connection string.
    if "replicaset" not in query_str:
        rs_name = _get_mongodb_replica_set_name(
            parsed["host"], parsed["port"], raw_uri
        )
        if rs_name:
            sep = "&" if parsed_uri.query else "?"
            raw_uri = f"{raw_uri}{sep}replicaSet={rs_name}"
            # Re-parse so query_str is current for the next step.
            parsed_uri = urlparse(raw_uri)
            query_str = parsed_uri.query.lower()
            print(f"  Auto-detected MongoDB replica set: {rs_name}")

    # 3. Add directConnection=true so the Java MongoDB driver inside Kafka
    #    Connect connects directly to the host in the URI rather than
    #    re-resolving members from the RS topology.  Without this, the driver
    #    follows the replica set's advertised member list (which often contains
    #    internal hostnames like localhost:27017 that are unreachable from the
    #    Kafka Connect container) and the connector fails to start.
    if "directconnection" not in query_str:
        sep = "&" if parsed_uri.query else "?"
        raw_uri = f"{raw_uri}{sep}directConnection=true"

    config = {
        "connector.class": CONNECTOR_CLASSES["mongodb"],
        "mongodb.connection.string": raw_uri,
        "topic.prefix": topic_prefix,
        "capture.mode": "change_streams_update_full",
        "snapshot.mode": "initial",
    }
    if tables:
        config["collection.include.list"] = ",".join(
            f"{parsed['database']}.{t}" for t in tables
        )
    config.update(_common_converter_config())
    return config


def _sqlserver_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
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


def _oracle_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    table_include = _build_table_include_list(
        "oracle", tables, parsed["database"], schema=parsed["user"]
    )
    config = {
        "connector.class": CONNECTOR_CLASSES["oracle"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "database.connection.adapter": "logminer",
        "log.mining.strategy": "online_catalog",
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _db2_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    table_include = _build_table_include_list(
        "db2", tables, parsed["database"], schema=parsed["user"]
    )
    config = {
        "connector.class": CONNECTOR_CLASSES["db2"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _vitess_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    """
    Vitess connects to VTGate via gRPC. No schema history needed.
    """
    table_include = _build_table_include_list("vitess", tables, parsed["database"])
    config = {
        "connector.class": CONNECTOR_CLASSES["vitess"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "vitess.keyspace": parsed["database"],
        "topic.prefix": topic_prefix,
        "vitess.tablet.type": "PRIMARY",
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    return config


def _spanner_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    """
    Spanner uses GCP credentials, not host/user/password.
    URI format: spanner://project_id/instance_id/database_id
    The user must set GOOGLE_APPLICATION_CREDENTIALS env var or provide
    gcp.spanner.credentials.path / gcp.spanner.credentials.json.
    Spanner Debezium connector does not support table.include.list;
    filtering is done via the change stream definition itself.
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


def _informix_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
    table_include = _build_table_include_list("informix", tables, parsed["database"])
    config = {
        "connector.class": CONNECTOR_CLASSES["informix"],
        "database.hostname": parsed["host"],
        "database.port": str(parsed["port"]),
        "database.user": parsed["user"],
        "database.password": parsed["password"],
        "database.dbname": parsed["database"],
        "topic.prefix": topic_prefix,
        "snapshot.mode": "initial",
    }
    if table_include:
        config["table.include.list"] = table_include
    config.update(_common_converter_config())
    config.update(_schema_history_config(parsed["database"]))
    return config


def _cassandra_config(parsed: Dict, tables: List[str], topic_prefix: str, connector_name: str = "pulse-cdc-connector") -> Dict:
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

    def __init__(self, connect_url: str = None):
        """
        Initialize connector manager.

        Args:
            connect_url: Kafka Connect REST API URL. Defaults to the
                         DEBEZIUM_URL environment variable, falling back to
                         the hardcoded container IP if the variable is unset.
        """
        if connect_url is None:
            connect_url = os.getenv("DEBEZIUM_URL", "http://10.5.0.10:8083")
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

        config = builder(parsed, tables, topic_prefix, connector_name)

        print(f"  Detected database type: {db_type}")
        print(f"  Connector class: {config['connector.class']}")
        if parsed.get("host"):
            print(f"  Host: {parsed['host']}:{parsed['port']}")
        print(f"  Database: {parsed.get('database', 'N/A')}")
        print(f"  Tables: {tables}")
        print(f"  Topic prefix: {topic_prefix}")

        return {"name": connector_name, "config": config}

    def _wait_for_task_state(
        self, connector_name: str, desired_states: set, timeout: int = 30
    ) -> Optional[str]:
        """
        Poll connector task status until any task reaches one of the desired
        states or the timeout expires.  Returns the observed state or None.
        """
        import time as _time
        deadline = _time.monotonic() + timeout
        while _time.monotonic() < deadline:
            status = self.get_connector_status(connector_name)
            if status:
                for task in status.get("tasks", []):
                    if task.get("state") in desired_states:
                        return task["state"]
            _time.sleep(2)
        return None

    def _reset_offsets(self, connector_name: str) -> bool:
        """
        Clear stored Kafka Connect offsets for *connector_name* using the
        stop → DELETE /offsets → resume flow required by Kafka Connect 3.6+.

        Returns True if offsets were successfully cleared, False otherwise.
        Silently succeeds if the connector has no stored offsets.
        """
        import time as _time
        short = (5, 15)

        # 1. Stop the connector (moves it to STOPPED state).
        stop_resp = requests.put(
            f"{self.connect_url}/connectors/{connector_name}/stop",
            timeout=short,
        )
        if stop_resp.status_code not in (200, 202, 204):
            print(f"  ⚠️  Could not stop connector for offset reset: {stop_resp.status_code}")
            return False

        # 2. Wait until STOPPED.
        stopped = self._wait_for_task_state(connector_name, {"STOPPED"}, timeout=20)
        # Kafka Connect reports STOPPED at connector level, not task level.
        # Fall back to checking the connector-level state after a brief wait.
        if not stopped:
            _time.sleep(5)

        # 3. Delete offsets.
        del_resp = requests.delete(
            f"{self.connect_url}/connectors/{connector_name}/offsets",
            timeout=short,
        )
        if del_resp.status_code in (200, 204):
            print(f"  ✅ Stale offsets cleared for '{connector_name}'")
        else:
            print(
                f"  ⚠️  Offset delete returned {del_resp.status_code}: {del_resp.text[:120]}"
            )

        # 4. Resume the connector.
        resume_resp = requests.post(
            f"{self.connect_url}/connectors/{connector_name}/resume",
            timeout=short,
        )
        if resume_resp.status_code not in (200, 202, 204):
            print(f"  ⚠️  Could not resume connector after offset reset: {resume_resp.status_code}")
            return False

        _time.sleep(3)
        return True

    def deploy_connector(self, config: Dict[str, Any]) -> bool:
        """
        Deploy a connector to Kafka Connect.

        If a connector with the same name exists it is updated; otherwise a
        new connector is created.  After deployment, connector task health is
        verified for up to 30 seconds.  If a task enters FAILED state due to
        stale / corrupted Kafka Connect offsets (e.g. after a manual connector
        delete followed by re-deploy) the method automatically:
          1. Stops the connector.
          2. Clears stored offsets via DELETE /connectors/{name}/offsets.
          3. Resumes the connector for a clean fresh run.

        PUT path snapshot-mode override
        --------------------------------
        When updating an already-running connector the snapshot.mode is
        overridden from "initial" to "no_data" so that a config change does
        NOT trigger a full re-snapshot of the database, which would flood
        Kafka with duplicate messages (Kafka does not auto-delete old records).
        "no_data" tells Debezium to skip the snapshot and stream from the
        current WAL / oplog position instead.

        Args:
            config: Connector configuration dictionary

        Returns:
            True if deployment succeeded, False otherwise
        """
        import time as _time

        connector_name = config["name"]
        # Use a generous read timeout for all deploy-path requests.
        # Kafka Connect validates the connector config synchronously (including
        # opening a live connection to the source database) before returning,
        # which can take well over 10 seconds for remote or replica-set databases.
        deploy_timeout = (_DEPLOY_CONNECT_TIMEOUT, _DEPLOY_READ_TIMEOUT)

        try:
            # Check if connector already exists
            resp = requests.get(
                f"{self.connect_url}/connectors/{connector_name}",
                timeout=deploy_timeout,
            )

            is_new = resp.status_code != 200

            if not is_new:
                # Update existing connector.
                # IMPORTANT: override snapshot.mode to "no_data" so that a
                # config update does NOT trigger a new full database snapshot.
                # "no_data" tells Debezium to skip the snapshot and resume CDC
                # streaming from the current oplog / WAL position instead.
                update_config = dict(config["config"])
                if update_config.get("snapshot.mode") == "initial":
                    update_config["snapshot.mode"] = "no_data"
                print(f"Updating existing connector: {connector_name}")
                resp = requests.put(
                    f"{self.connect_url}/connectors/{connector_name}/config",
                    json=update_config,
                    headers={"Content-Type": "application/json"},
                    timeout=deploy_timeout,
                )
            else:
                # Create new connector.
                print(f"Creating new connector: {connector_name}")
                resp = requests.post(
                    f"{self.connect_url}/connectors",
                    json=config,
                    headers={"Content-Type": "application/json"},
                    timeout=deploy_timeout,
                )

            if resp.status_code not in (200, 201):
                print(
                    f"Failed to deploy connector: {resp.status_code} - {resp.text}"
                )
                return False

            print(f"Connector '{connector_name}' deployed successfully")

            # ── Post-deploy task health check ────────────────────────────────
            # Poll for up to 30 s to confirm at least one task is RUNNING.
            # If a task is FAILED (typically due to stale / corrupted Kafka
            # Connect offsets left over from a previous truncated snapshot run)
            # automatically stop the connector, clear its stored offsets via
            # the Kafka Connect REST API, and resume it for a clean start.
            print(f"  Verifying connector task health (up to 30s)…")
            observed = self._wait_for_task_state(
                connector_name, {"RUNNING", "FAILED"}, timeout=30
            )
            if observed == "RUNNING":
                print(f"  ✅ Connector task is RUNNING")
                return True
            if observed == "FAILED":
                print(
                    f"  ⚠️  Task FAILED — likely stale offsets from a previous "
                    f"run.  Attempting automatic offset reset…"
                )
                if self._reset_offsets(connector_name):
                    # After reset, wait again to confirm recovery.
                    recovered = self._wait_for_task_state(
                        connector_name, {"RUNNING", "FAILED"}, timeout=30
                    )
                    if recovered == "RUNNING":
                        print(f"  ✅ Connector recovered after offset reset")
                        return True
                    print(
                        f"  ❌ Connector still FAILED after offset reset. "
                        f"Check Kafka Connect logs for details."
                    )
                    return False
                # reset_offsets failed; return True anyway — the connector
                # config is deployed even if tasks are stuck.
                print(
                    "  ⚠️  Offset reset did not succeed.  Connector config is "
                    "deployed but tasks may need manual intervention."
                )
                return True
            # Timeout — connector may still be initialising.
            print(
                f"  ⚠️  Could not confirm task state within 30s "
                f"(connector may still be starting up)"
            )
            return True

        except requests.Timeout:
            print(
                f"Timed out waiting for Kafka Connect to deploy '{connector_name}' "
                f"(read timeout={_DEPLOY_READ_TIMEOUT}s). "
                "The connector may still be deploying in the background — "
                "check its status with get_connector_status()."
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
