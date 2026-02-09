"""
Debezium Connector Manager: Deploy and manage CDC connectors via Kafka Connect REST API.

This module provides functions to:
- Create connector configurations for PostgreSQL and MySQL
- Deploy connectors to Kafka Connect
- Monitor connector status
- Wait for Kafka Connect availability

Usage:
    from streaming.ingestion.debezium_connector_manager import DebeziumConnectorManager

    manager = DebeziumConnectorManager()
    config = manager.create_postgres_config(
        connector_name="pulse-cdc",
        db_host="localhost",
        db_port=5432,
        db_name="ecommerce",
        db_user="debezium_user",
        db_password="debezium_pass",
        tables=["orders", "payments", "inventory"]
    )
    manager.deploy_connector(config)
"""

import time
import json
import requests
from typing import List, Dict, Any, Optional


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

    def create_postgres_config(
        self,
        connector_name: str,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        tables: List[str],
        slot_name: str = "debezium_slot",
        topic_prefix: str = "ecom",
    ) -> Dict[str, Any]:
        """
        Create PostgreSQL Debezium connector configuration.

        Args:
            connector_name: Unique connector name
            db_host: Database host
            db_port: Database port
            db_name: Database name
            db_user: Database user with replication privileges
            db_password: Database password
            tables: List of table names to capture
            slot_name: PostgreSQL replication slot name
            topic_prefix: Kafka topic prefix

        Returns:
            Connector configuration dictionary
        """
        table_include = ",".join(f"public.{t}" for t in tables)

        return {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": db_host,
                "database.port": str(db_port),
                "database.user": db_user,
                "database.password": db_password,
                "database.dbname": db_name,
                "database.server.name": db_name,
                "topic.prefix": topic_prefix,
                "table.include.list": table_include,
                "slot.name": slot_name,
                "plugin.name": "pgoutput",
                "publication.autocreate.mode": "filtered",
                "snapshot.mode": "initial",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "tombstones.on.delete": "false",
                "decimal.handling.mode": "string",
                "time.precision.mode": "connect",
            },
        }

    def create_mysql_config(
        self,
        connector_name: str,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        tables: List[str],
        server_id: int = 184054,
        topic_prefix: str = "ecom",
    ) -> Dict[str, Any]:
        """
        Create MySQL Debezium connector configuration.

        Args:
            connector_name: Unique connector name
            db_host: Database host
            db_port: Database port
            db_name: Database name
            db_user: Database user with replication privileges
            db_password: Database password
            tables: List of table names to capture
            server_id: MySQL server ID for replication
            topic_prefix: Kafka topic prefix

        Returns:
            Connector configuration dictionary
        """
        table_include = ",".join(f"{db_name}.{t}" for t in tables)

        return {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": db_host,
                "database.port": str(db_port),
                "database.user": db_user,
                "database.password": db_password,
                "database.server.id": str(server_id),
                "database.server.name": db_name,
                "topic.prefix": topic_prefix,
                "table.include.list": table_include,
                "database.include.list": db_name,
                "schema.history.internal.kafka.bootstrap.servers": "10.5.0.7:9092",
                "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
                "snapshot.mode": "initial",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "tombstones.on.delete": "false",
                "decimal.handling.mode": "string",
                "time.precision.mode": "connect",
            },
        }

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
