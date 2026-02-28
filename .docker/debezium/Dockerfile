# =============================================================================
# Dockerfile.debezium
# =============================================================================
# Extends the official Debezium 3.4 Kafka Connect image with ALL connectors
# needed by the mapping pipeline's true CDC db mode.
#
# ── Pre-installed in quay.io/debezium/connect:3.4 (nothing to do) ───────────
#   ✅ PostgreSQL   (debezium-connector-postgres)
#   ✅ MySQL        (debezium-connector-mysql)
#   ✅ MongoDB      (debezium-connector-mongodb)
#   ✅ SQL Server   (debezium-connector-sqlserver)
#   ✅ Oracle       (debezium-connector-oracle)  ← JARs only, JDBC driver needed separately
#   ✅ Db2          (debezium-connector-db2)
#
# ── Added by this Dockerfile ─────────────────────────────────────────────────
#   ✅ MariaDB      — standalone connector available since 2.7, fully mature in 3.x
#   ✅ Vitess       — community connector, not in base image
#   ✅ Spanner      — community connector, not in base image
#   ✅ Informix     — community connector, not in base image
#   ✅ Scripting    — event filtering/routing extension
#
# ── Cannot be added here ─────────────────────────────────────────────────────
#   ⚠️  Cassandra  — requires Debezium Server (separate container), see bottom
# =============================================================================

FROM quay.io/debezium/connect:3.4

ENV DEBEZIUM_VERSION=3.4.0.Final

USER root

# NOTE: Docker's COPY instruction does not expand ENV/ARG variables in source
# paths, so the filenames below are hardcoded. They must match the archives
# present in ./jars/ and must correspond to DEBEZIUM_VERSION above.

# =============================================================================
# SECTION 1 — MariaDB Connector
# -----------------------------------------------------------------------------
# In Debezium 3.4, MariaDB has its own dedicated connector (available since
# 2.7+). This replaces the old workaround of using the MySQL connector for
# MariaDB. The standalone connector correctly handles MariaDB-specific
# features, GTID format, and protocol differences.
#
# Use this in your connector config for MariaDB:
#   "connector.class": "io.debezium.connector.mariadb.MariaDbConnector"
#   "database.hostname": "<your-mariadb-host>"
# =============================================================================
COPY jars/debezium-connector-mariadb-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-mariadb-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-mariadb-3.4.0.Final-plugin.tar.gz

# =============================================================================
# SECTION 2 — Vitess Connector
# -----------------------------------------------------------------------------
# Connects to Vitess VTGate via the VStream gRPC API (port 15991, not 3306).
# VStream must be enabled on the source Vitess cluster.
# =============================================================================
COPY jars/debezium-connector-vitess-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-vitess-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-vitess-3.4.0.Final-plugin.tar.gz

# =============================================================================
# SECTION 3 — Google Spanner Connector
# -----------------------------------------------------------------------------
# Uses Spanner Change Streams. Requires a change stream created on the DB:
#   CREATE CHANGE STREAM pulse_change_stream FOR ALL;
#
# GCP credentials must be mounted at runtime in docker-compose:
#   volumes:
#     - ./gcp-credentials.json:/etc/gcp/credentials.json
#   environment:
#     - GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/credentials.json
# =============================================================================
COPY jars/debezium-connector-spanner-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-spanner-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-spanner-3.4.0.Final-plugin.tar.gz

# =============================================================================
# SECTION 4 — IBM Informix Connector
# -----------------------------------------------------------------------------
# New in Debezium 2.5. Uses Informix CDC (syscdcv1).
# Requires Informix 12.10+ with the CDC option enabled on the server.
# Needs a schema history Kafka topic (like MySQL/SQL Server).
# =============================================================================
COPY jars/debezium-connector-informix-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-informix-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-informix-3.4.0.Final-plugin.tar.gz

# =============================================================================
# SECTION 5 — Debezium Scripting Extension
# -----------------------------------------------------------------------------
# Enables Groovy/JS-based filtering and routing transforms in connector config.
# In Debezium 3.x, placed in its own plugin directory so it is available to
# all connectors on the plugin path.
# Example use:
#   transforms=filter
#   transforms.filter.type=io.debezium.transforms.Filter
#   transforms.filter.language=jsr223.groovy
#   transforms.filter.condition=value.op == 'u'
# =============================================================================
COPY jars/debezium-scripting-3.4.0.Final.tar.gz /tmp/
RUN mkdir -p /kafka/connect/debezium-scripting && \
    tar -xzf /tmp/debezium-scripting-3.4.0.Final.tar.gz -C /kafka/connect/debezium-scripting/ && \
    rm /tmp/debezium-scripting-3.4.0.Final.tar.gz

# =============================================================================
# SECTION 6 — Oracle JDBC Driver (cannot be bundled — license restricted)
# -----------------------------------------------------------------------------
# The Oracle connector JARs are pre-installed in the base image, but ojdbc8.jar
# cannot be redistributed. It is mounted at runtime by docker-compose.yml:
#   volumes:
#     - ./jars/ojdbc8.jar:/kafka/connect/debezium-connector-oracle/ojdbc8.jar
#
# The Oracle connector will fail to start without this file.
# =============================================================================

USER 1001

# =============================================================================
# CASSANDRA — Requires a separate Debezium Server container
# =============================================================================
# The Cassandra connector reads CommitLog files directly from disk.
# It must run as quay.io/debezium/server:3.4 sharing a volume with Cassandra.
#
# Add to docker-compose.yml:
#
#   cassandra:
#     image: cassandra:4.1
#     volumes:
#       - cassandra_commitlog:/var/lib/cassandra/commitlog
#
#   debezium-cassandra:
#     image: quay.io/debezium/server:3.4
#     container_name: debezium-cassandra
#     volumes:
#       - cassandra_commitlog:/cassandra/commitlog
#       - ./conf/debezium-cassandra.properties:/debezium/conf/application.properties
#     networks:
#       spark-network:
#         ipv4_address: 10.5.0.56
#     restart: always
#
#   volumes:
#     cassandra_commitlog:
#
# Enable CDC per table on Cassandra side (CQL, not system-level):
#   ALTER TABLE keyspace.your_table WITH cdc = true;
#
# debezium-cassandra.properties:
#   debezium.source.connector.class=io.debezium.connector.cassandra.CassandraConnector
#   debezium.source.cassandra.hosts=<cassandra-ip>
#   debezium.source.cassandra.port=9042
#   debezium.source.cassandra.user=cassandra
#   debezium.source.cassandra.password=cassandra
#   debezium.source.cassandra.keyspace=your_keyspace
#   debezium.source.commitlog.dir=/cassandra/commitlog
#   debezium.source.topic.prefix=cassandra
#   debezium.sink.type=kafka
#   debezium.sink.kafka.producer.bootstrap.servers=10.5.0.7:9092
# =============================================================================
