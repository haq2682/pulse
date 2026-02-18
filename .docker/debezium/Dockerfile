# =============================================================================
# Dockerfile.debezium
# =============================================================================
# Extends the official Debezium 2.5 Kafka Connect image with ALL connectors
# needed by the mapping pipeline's true CDC db mode.
#
# ── Pre-installed in quay.io/debezium/connect:2.5 (nothing to do) ───────────
#   ✅ PostgreSQL   (debezium-connector-postgres)
#   ✅ MySQL        (debezium-connector-mysql)
#   ✅ MongoDB      (debezium-connector-mongodb)
#   ✅ SQL Server   (debezium-connector-sqlserver)
#   ✅ Oracle       (debezium-connector-oracle)  ← JARs only, JDBC driver needed separately
#   ✅ Db2          (debezium-connector-db2)
#
# ── Added by this Dockerfile ─────────────────────────────────────────────────
#   ✅ Vitess       — community connector, not in base image
#   ✅ Spanner      — community connector, not in base image
#   ✅ Informix     — new community connector in 2.5, not in base image
#   ✅ Scripting    — event filtering/routing extension
#
# ── MariaDB situation ────────────────────────────────────────────────────────
#   ⚠️  The standalone debezium-connector-mariadb artifact did NOT exist
#       until version 2.7.0. It is NOT available on Maven Central for 2.5.x.
#       In 2.5, MariaDB works via the pre-installed MySQL connector using:
#         connector.class = io.debezium.connector.mysql.MySqlConnector
#       No extra installation needed — see SECTION 1 note below.
#
# ── Cannot be added here ─────────────────────────────────────────────────────
#   ⚠️  Cassandra  — requires Debezium Server (separate container), see bottom
# =============================================================================

FROM quay.io/debezium/connect:2.5

ENV DEBEZIUM_VERSION=2.5.4.Final

USER root

# =============================================================================
# SECTION 1 — MariaDB (no installation required in 2.5)
# -----------------------------------------------------------------------------
# In Debezium 2.5, MariaDB support is bundled inside the MySQL connector as
# a preview feature. The MySQL connector is already in the base image.
#
# Use this in your connector config for MariaDB:
#   "connector.class": "io.debezium.connector.mysql.MySqlConnector"
#   "database.hostname": "<your-mariadb-host>"
#
# If you later upgrade to Debezium 2.7+, switch the base image to:
#   FROM quay.io/debezium/connect:2.7
#   ENV DEBEZIUM_VERSION=2.7.4.Final
# And uncomment:
#   RUN curl -fSL \
#       https://repo1.maven.org/maven2/io/debezium/debezium-connector-mariadb/${DEBEZIUM_VERSION}/debezium-connector-mariadb-${DEBEZIUM_VERSION}-plugin.tar.gz \
#       | tar -xzv -C /kafka/connect/
# =============================================================================

# =============================================================================
# SECTION 2 — Vitess Connector
# -----------------------------------------------------------------------------
# Connects to Vitess VTGate via the VStream gRPC API (port 15991, not 3306).
# VStream must be enabled on the source Vitess cluster.
# =============================================================================
RUN curl -fSL \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-vitess/${DEBEZIUM_VERSION}/debezium-connector-vitess-${DEBEZIUM_VERSION}-plugin.tar.gz \
    | tar -xzv -C /kafka/connect/

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
RUN curl -fSL \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-spanner/${DEBEZIUM_VERSION}/debezium-connector-spanner-${DEBEZIUM_VERSION}-plugin.tar.gz \
    | tar -xzv -C /kafka/connect/

# =============================================================================
# SECTION 4 — IBM Informix Connector
# -----------------------------------------------------------------------------
# New in Debezium 2.5. Uses Informix CDC (syscdcv1).
# Requires Informix 12.10+ with the CDC option enabled on the server.
# Needs a schema history Kafka topic (like MySQL/SQL Server).
# =============================================================================
RUN curl -fSL \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-informix/${DEBEZIUM_VERSION}/debezium-connector-informix-${DEBEZIUM_VERSION}-plugin.tar.gz \
    | tar -xzv -C /kafka/connect/

# =============================================================================
# SECTION 5 — Debezium Scripting Extension
# -----------------------------------------------------------------------------
# Enables Groovy/JS-based filtering and routing transforms in connector config.
# Placed inside the oracle connector dir which is already on the plugin path.
# Example use:
#   transforms=filter
#   transforms.filter.type=io.debezium.transforms.Filter
#   transforms.filter.language=jsr223.groovy
#   transforms.filter.condition=value.op == 'u'
# =============================================================================
RUN curl -fSL \
    https://repo1.maven.org/maven2/io/debezium/debezium-scripting/${DEBEZIUM_VERSION}/debezium-scripting-${DEBEZIUM_VERSION}.tar.gz \
    | tar -xzv -C /kafka/connect/debezium-connector-oracle/

# =============================================================================
# SECTION 6 — Oracle JDBC Driver (cannot be bundled — license restricted)
# -----------------------------------------------------------------------------
# The Oracle connector JARs are pre-installed in the base image, but ojdbc8.jar
# cannot be redistributed. You must provide it manually:
#
#   1. Download ojdbc8.jar from:
#      https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
#
#   2. Place it at ./oracle-jdbc/ojdbc8.jar next to your docker-compose.yml
#
#   3. Add to the debezium service in docker-compose.yml:
#      volumes:
#        - ./oracle-jdbc/ojdbc8.jar:/kafka/connect/debezium-connector-oracle/ojdbc8.jar
#
# The Oracle connector will fail to start without this file.
# =============================================================================

USER 1001

# =============================================================================
# CASSANDRA — Requires a separate Debezium Server container
# =============================================================================
# The Cassandra connector reads CommitLog files directly from disk.
# It must run as quay.io/debezium/server:2.5 sharing a volume with Cassandra.
#
# Add to docker-compose.yml:
#
#   cassandra:
#     image: cassandra:4.1
#     volumes:
#       - cassandra_commitlog:/var/lib/cassandra/commitlog
#
#   debezium-cassandra:
#     image: quay.io/debezium/server:2.5
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
