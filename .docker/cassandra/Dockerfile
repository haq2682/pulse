# =============================================================================
# Dockerfile.cassandra
# =============================================================================
# Extends the official Cassandra 4.1 image with CDC enabled in cassandra.yaml.
#
# CDC (Change Data Capture) is required for the Debezium Cassandra connector
# to read commit log files and stream change events into Kafka.
#
# What this Dockerfile does:
#   - Enables cdc_enabled: true  in /etc/cassandra/cassandra.yaml
#
# The debezium-cassandra service (quay.io/debezium/server:3.4) shares the
# /var/lib/cassandra/commitlog volume with this container and reads commit
# log segments directly from it.
#
# ⚠️  CDC must also be enabled per table after the container starts:
#       ALTER TABLE keyspace_name.orders   WITH cdc = true;
#       ALTER TABLE keyspace_name.payments WITH cdc = true;
# =============================================================================

FROM cassandra:4.1

RUN sed -i 's/^#\?\s*cdc_enabled:.*/cdc_enabled: true/' /etc/cassandra/cassandra.yaml
