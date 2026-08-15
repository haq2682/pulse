# Enables cdc_enabled: true, required for the Debezium Cassandra connector
# to read commit log files. CDC must also be enabled per table after the
# container starts: ALTER TABLE keyspace.table WITH cdc = true;
FROM cassandra:4.1

RUN sed -i 's/^#\?\s*cdc_enabled:.*/cdc_enabled: true/' /etc/cassandra/cassandra.yaml
