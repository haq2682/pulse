# Extends quay.io/debezium/connect:3.4 (which pre-installs Postgres/MySQL/
# MongoDB/SQL Server/Oracle/Db2) with MariaDB, Vitess, Spanner, and Informix
# connectors, plus the scripting extension. Cassandra is not added here -
# it requires a separate Debezium Server container reading commit logs
# directly, not a Kafka Connect connector.
FROM quay.io/debezium/connect:3.4

ENV DEBEZIUM_VERSION=3.4.0.Final

USER root

# COPY does not expand ENV/ARG in source paths, so filenames below are
# hardcoded - must match DEBEZIUM_VERSION above.
COPY jars/debezium-connector-mariadb-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-mariadb-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-mariadb-3.4.0.Final-plugin.tar.gz

COPY jars/debezium-connector-vitess-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-vitess-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-vitess-3.4.0.Final-plugin.tar.gz

COPY jars/debezium-connector-spanner-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-spanner-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-spanner-3.4.0.Final-plugin.tar.gz

COPY jars/debezium-connector-informix-3.4.0.Final-plugin.tar.gz /tmp/
RUN tar -xzf /tmp/debezium-connector-informix-3.4.0.Final-plugin.tar.gz -C /kafka/connect/ && \
    rm /tmp/debezium-connector-informix-3.4.0.Final-plugin.tar.gz

COPY jars/debezium-scripting-3.4.0.Final.tar.gz /tmp/
RUN mkdir -p /kafka/connect/debezium-scripting && \
    tar -xzf /tmp/debezium-scripting-3.4.0.Final.tar.gz -C /kafka/connect/debezium-scripting/ && \
    rm /tmp/debezium-scripting-3.4.0.Final.tar.gz

# Oracle connector JARs are pre-installed, but ojdbc8.jar is license-
# restricted and can't be bundled - mounted at runtime instead.

USER 1001
