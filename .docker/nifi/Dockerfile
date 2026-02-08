FROM apache/nifi:latest

USER root

# Download Parquet, Hadoop, and MinIO NARs from Maven Central
RUN curl -L -o /opt/nifi/nifi-current/lib/nifi-parquet-nar-2.7.2.nar \
      https://repo1.maven.org/maven2/org/apache/nifi/nifi-parquet-nar/2.7.2/nifi-parquet-nar-2.7.2.nar \
  && curl -L -o /opt/nifi/nifi-current/lib/nifi-hadoop-libraries-nar-2.7.2.nar \
      https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-libraries-nar/2.7.2/nifi-hadoop-libraries-nar-2.7.2.nar \
  && curl -L -o /opt/nifi/nifi-current/lib/nifi-minio-nar-2.7.2.nar \
      https://repo1.maven.org/maven2/org/apache/nifi/nifi-minio-nar/2.7.2/nifi-minio-nar-2.7.2.nar

# Download PostgreSQL JDBC driver
RUN curl -L -o /opt/nifi/nifi-current/lib/postgresql-42.6.0.jar \
      https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Clean up any potential work/nar cache that might be stale
RUN rm -rf /opt/nifi/nifi-current/work/nar

# Set proper ownership
RUN chown -R nifi:nifi /opt/nifi/nifi-current/lib/

USER nifi