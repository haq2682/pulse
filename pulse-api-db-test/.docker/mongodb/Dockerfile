FROM mongo:7

USER root

RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install pymongo && \
    mkdir -p /data/import-files

COPY ./files /data/import-files
COPY ./scripts/ingest.py /docker-entrypoint-initdb.d/ingest.py
COPY ./scripts/entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh