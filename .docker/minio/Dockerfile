FROM minio/minio:latest

USER root

RUN which curl || (apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*) || \
    (apk add --no-cache curl) || true
