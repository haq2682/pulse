#!/bin/sh
# Build-time download of aws-java-sdk-bundle-1.12.262.jar, needed by both
# pulse-api (mapping/map.py's Spark driver, which runs as a subprocess of
# that container) and pulse-spark (the actual worker/executor classpath at
# /opt/spark/external-jars/) - Hadoop's S3A connector (used by hadoop-aws,
# already present in both images' jars/deps copies) needs it to write
# Parquet output to MinIO. Never actually vendored anywhere in this repo -
# verified live in both images: writes failed with NoClassDefFoundError
# for com.amazonaws.auth.AWSCredentialsProvider without it.
#
# Not committed to the repo directly (~280MB, an already-compressed binary
# git would have to carry forever) - fetched here at build time instead,
# same reasoning as download_nltk.py/download_hf_model.py in .docker/api/.
# Maven Central is a CDN-backed artifact registry (same reliability tier as
# PyPI/npm), not a flaky mirror, but this is still a large file, so this
# retries with curl's own resume support (-C -) rather than restarting from
# zero on every transient failure, and verifies the download against Maven
# Central's published SHA1 checksum rather than trusting a completed-
# looking file (verified live earlier tonight, for a different dependency,
# that a corrupted-but-complete-sized download is a real risk on this
# network).
#
# Usage: download_aws_sdk.sh <destination-directory>
set -eu

DEST_DIR="${1:?usage: download_aws_sdk.sh <destination-directory>}"
DEST="${DEST_DIR}/aws-java-sdk-bundle-1.12.262.jar"
URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
SHA1_URL="${URL}.sha1"
MAX_ATTEMPTS=6

mkdir -p "$DEST_DIR"

expected_sha1=$(curl -fsSL "$SHA1_URL" | tr -d ' \n')
echo "expected sha1: $expected_sha1"

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
    echo "[attempt $attempt] downloading $URL -> $DEST"
    curl -fSL --retry 3 --retry-delay 5 --retry-connrefused -C - -o "$DEST" "$URL" || true

    if [ -f "$DEST" ]; then
        actual_sha1=$(sha1sum "$DEST" | awk '{print $1}')
        if [ "$actual_sha1" = "$expected_sha1" ]; then
            echo "checksum OK ($actual_sha1)"
            exit 0
        fi
        echo "[attempt $attempt] checksum mismatch: got $actual_sha1, expected $expected_sha1 - retrying from scratch"
        rm -f "$DEST"
    fi

    attempt=$((attempt + 1))
    sleep 5
done

echo "giving up after $MAX_ATTEMPTS attempts"
exit 1
