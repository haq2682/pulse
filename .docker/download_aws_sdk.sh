#!/bin/sh
# Build-time download of aws-java-sdk-bundle, needed by pulse-api and
# pulse-spark's hadoop-aws S3A connector to write Parquet to MinIO. Not
# committed to the repo (~280MB) - fetched here instead, with checksum
# verification and resumable retries.
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
