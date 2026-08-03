# Start from Eclipse Temurin 17 (OpenJDK 17)
FROM eclipse-temurin:17-jdk

# Install build dependencies
RUN apt-get update && \
    apt-get install -y wget build-essential libssl-dev zlib1g-dev \
    libncurses5-dev libncursesw5-dev libreadline-dev libsqlite3-dev \
    libffi-dev libbz2-dev liblzma-dev libgdbm-dev uuid-dev dos2unix && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install exact Python 3.10.19
WORKDIR /tmp
RUN wget https://www.python.org/ftp/python/3.10.19/Python-3.10.19.tgz && \
    tar -xvzf Python-3.10.19.tgz && \
    cd Python-3.10.19 && \
    ./configure --enable-optimizations && \
    make -j"$(nproc)" && \
    make altinstall && \
    cd / && rm -rf /tmp/Python-3.10.19 /tmp/Python-3.10.19.tgz

# Set python3 to exact version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.10 1

WORKDIR /app

# Copy requirements and install
COPY requirements.txt packages.txt ./

# Ensure /app is fully writable by root
RUN chmod -R 777 /app

# Use python3 -m pip instead of pip3 and bootstrap pip explicitly
RUN python3 -m ensurepip && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt && \
    python3 -m pip install --no-cache-dir -r packages.txt

# Download spaCy model
RUN python3 -m spacy download en_core_web_md

# Download NLTK corpora used by mapping/algorithms/nltk_mapping.py at
# import time (nltk.download("punkt"/"stopwords"/"wordnet"/etc). Baked in
# here at build time, same reasoning as the spaCy model above, rather than
# fetched at runtime on first use - a runtime download means every mapping
# pipeline run depends on live internet access from inside the request-
# serving pulse-api container, and is a real, reproducible point of
# failure (verified live: a mapping run hung indefinitely mid-download).
# /usr/local/share/nltk_data is one of NLTK's built-in default search
# paths, so this is found automatically with no NLTK_DATA env var needed -
# world-readable so the non-root `appuser` in .docker/api/Dockerfile's
# image (built FROM this one) can read it too, even though it didn't
# download it.
RUN python3 -m nltk.downloader -d /usr/local/share/nltk_data \
        punkt punkt_tab stopwords wordnet averaged_perceptron_tagger && \
    chmod -R a+rX /usr/local/share/nltk_data

# Copy scripts
COPY ./sql /app/sql
COPY ./bash /app/bash
COPY ./mapping /app/mapping
COPY ./cleaning /app/cleaning
COPY ./transformation /app/transformation
COPY ./analysis /app/analysis
COPY ./machine-learning /app/machine-learning

# Pre-downloaded JARs for Spark's Delta Lake + S3A support (hadoop-aws,
# delta-spark, delta-storage), plus the build-time-fetched aws-java-sdk-bundle
# (see download_aws_sdk.sh - not committed to the repo, ~280MB). This image is
# used directly (not further built) as the KubernetesPodOperator task-pod
# image for cleaning/transformation/analysis/machine-learning - see
# airflow/config/pipeline_config.py's PYTHON_IMAGE - so these Spark sessions
# need the same jars/deps/ classpath mapping/map.py already gets from
# .docker/api/Dockerfile.
COPY ./jars /app/jars
COPY .docker/download_aws_sdk.sh /tmp/
RUN chmod +x /tmp/download_aws_sdk.sh && \
    /tmp/download_aws_sdk.sh /app/jars/deps && \
    rm /tmp/download_aws_sdk.sh

CMD ["tail", "-f", "/dev/null"]