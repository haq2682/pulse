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

# Set python3 to exact version (no pip3 alternative)
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.10 1

WORKDIR /app

# Copy requirements and install
COPY requirements.txt packages.txt ./

# Use python3 -m pip instead of pip3 and bootstrap pip explicitly
RUN python3 -m ensurepip && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt && \
    python3 -m pip install --no-cache-dir -r packages.txt

# Download spaCy model
RUN python3 -m spacy download en_core_web_md

# Copy scripts
COPY ./bash /app/bash
COPY ./mapping /app/mapping
COPY ./cleaning /app/cleaning
COPY ./transformation /app/transformation

CMD ["tail", "-f", "/dev/null"]