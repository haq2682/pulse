FROM apache/spark:latest

USER root

RUN apt-get update && apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER spark