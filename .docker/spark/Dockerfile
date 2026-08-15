ARG BASE_IMAGE=python-py310
FROM ${BASE_IMAGE}

RUN apt-get update && \
    apt-get install -y curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3.10
ENV PYSPARK_DRIVER_PYTHON=python3.10
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/external-jars/*

COPY ./jars/deps/ /opt/spark/external-jars/

# aws-java-sdk-bundle isn't checked into jars/deps/ - see download_aws_sdk.sh.
# Needed here too: executors write Parquet to MinIO via hadoop-aws's S3A
# connector, separately from pulse-api's own copy of the same jar.
COPY .docker/download_aws_sdk.sh /tmp/
RUN chmod +x /tmp/download_aws_sdk.sh && \
    /tmp/download_aws_sdk.sh /opt/spark/external-jars && \
    rm /tmp/download_aws_sdk.sh

RUN ln -sf /usr/local/bin/python3.10 /usr/local/bin/python3

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN useradd -m -s /bin/bash spark && \
    chown -R spark:spark /opt/spark

USER spark

WORKDIR /opt/spark