ARG BASE_IMAGE=python-py310
FROM ${BASE_IMAGE}

# Install Spark runtime dependencies
RUN apt-get update && \
    apt-get install -y curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark 3.5.0
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3.10
ENV PYSPARK_DRIVER_PYTHON=python3.10
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/external-jars/*

# Copy external JARs (kafka connector + transitive deps)
COPY ./jars/deps/ /opt/spark/external-jars/

# Ensure python3.10 symlink is intact (inherited but explicit for safety)
RUN ln -sf /usr/local/bin/python3.10 /usr/local/bin/python3

# pip already installed from python-py310 base, install pyspark requirements if any
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Create spark user to match apache/spark:3.5.0 upstream behavior
RUN useradd -m -s /bin/bash spark && \
    chown -R spark:spark /opt/spark

USER spark

WORKDIR /opt/spark