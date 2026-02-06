FROM apache/spark:3.5.0

USER root

# Install Python 3.10 dependencies
RUN apt-get update && \
    apt-get install -y wget build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev curl llvm libncurses5-dev \
    libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev git && \
    wget https://www.python.org/ftp/python/3.10.19/Python-3.10.19.tgz && \
    tar xvf Python-3.10.19.tgz && \
    cd Python-3.10.19 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && rm -rf Python-3.10.19 Python-3.10.19.tgz

RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    -P /opt/spark/jars/

# Ensure python3 points to python3.10
RUN ln -sf /usr/local/bin/python3.10 /usr/local/bin/python3

# Install pip
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

ENV PYSPARK_PYTHON=python3.10
ENV PYSPARK_DRIVER_PYTHON=python3.10

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER spark
