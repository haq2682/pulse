FROM eclipse-temurin:17-jdk

RUN apt-get update && \
    apt-get install -y wget build-essential libssl-dev zlib1g-dev \
    libncurses5-dev libncursesw5-dev libreadline-dev libsqlite3-dev \
    libffi-dev libbz2-dev liblzma-dev libgdbm-dev uuid-dev dos2unix && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN wget https://www.python.org/ftp/python/3.10.19/Python-3.10.19.tgz && \
    tar -xvzf Python-3.10.19.tgz && \
    cd Python-3.10.19 && \
    ./configure --enable-optimizations && \
    make -j"$(nproc)" && \
    make altinstall && \
    cd / && rm -rf /tmp/Python-3.10.19 /tmp/Python-3.10.19.tgz

RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.10 1

WORKDIR /app

COPY requirements.txt packages.txt ./

RUN chmod -R 777 /app

RUN python3 -m ensurepip && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt && \
    python3 -m pip install --no-cache-dir -r packages.txt

RUN python3 -m spacy download en_core_web_md

# NLTK corpora baked in at build time (not fetched at runtime) so the
# mapping pipeline never depends on live internet access mid-request.
RUN python3 -m nltk.downloader -d /usr/local/share/nltk_data \
        punkt punkt_tab stopwords wordnet averaged_perceptron_tagger && \
    chmod -R a+rX /usr/local/share/nltk_data

COPY ./sql /app/sql
COPY ./bash /app/bash
COPY ./mapping /app/mapping
COPY ./cleaning /app/cleaning
COPY ./transformation /app/transformation
COPY ./analysis /app/analysis
COPY ./machine-learning /app/machine-learning

COPY ./jars /app/jars
COPY .docker/download_aws_sdk.sh /tmp/
RUN chmod +x /tmp/download_aws_sdk.sh && \
    /tmp/download_aws_sdk.sh /app/jars/deps && \
    rm /tmp/download_aws_sdk.sh

CMD ["tail", "-f", "/dev/null"]