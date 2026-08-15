ARG BASE_IMAGE=python-py310
FROM ${BASE_IMAGE}

WORKDIR /app

RUN adduser --disabled-password --gecos "" appuser

COPY ./api /app
COPY ./cleaning /app/cleaning
COPY ./mapping /app/mapping
COPY ./transformation /app/transformation
COPY ./analysis /app/analysis
COPY ./machine-learning /app/machine-learning
COPY ./deprecated_sql /app/schema
COPY ./jars /app/jars

# aws-java-sdk-bundle isn't vendored in jars/deps/ - fetched at build time
# instead of committed. See download_aws_sdk.sh.
COPY .docker/download_aws_sdk.sh /tmp/
RUN chmod +x /tmp/download_aws_sdk.sh && \
    /tmp/download_aws_sdk.sh /app/jars/deps && \
    rm /tmp/download_aws_sdk.sh

RUN chown -R appuser:appuser /app

# NLTK corpora baked in at build time so mapping never depends on runtime
# internet access. NLTK_DATA must be set here too, or nltk.download() picks
# a different default at runtime and silently redownloads every run.
ENV NLTK_DATA=/usr/local/share/nltk_data
COPY .docker/api/download_nltk.py .docker/api/extract_nltk_zips.py /tmp/
RUN python3 /tmp/download_nltk.py

# Separate RUN so Docker's cache hit on the slow download step above stays
# intact if this needs to change later.
RUN python3 /tmp/extract_nltk_zips.py && \
    rm /tmp/download_nltk.py /tmp/extract_nltk_zips.py && \
    chmod -R a+rX /usr/local/share/nltk_data

# Same build-time-bake reasoning as NLTK above, for the sentence-
# transformers model. HF_HOME set here so the runtime appuser looks in the
# same world-readable cache root downloaded into.
ENV HF_HOME=/usr/local/share/huggingface
COPY .docker/api/download_hf_model.py /tmp/
RUN python3 /tmp/download_hf_model.py && \
    rm /tmp/download_hf_model.py && \
    chmod -R a+rX /usr/local/share/huggingface

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health', timeout=3).status == 200 else 1)" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]