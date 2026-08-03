ARG BASE_IMAGE=python-py310
FROM ${BASE_IMAGE}

WORKDIR /app

# Create non-root user before copying application files
RUN adduser --disabled-password --gecos "" appuser

COPY ./api /app
COPY ./cleaning /app/cleaning
COPY ./mapping /app/mapping
COPY ./transformation /app/transformation
COPY ./analysis /app/analysis
COPY ./machine-learning /app/machine-learning
COPY ./deprecated_sql /app/schema
COPY ./jars /app/jars

# hadoop-aws, delta-spark, and delta-storage already live in jars/deps/,
# but aws-java-sdk-bundle (needed by hadoop-aws's S3A connector at write
# time - verified live via NoClassDefFoundError without it) was never
# actually vendored there. Fetched at build time instead of committed to
# the repo - see download_aws_sdk.sh for why. Shared with .docker/spark/
# Dockerfile, which needs this same jar at a different destination path.
COPY .docker/download_aws_sdk.sh /tmp/
RUN chmod +x /tmp/download_aws_sdk.sh && \
    /tmp/download_aws_sdk.sh /app/jars/deps && \
    rm /tmp/download_aws_sdk.sh

# Change ownership while still root
RUN chown -R appuser:appuser /app

# mapping/algorithms/nltk_mapping.py downloads these corpora at import time
# (nltk.download(...)) - baked in here instead so mapping/map.py's Spark
# session (spawned as a subprocess of this same container by onboarding.py's
# /start-mapping) never depends on live internet access at request time.
# Verified live: a runtime download hung/crawled indefinitely under this
# environment's network conditions, blocking the entire mapping pipeline.
# /usr/local/share/nltk_data is one of NLTK's built-in default search paths
# (no NLTK_DATA env var needed), and world-readable so the non-root
# appuser (already switched to below) can read what root downloaded here.
# See download_nltk.py for why this isn't the interactive downloader CLI.
# NLTK_DATA also has to be set here (not just at download time) - verified
# live that mapping/algorithms/nltk_mapping.py's own nltk.download() calls
# (no explicit download_dir, unlike download_nltk.py's build-time prefetch)
# otherwise pick a different default and silently redownload every run.
ENV NLTK_DATA=/usr/local/share/nltk_data
COPY .docker/api/download_nltk.py .docker/api/extract_nltk_zips.py /tmp/
RUN python3 /tmp/download_nltk.py

# Separate RUN instruction on purpose - keeps Docker's cache hit on the
# (slow) download step above intact even if this safety net needs to
# change later. See extract_nltk_zips.py for why this exists.
RUN python3 /tmp/extract_nltk_zips.py && \
    rm /tmp/download_nltk.py /tmp/extract_nltk_zips.py && \
    chmod -R a+rX /usr/local/share/nltk_data

# Same reasoning as the NLTK corpora above, for the sentence-transformers
# model mapping/algorithms/roberta_mapping.py loads at import time. HF_HOME
# has to be set here (image-wide, before the download) so the *runtime*
# process - running as the non-root appuser below - looks in the same
# world-readable cache location root downloaded into, instead of defaulting
# to a per-user cache dir it would then try to redownload into.
ENV HF_HOME=/usr/local/share/huggingface
COPY .docker/api/download_hf_model.py /tmp/
RUN python3 /tmp/download_hf_model.py && \
    rm /tmp/download_hf_model.py && \
    chmod -R a+rX /usr/local/share/huggingface

# Switch to non-root user
USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health', timeout=3).status == 200 else 1)" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]