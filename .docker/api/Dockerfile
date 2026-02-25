FROM python-py310

WORKDIR /app

# Copy all requirements file(s) to the container
COPY requirements.txt packages.txt ./

# Install Python dependencies
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt && \
    python3 -m pip install --no-cache-dir -r packages.txt

COPY ./api /app
COPY ./cleaning /app/cleaning
COPY ./mapping /app/mapping
COPY ./transformation /app/transformation
COPY ./analysis /app/analysis
COPY ./machine-learning /app/machine-learning

# Run as non-root user for security
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]