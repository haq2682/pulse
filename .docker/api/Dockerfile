FROM python-py310

WORKDIR /app

# Copy all requirements file(s) to the container
COPY requirements.txt packages.txt ./

# Install Python dependencies
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r requirements.txt && \
    python3 -m pip install --no-cache-dir -r packages.txt

COPY ./api /app
COPY ./cleaning /app/
COPY ./mapping /app/mapping
COPY ./transformation /app/transformation

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]