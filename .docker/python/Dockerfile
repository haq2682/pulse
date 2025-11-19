FROM python:3.10.19

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt packages.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r packages.txt

# Copy bash scripts
COPY ./bash /app/bash
RUN chmod +x /app/bash/*.sh

# Download spaCy model during build (this is safe)
RUN python -m spacy download en_core_web_md

# Copy application code
COPY ./app /app

# Use entrypoint script instead of direct CMD
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]