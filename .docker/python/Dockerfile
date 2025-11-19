FROM python:3.10.19

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt packages.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r packages.txt

# Download spaCy model during build
RUN python -m spacy download en_core_web_md

# Copy bash scripts
COPY ./bash /app/bash
RUN chmod +x /app/bash/*.sh
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]