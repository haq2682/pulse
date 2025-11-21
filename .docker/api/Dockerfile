FROM python-py310

WORKDIR /app
COPY ./api /app
COPY ./cleaning /app/
COPY ./mapping /app/mapping
COPY ./transformation /app/transformation

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]