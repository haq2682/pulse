"""
Initial bulk ingest for db and api modes.

Replaces the Debezium trigger-once → Kafka → Spark Streaming path for the
FIRST-TIME onboarding snapshot.  Data is written straight to the MinIO
``ingested/`` folder in Parquet format (one or more chunked part files per
canonical table) so that the existing batch-mode mapping pipeline
(run_batch_mode) can process it immediately without any Kafka or Spark
Streaming involvement.

Subsequent incremental changes after the user confirms their mappings are
handled by the continuous Debezium CDC / API-polling Airflow DAG, which
re-reads the confirmed manual mappings from Redis on every micro-batch and
applies them automatically — no further user action needed.

Two public entry points
-----------------------
    run_jdbc_initial_load(db_uri, tables, bucket_name, chunk_size=200_000)
        Reads the full snapshot of every requested table from the source
        database using pandas / SQLAlchemy (SQL) or PyMongo (MongoDB) and
        writes chunked Parquet files to  ingested/{canonical_table}/chunk_NNNN.parquet.

    run_api_initial_load(api_url, poll_duration, bucket_name, poll_interval=10)
        Polls the user's external API for *poll_duration* seconds, accumulates
        records per canonical table, and writes them to
        ingested/{canonical_table}/chunk_NNNN.parquet.
"""

from __future__ import annotations

import json
import os
import sys
import time
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
from minio import Minio
from dotenv import load_dotenv, find_dotenv

# ---------------------------------------------------------------------------
# Make sure the mapping root is on sys.path so peer modules are importable.
# ---------------------------------------------------------------------------
_MAPPING_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _MAPPING_DIR not in sys.path:
    sys.path.insert(0, _MAPPING_DIR)

from utils.helpers import parse_minio_endpoint  # noqa: E402

load_dotenv(find_dotenv())

# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

def _make_minio_client() -> Minio:
    endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    return Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def _ensure_bucket(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"  Created bucket: {bucket_name}", flush=True)


def _write_parquet(client: Minio, bucket_name: str, object_name: str, df: pd.DataFrame) -> None:
    """Serialise *df* to Parquet and upload to MinIO as *object_name*."""
    # Stringify datetime-like values so pyarrow can infer a consistent schema
    # across chunks — mixed tz-aware/tz-naive timestamps would otherwise cause
    # schema merge failures when the batch mapping reads all chunks later.
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]", "object"]).columns:
        df[col] = df[col].apply(
            lambda v: v.isoformat() if hasattr(v, "isoformat") else v
        )
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    data = buf.getvalue()
    client.put_object(
        bucket_name,
        object_name,
        BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )


# ---------------------------------------------------------------------------
# Canonical table name resolver (reuse the same fuzzy map already in place)
# ---------------------------------------------------------------------------

def _resolve_canonical(raw_name: str) -> Optional[str]:
    """Return the canonical schema table name for *raw_name*, or None."""
    try:
        from streaming.ingestion.db_ingest_service import map_to_canonical_table
        return map_to_canonical_table(raw_name)
    except Exception:
        pass
    # Inline fallback so the function always works even if the import fails.
    from rapidfuzz import fuzz, process as _process
    VALID_TABLES = [
        "addresses", "customers", "suppliers", "categories", "products",
        "inventory", "wishlist", "shopping_cart", "cart_items", "orders",
        "order_items", "payments", "reviews", "marketing_campaigns",
        "customer_sessions",
    ]
    EXACT_MAP = {
        "customer": "customers", "users": "customers", "user": "customers",
        "address": "addresses",
        "product": "products", "item": "products", "items": "products",
        "inventories": "inventory", "stock": "inventory",
        "order": "orders",
        "review": "reviews", "ratings": "reviews",
        "category": "categories",
        "wishlists": "wishlist",
        "payment": "payments", "transactions": "payments",
        "orderitems": "order_items", "order_details": "order_items",
        "shopping_carts": "shopping_cart", "cart": "shopping_cart", "carts": "shopping_cart",
        "cart_item": "cart_items", "cartitems": "cart_items",
        "shopping_cart_items": "cart_items",
        "sessions": "customer_sessions",
        "campaigns": "marketing_campaigns",
        "supplier": "suppliers", "vendors": "suppliers",
    }
    lower = raw_name.lower().strip()
    if lower in VALID_TABLES:
        return lower
    if lower in EXACT_MAP:
        return EXACT_MAP[lower]
    match = _process.extractOne(lower, VALID_TABLES, scorer=fuzz.ratio, score_cutoff=85)
    return match[0] if match else None


# ---------------------------------------------------------------------------
# SQLAlchemy URI mapping
# ---------------------------------------------------------------------------

def _sqlalchemy_url(db_uri: str) -> Optional[str]:
    """
    Convert a generic database URI to a SQLAlchemy connection string.
    Returns None for databases that are not supported via SQLAlchemy
    (MongoDB, Spanner, Cassandra) so callers can use the native driver instead.
    """
    scheme = urlparse(db_uri).scheme.lower()
    DRIVER_MAP: Dict[str, Optional[str]] = {
        "postgresql":   "postgresql+psycopg2",
        "postgres":     "postgresql+psycopg2",
        "mysql":        "mysql+pymysql",
        "mariadb":      "mysql+pymysql",   # PyMySQL is MariaDB-compatible
        "mssql":        "mssql+pyodbc",
        "sqlserver":    "mssql+pyodbc",
        "oracle":       "oracle+oracledb",
        "db2":          "db2+ibm_db",
        "vitess":       "mysql+pymysql",   # Vitess speaks MySQL wire protocol
        "informix":     "informix+ibm_db_sa",
        # Non-SQLAlchemy paths:
        "mongodb":      None,
        "mongodb+srv":  None,
        "spanner":      None,
        "cassandra":    None,
    }
    driver = DRIVER_MAP.get(scheme)
    if driver is None:
        return None
    # Re-prefix the URI so SQLAlchemy uses the right dialect+driver.
    return driver + db_uri[len(scheme):]


# ---------------------------------------------------------------------------
# JDBC initial load (SQL + MongoDB)
# ---------------------------------------------------------------------------

def run_jdbc_initial_load(
    db_uri: str,
    tables: List[str],
    bucket_name: str,
    chunk_size: int = 200_000,
) -> Dict[str, int]:
    """
    Bulk-read every requested table from the source database and write each
    one to ``ingested/{canonical_table}/chunk_NNNN.parquet`` inside *bucket_name*.

    SQL databases are read with pandas ``read_sql_table`` (chunked) via
    SQLAlchemy.  MongoDB collections are read via PyMongo cursor with
    skip/limit pagination.

    Args:
        db_uri:      Database URI (postgresql://, mysql://, mongodb://, …).
        tables:      Tables/collections to load.  Pass [] to auto-discover all.
        bucket_name: MinIO bucket name (tenant's business_id).
        chunk_size:  Rows per write chunk (default 200 000).

    Returns:
        Dict mapping canonical table name → total rows written.
    """
    print(f"\n{'='*60}", flush=True)
    print(f"INITIAL LOAD (DB): bucket={bucket_name}", flush=True)
    print(f"{'='*60}\n", flush=True)

    client = _make_minio_client()
    _ensure_bucket(client, bucket_name)

    scheme = urlparse(db_uri).scheme.lower()
    is_mongo = scheme in ("mongodb", "mongodb+srv")

    if is_mongo:
        row_counts = _mongo_load(db_uri, tables, bucket_name, client, chunk_size)
    else:
        row_counts = _sql_load(db_uri, tables, bucket_name, client, chunk_size)

    total = sum(row_counts.values())
    print(f"\n✅ DB initial load complete — {total:,} rows across {len(row_counts)} tables", flush=True)
    for tbl, n in sorted(row_counts.items()):
        print(f"   {tbl}: {n:,}", flush=True)
    return row_counts


def _sql_load(
    db_uri: str,
    tables: List[str],
    bucket_name: str,
    client: Minio,
    chunk_size: int,
) -> Dict[str, int]:
    """Load SQL tables via pandas + SQLAlchemy."""
    try:
        from sqlalchemy import create_engine, inspect as sa_inspect
    except ImportError as exc:
        raise ImportError(
            "SQLAlchemy is required for DB initial load.\n"
            "Install: pip install sqlalchemy psycopg2-binary pymysql"
        ) from exc

    sa_url = _sqlalchemy_url(db_uri)
    if sa_url is None:
        raise ValueError(
            f"DB scheme '{urlparse(db_uri).scheme}' is not handled by SQLAlchemy. "
            "Please use the API mode for this source."
        )

    print(f"  Connecting: {sa_url.split('@')[-1]}", flush=True)
    engine = create_engine(sa_url, pool_pre_ping=True)

    # Auto-discover tables when none specified.
    if not tables:
        insp = sa_inspect(engine)
        tables = insp.get_table_names()
        print(f"  Auto-discovered {len(tables)} tables", flush=True)

    row_counts: Dict[str, int] = {}

    for raw_table in tables:
        canonical = _resolve_canonical(raw_table)
        if canonical is None:
            print(f"  ⚠️  Skipping '{raw_table}' — no canonical schema match", flush=True)
            continue

        print(f"\n  📥 {raw_table} → {canonical} …", flush=True)

        chunk_idx = 0
        total_rows = 0
        buf_frames: List[pd.DataFrame] = []
        buf_rows = 0

        def _flush_buffer(frames: List[pd.DataFrame], idx: int) -> None:
            merged = pd.concat(frames, ignore_index=True)
            obj = f"ingested/{canonical}/chunk_{idx:04d}.parquet"
            _write_parquet(client, bucket_name, obj, merged)
            print(f"    chunk_{idx:04d}: {len(merged):,} rows → {obj}", flush=True)

        def _read_chunks(sql_callable):
            """Try chunked read; on error re-raise so caller can retry."""
            return pd.read_sql(sql_callable, con=engine, chunksize=chunk_size)

        # Try read_sql_table first (preserves native dtypes), fall back to
        # a raw SELECT * query for views or quoted/schema-qualified names.
        try:
            chunk_iter = pd.read_sql_table(raw_table, con=engine, chunksize=chunk_size)
        except Exception as err_tbl:
            print(f"    read_sql_table failed ({err_tbl}) — trying SELECT * …", flush=True)
            try:
                chunk_iter = pd.read_sql_query(  # type: ignore[call-overload]
                    f"SELECT * FROM {raw_table}",
                    con=engine,
                    chunksize=chunk_size,
                )
            except Exception as err_qry:
                print(f"  ❌ Cannot read '{raw_table}': {err_qry}", flush=True)
                continue

        try:
            for chunk_df in chunk_iter:
                # Normalise: keep NaN as None (JSON-null compat).
                chunk_df = chunk_df.where(pd.notnull(chunk_df), None)

                buf_frames.append(chunk_df)
                buf_rows += len(chunk_df)
                total_rows += len(chunk_df)

                if buf_rows >= chunk_size:
                    _flush_buffer(buf_frames, chunk_idx)
                    chunk_idx += 1
                    buf_frames = []
                    buf_rows = 0

            # Final partial chunk.
            if buf_frames:
                _flush_buffer(buf_frames, chunk_idx)

        except Exception as exc:
            print(f"  ❌ Error reading chunks of '{raw_table}': {exc}", flush=True)
            continue

        if total_rows > 0:
            row_counts[canonical] = row_counts.get(canonical, 0) + total_rows
            print(f"  ✅ {canonical}: {total_rows:,} rows", flush=True)
        else:
            print(f"  ⚠️  {canonical}: empty table — skipping", flush=True)

    engine.dispose()
    return row_counts


def _mongo_load(
    db_uri: str,
    collections: List[str],
    bucket_name: str,
    client: Minio,
    chunk_size: int,
) -> Dict[str, int]:
    """Load MongoDB collections via PyMongo skip/limit pagination."""
    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise ImportError(
            "PyMongo is required for MongoDB initial load.\n"
            "Install: pip install pymongo"
        ) from exc

    db_name = urlparse(db_uri).path.lstrip("/").split("?")[0]

    # Ensure authSource=admin when the URI doesn't already specify it.
    # Debezium users are conventionally created in the admin database, so
    # PyMongo must authenticate there even when the target database is
    # different.  Without this, PyMongo tries to auth against the database
    # named in the URI path and gets "Authentication failed" (code 18).
    _parsed_uri = urlparse(db_uri)
    _qs = _parsed_uri.query or ""
    if "authSource" not in _qs and "authsource" not in _qs.lower():
        _sep = "&" if _qs else ""
        _uri_with_auth = db_uri.rstrip("?") + ("?" if not _qs else "") + _sep + "authSource=admin"
    else:
        _uri_with_auth = db_uri

    # directConnection=True bypasses replica-set topology discovery.
    # Without it, PyMongo reads the replica-set member list from the server
    # and tries to reconnect to the *advertised* hostnames (often "localhost"
    # inside Docker / private networks), which are unreachable from the
    # container running this initial load.  With directConnection=True we
    # always talk to the exact host:port in the URI.
    mongo = MongoClient(_uri_with_auth, directConnection=True)
    db = mongo[db_name]

    if not collections:
        collections = db.list_collection_names()
        print(f"  Auto-discovered {len(collections)} MongoDB collections", flush=True)

    row_counts: Dict[str, int] = {}

    for coll_name in collections:
        canonical = _resolve_canonical(coll_name)
        if canonical is None:
            print(f"  ⚠️  Skipping '{coll_name}' — no canonical match", flush=True)
            continue

        print(f"\n  📥 {coll_name} → {canonical} …", flush=True)
        coll = db[coll_name]
        total_docs = coll.count_documents({})
        if total_docs == 0:
            print(f"  ⚠️  Collection '{coll_name}' is empty — skipping", flush=True)
            continue

        chunk_idx = 0
        total_rows = 0
        skip = 0

        while True:
            docs = list(coll.find({}, {"_id": 0}).skip(skip).limit(chunk_size))
            if not docs:
                break

            rows = []
            for doc in docs:
                row: Dict = {}
                for k, v in doc.items():
                    if isinstance(v, (dict, list)):
                        row[k] = json.dumps(v, default=str)
                    elif hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    else:
                        row[k] = v
                rows.append(row)

            df = pd.DataFrame(rows)
            obj = f"ingested/{canonical}/chunk_{chunk_idx:04d}.parquet"
            _write_parquet(client, bucket_name, obj, df)
            n = len(df)
            total_rows += n
            print(f"    chunk_{chunk_idx:04d}: {n:,} rows → {obj}", flush=True)

            chunk_idx += 1
            skip += chunk_size
            if len(docs) < chunk_size:
                break

        if total_rows > 0:
            row_counts[canonical] = row_counts.get(canonical, 0) + total_rows

    mongo.close()
    return row_counts


# ---------------------------------------------------------------------------
# API initial load
# ---------------------------------------------------------------------------

def run_api_initial_load(
    api_url: str,
    poll_duration: int,
    bucket_name: str,
    poll_interval: int = 10,
) -> Dict[str, int]:
    """
    Poll the user's external API for *poll_duration* seconds and write all
    collected records to ``ingested/{canonical_table}/chunk_NNNN.parquet``.

    Entirely bypasses Kafka and Spark Streaming for the initial onboarding
    snapshot.  The expected API response format is::

        {
          "tables": [
            { "table_name": "<name>", "data": [{...}, ...] },
            ...
          ]
        }

    Args:
        api_url:       External API endpoint URL.
        poll_duration: How long to poll in total (seconds).
        bucket_name:   MinIO bucket / tenant business_id.
        poll_interval: Seconds between successive polls (default 10).

    Returns:
        Dict mapping canonical table name → total rows written.
    """
    try:
        import requests as _req
    except ImportError as exc:
        raise ImportError("requests library is required: pip install requests") from exc

    print(f"\n{'='*60}", flush=True)
    print(f"INITIAL LOAD (API): bucket={bucket_name}", flush=True)
    print(f"Polling {api_url} for {poll_duration}s …", flush=True)
    print(f"{'='*60}\n", flush=True)

    client = _make_minio_client()
    _ensure_bucket(client, bucket_name)

    # Per-table in-memory accumulator; flushed when it exceeds MAX_IN_MEM rows.
    MAX_IN_MEM = 500_000
    accumulated:     Dict[str, List[dict]] = {}
    written_chunks:  Dict[str, int]        = {}   # table → next chunk index
    total_rows:      Dict[str, int]        = {}

    def _flush(canonical: str, rows: List[dict]) -> None:
        if not rows:
            return
        df = pd.DataFrame(rows)
        idx = written_chunks.get(canonical, 0)
        obj = f"ingested/{canonical}/chunk_{idx:04d}.parquet"
        _write_parquet(client, bucket_name, obj, df)
        written_chunks[canonical] = idx + 1
        print(f"  {canonical} chunk_{idx:04d}: {len(rows):,} rows → {obj}", flush=True)

    deadline = time.monotonic() + poll_duration
    poll_num  = 0

    while time.monotonic() < deadline:
        try:
            resp = _req.get(api_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"  ⚠️  Poll error (retrying): {exc}", flush=True)
            time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))
            continue

        for tbl_entry in (data.get("tables") or []):
            raw_name  = (tbl_entry.get("table_name") or "").strip()
            canonical = _resolve_canonical(raw_name)
            if not canonical:
                continue

            rows = tbl_entry.get("data") or []
            clean_rows = []
            for row in rows:
                clean: Dict = {}
                for k, v in row.items():
                    if isinstance(v, (dict, list)):
                        clean[k] = json.dumps(v, default=str)
                    elif hasattr(v, "isoformat"):
                        clean[k] = v.isoformat()
                    else:
                        clean[k] = v
                clean_rows.append(clean)

            accumulated.setdefault(canonical, []).extend(clean_rows)
            total_rows[canonical] = total_rows.get(canonical, 0) + len(clean_rows)

            if len(accumulated[canonical]) >= MAX_IN_MEM:
                _flush(canonical, accumulated[canonical])
                accumulated[canonical] = []

        poll_num += 1
        remaining = max(0, deadline - time.monotonic())
        print(
            f"  Poll {poll_num}: {sum(total_rows.values()):,} rows total "
            f"({remaining:.0f}s remaining)",
            flush=True,
        )
        time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))

    # Final flush for all tables with pending data.
    for canonical, rows in accumulated.items():
        _flush(canonical, rows)

    grand_total = sum(total_rows.values())
    print(f"\n✅ API initial load complete — {grand_total:,} rows across {len(total_rows)} tables", flush=True)
    for tbl, n in sorted(total_rows.items()):
        print(f"   {tbl}: {n:,}", flush=True)
    return total_rows
