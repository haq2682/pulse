def _discover_cleaned_tables(minio_client, bucket_name):
    """
    Recursively scan ``cleaned/`` and return a dict mapping each discovered
    table name to its storage format: ``"delta"``, ``"parquet"``, or ``"csv"``.

    Uses ``recursive=True`` so every object is returned regardless of whether
    MinIO emits directory placeholder entries — avoids the ``recursive=False``
    false-negative on ``_delta_log/`` subdirectories.

    Priority: delta > parquet > csv
    """
    discovered_delta = set()
    discovered_parquet = set()
    discovered_csv = set()

    try:
        all_objs = list(
            minio_client.list_objects(bucket_name, prefix="cleaned/", recursive=True)
        )
    except Exception as e:
        print(f"WARNING: Could not scan cleaned/ directory: {e}")
        return {}

    for obj in all_objs:
        path = obj.object_name  # e.g. "cleaned/orders/_delta_log/00000.json"
        parts = path.split("/")
        if len(parts) < 2:
            continue

        if path.endswith(".csv") and len(parts) == 2:
            # Direct CSV file: cleaned/orders.csv
            table_name = parts[1].replace(".csv", "")
            discovered_csv.add(table_name)
        elif len(parts) >= 3:
            table_name = parts[1]
            if "_delta_log" in parts:
                discovered_delta.add(table_name)
            elif path.endswith(".parquet") and "_delta_log" not in path:
                discovered_parquet.add(table_name)

    result = {}
    for t in discovered_delta:
        result[t] = "delta"
    for t in discovered_parquet:
        if t not in result:
            result[t] = "parquet"
    for t in discovered_csv:
        if t not in result:
            result[t] = "csv"

    return result


def load_data_from_minio(spark, minio_client, bucket_name):
    """
    Load cleaned DataFrames from MinIO.

    Dynamically discovers every table that cleaning actually wrote under
    ``cleaned/`` instead of iterating a fixed list — tables that do not exist
    are simply skipped rather than silently failing three checks in a row.

    Read priority (highest → lowest):
    1. **Delta table** – ``_delta_log/`` present anywhere inside the table dir.
    2. **Plain Parquet** – ``.parquet`` data files present but no delta log.
    3. **CSV** – direct ``cleaned/{table}.csv`` file.
    """
    # Primary keys used for deduplication guard (Delta MERGE already handles
    # this, but kept as a safety net for plain-parquet / CSV replays).
    primary_keys = {
        "products": ["product_id"],
        "customers": ["customer_id"],
        "orders": ["order_id"],
        "order_items": ["order_item_id"],
        "suppliers": ["supplier_id"],
        "inventory": ["inventory_id"],
        "customer_sessions": ["session_id"],
        "wishlist": ["wishlist_id"],
        "shopping_cart": ["cart_id"],
        "cart_items": ["cart_item_id"],
        "reviews": ["review_id"],
        "payments": ["payment_id"],
        "marketing_campaigns": ["campaign_id"],
        "addresses": ["address_id"],
        "categories": ["category_id"],
    }

    print(f"\n🔍 Scanning cleaned/ directory in bucket '{bucket_name}'...")
    table_formats = _discover_cleaned_tables(minio_client, bucket_name)

    if not table_formats:
        print("⚠️  No tables found in cleaned/ — transformation has nothing to process.")
        return {}

    print(f"   Discovered {len(table_formats)} table(s):")
    for tname in sorted(table_formats):
        print(f"   └─ {tname}  [{table_formats[tname]}]")

    dataframes = {}

    for table_name, fmt in table_formats.items():
        s3_path = f"s3a://{bucket_name}/cleaned/{table_name}"
        df = None

        if fmt == "delta":
            try:
                df = spark.read.format("delta").load(s3_path)
                print(f"Loaded {table_name} from Delta")
            except Exception as e:
                print(f"Delta read failed for {table_name}: {e}")
                # Delta failed — do NOT fall through to plain parquet because
                # Delta directories contain mixed-schema checkpoint .parquet
                # files alongside data files; plain-parquet read will error.

        elif fmt == "parquet":
            try:
                df = (
                    spark.read
                    .option("mergeSchema", "true")
                    .parquet(s3_path)
                )
                print(f"Loaded {table_name} from Parquet (legacy)")
            except Exception as e:
                print(f"Parquet read failed for {table_name}: {e}")

        elif fmt == "csv":
            try:
                df = spark.read.csv(
                    f"s3a://{bucket_name}/cleaned/{table_name}.csv",
                    header=True,
                    inferSchema=True,
                )
                print(f"Loaded {table_name} from CSV (legacy)")
            except Exception as e:
                print(f"CSV read failed for {table_name}: {e}")

        if df is None:
            print(f"⚠️  Could not load {table_name} — skipping")
            continue

        # Dedup guard on primary key
        if table_name in primary_keys:
            df = df.dropDuplicates(primary_keys[table_name])

        dataframes[table_name] = df

    loaded = sorted(dataframes)
    missing = sorted(set(primary_keys) - set(dataframes))
    print(f"\n   ✅ Loaded {len(loaded)} table(s): {', '.join(loaded) or 'none'}")
    if missing:
        print(f"   ⚠️  Not found in cleaned/: {', '.join(missing)}")

    return dataframes
