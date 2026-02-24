"""
Model baseline manager.

After every (re)training run, save the feature distributions of the training
data to MinIO so that drift_detection.py can compare future data against them.

Storage layout
--------------
  {bucket}/models/drift_baselines/{model_name}/baseline.json

JSON schema
-----------
{
  "<feature_name>": {
    "mean":    float,
    "std":     float,
    "min":     float,
    "max":     float,
    "median":  float,
    "samples": [float, ...]   # up to KS_BASELINE_MAX_SAMPLES values
  },
  ...
  "_meta": {
    "model_name":   str,
    "bucket":       str,
    "saved_at":     ISO-8601 str,
    "n_rows":       int,
    "features":     [str, ...]
  }
}
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.pipeline_config import (
    KS_BASELINE_MAX_SAMPLES,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    MINIO_PREFIX_DRIFT_BASELINES,
    MINIO_PREFIX_TRANSFORMED,
    MODEL_FEATURE_MAP,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MinIO helper
# ---------------------------------------------------------------------------

def _get_minio_client(endpoint=None, access_key=None, secret_key=None):
    from minio import Minio
    endpoint   = (endpoint or MINIO_ENDPOINT).lstrip("http://").lstrip("https://")
    access_key = access_key or MINIO_ACCESS_KEY
    secret_key = secret_key or MINIO_SECRET_KEY
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=MINIO_SECURE)


def _read_parquet_from_minio(client, bucket: str, prefix: str) -> "pd.DataFrame | None":
    import io
    import pandas as pd

    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    parquet_objects = [o for o in objects if o.object_name.endswith(".parquet")]

    if not parquet_objects:
        return None

    frames = []
    for obj in parquet_objects:
        try:
            data = client.get_object(bucket, obj.object_name)
            buf  = io.BytesIO(data.read())
            frames.append(pd.read_parquet(buf))
        except Exception as exc:
            log.warning("Could not read %s: %s", obj.object_name, exc)

    return None if not frames else pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_baseline(
    model_name: str,
    bucket: str,
    minio_client=None,
    max_samples: int = KS_BASELINE_MAX_SAMPLES,
) -> bool:
    """
    Load the current transformed data for *model_name*, compute per-feature
    statistics, and persist them as the new baseline in MinIO.

    Called by ml_retrain_dag after each successful (re)training run.

    Returns True on success, False on failure.
    """
    client = minio_client or _get_minio_client()

    if model_name not in MODEL_FEATURE_MAP:
        log.error("Unknown model '%s' – not in MODEL_FEATURE_MAP", model_name)
        return False

    feature_cfg = MODEL_FEATURE_MAP[model_name]
    table_name  = feature_cfg["table"]
    features    = feature_cfg["features"]

    prefix = f"{MINIO_PREFIX_TRANSFORMED}{table_name}/"
    df = _read_parquet_from_minio(client, bucket, prefix)

    if df is None or df.empty:
        log.error(
            "Cannot save baseline for '%s': no data at %s/%s",
            model_name, bucket, prefix,
        )
        return False

    baseline: dict[str, Any] = {}
    available_features = []

    for feature in features:
        if feature not in df.columns:
            log.warning("Feature '%s' missing from table '%s', skipping", feature, table_name)
            continue

        series = df[feature].dropna()
        if len(series) == 0:
            log.warning("Feature '%s' is all-null, skipping", feature)
            continue

        arr = series.values.astype(float)
        arr = arr[np.isfinite(arr)]

        if len(arr) == 0:
            continue

        # Randomly subsample to cap storage size
        if len(arr) > max_samples:
            rng = np.random.default_rng(seed=42)
            arr = rng.choice(arr, size=max_samples, replace=False)

        baseline[feature] = {
            "mean":    float(np.mean(arr)),
            "std":     float(np.std(arr)),
            "min":     float(np.min(arr)),
            "max":     float(np.max(arr)),
            "median":  float(np.median(arr)),
            "samples": arr.tolist(),
        }
        available_features.append(feature)

    if not available_features:
        log.error("No usable features found for model '%s'", model_name)
        return False

    baseline["_meta"] = {
        "model_name":  model_name,
        "bucket":      bucket,
        "saved_at":    datetime.now(timezone.utc).isoformat(),
        "n_rows":      len(df),
        "features":    available_features,
    }

    # Upload to MinIO
    object_key  = f"{MINIO_PREFIX_DRIFT_BASELINES}{model_name}/baseline.json"
    json_bytes  = json.dumps(baseline, default=str).encode("utf-8")
    buf         = io.BytesIO(json_bytes)

    try:
        client.put_object(
            bucket, object_key, buf, length=len(json_bytes),
            content_type="application/json",
        )
        log.info(
            "Saved baseline for '%s' to %s/%s (%d features, n=%d)",
            model_name, bucket, object_key, len(available_features), len(df),
        )
        return True
    except Exception as exc:
        log.error("Failed to upload baseline for '%s': %s", model_name, exc)
        return False


def save_baselines_for_all_models(
    bucket: str,
    model_names: list[str] | None = None,
    minio_client=None,
) -> dict[str, bool]:
    """
    Save baselines for multiple models.  Returns {model_name: success_bool}.
    """
    client = minio_client or _get_minio_client()
    names  = model_names or list(MODEL_FEATURE_MAP.keys())
    return {
        name: save_baseline(name, bucket, minio_client=client)
        for name in names
    }


def load_baseline(
    model_name: str,
    bucket: str,
    minio_client=None,
) -> dict[str, Any] | None:
    """
    Load the saved baseline dict for *model_name*.  Returns None if not found.
    """
    client     = minio_client or _get_minio_client()
    object_key = f"{MINIO_PREFIX_DRIFT_BASELINES}{model_name}/baseline.json"

    try:
        data = client.get_object(bucket, object_key)
        return json.loads(data.read())
    except Exception as exc:
        log.warning("No baseline found for '%s': %s", model_name, exc)
        return None


def baseline_exists(model_name: str, bucket: str, minio_client=None) -> bool:
    """Return True if a baseline file already exists for *model_name*."""
    client     = minio_client or _get_minio_client()
    object_key = f"{MINIO_PREFIX_DRIFT_BASELINES}{model_name}/baseline.json"
    try:
        client.stat_object(bucket, object_key)
        return True
    except Exception:
        return False
