"""
KS-test (Kolmogorov-Smirnov) drift detection engine.

Usage
-----
Called from ml_retrain_dag.py via PythonOperator.  Compares current feature
distributions (loaded from MinIO transformed/ parquet files) against saved
baseline distributions from the last training run.

Returns a drift report that the DAG uses to decide whether to retrain models.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from typing import Any

import numpy as np
from scipy import stats

# Allow running standalone as well as from within Airflow
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.pipeline_config import (
    KS_ALPHA,
    KS_DRIFT_RATIO_TRIGGER,
    KS_MIN_SAMPLE_SIZE,
    MODEL_FEATURE_MAP,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    MINIO_PREFIX_DRIFT_BASELINES,
    MINIO_PREFIX_TRANSFORMED,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

def _get_minio_client(endpoint=None, access_key=None, secret_key=None):
    from minio import Minio
    endpoint   = (endpoint or MINIO_ENDPOINT).lstrip("http://").lstrip("https://")
    access_key = access_key or MINIO_ACCESS_KEY
    secret_key = secret_key or MINIO_SECRET_KEY
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=MINIO_SECURE)


def _read_parquet_from_minio(client, bucket: str, prefix: str) -> "pd.DataFrame | None":
    """Download all parquet objects under *prefix* and concat into a DataFrame."""
    import pandas as pd

    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    parquet_objects = [o for o in objects if o.object_name.endswith(".parquet")]

    if not parquet_objects:
        log.warning("No parquet files found at %s/%s", bucket, prefix)
        return None

    frames = []
    for obj in parquet_objects:
        try:
            data = client.get_object(bucket, obj.object_name)
            buf = io.BytesIO(data.read())
            frames.append(pd.read_parquet(buf))
        except Exception as exc:
            log.warning("Could not read %s: %s", obj.object_name, exc)

    if not frames:
        return None

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# KS-test core
# ---------------------------------------------------------------------------

def _ks_test_feature(
    baseline_samples: list[float],
    current_samples: np.ndarray,
    alpha: float = KS_ALPHA,
) -> dict[str, Any]:
    """Run a two-sample KS test and return a result dict."""
    baseline_arr = np.array(baseline_samples, dtype=float)
    current_arr  = np.array(current_samples,  dtype=float)

    # Drop NaN/inf
    baseline_arr = baseline_arr[np.isfinite(baseline_arr)]
    current_arr  = current_arr[np.isfinite(current_arr)]

    if len(baseline_arr) < 2 or len(current_arr) < 2:
        return {
            "ks_statistic": None,
            "p_value": None,
            "drifted": False,
            "skipped": True,
            "reason": "Insufficient samples after filtering",
        }

    ks_stat, p_value = stats.ks_2samp(baseline_arr, current_arr)
    drifted = bool(p_value < alpha)

    return {
        "ks_statistic": float(ks_stat),
        "p_value":       float(p_value),
        "drifted":       drifted,
        "skipped":       False,
        "baseline_n":    len(baseline_arr),
        "current_n":     len(current_arr),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_ks_tests_for_model(
    model_name: str,
    bucket: str,
    alpha: float = KS_ALPHA,
    min_samples: int = KS_MIN_SAMPLE_SIZE,
    minio_client=None,
) -> dict[str, Any]:
    """
    Run KS tests for a single model.

    1. Load the saved baseline (saved by model_baseline.save_baseline).
    2. Load current data from MinIO transformed/<table>/*.parquet.
    3. For each feature, run scipy.stats.ks_2samp.
    4. Return a drift report dict.

    Returns
    -------
    {
        "model_name":        str,
        "drift_detected":    bool,
        "drift_ratio":       float,   # fraction of features that drifted
        "drifted_features":  list[str],
        "details":           {feature: {ks_statistic, p_value, drifted, ...}},
        "error":             str | None,
    }
    """
    client = minio_client or _get_minio_client()

    report: dict[str, Any] = {
        "model_name":       model_name,
        "drift_detected":   False,
        "drift_ratio":      0.0,
        "drifted_features": [],
        "details":          {},
        "error":            None,
    }

    if model_name not in MODEL_FEATURE_MAP:
        report["error"] = f"No feature map entry for model '{model_name}'"
        log.warning(report["error"])
        return report

    feature_cfg = MODEL_FEATURE_MAP[model_name]
    table_name  = feature_cfg["table"]
    features    = feature_cfg["features"]

    # ── Load baseline ──────────────────────────────────────────────────────
    baseline_key = f"{MINIO_PREFIX_DRIFT_BASELINES}{model_name}/baseline.json"
    try:
        data = client.get_object(bucket, baseline_key)
        baseline: dict[str, dict] = json.loads(data.read())
    except Exception as exc:
        report["error"] = f"No baseline found for '{model_name}' ({exc}). Run initial training first."
        log.warning(report["error"])
        return report

    # ── Load current data ──────────────────────────────────────────────────
    prefix = f"{MINIO_PREFIX_TRANSFORMED}{table_name}/"
    current_df = _read_parquet_from_minio(client, bucket, prefix)

    if current_df is None or len(current_df) < min_samples:
        report["error"] = (
            f"Insufficient current data for '{model_name}' "
            f"(found {0 if current_df is None else len(current_df)} rows, "
            f"need ≥{min_samples})"
        )
        log.warning(report["error"])
        return report

    # ── Run KS test per feature ────────────────────────────────────────────
    drifted_features = []
    details: dict[str, Any] = {}

    for feature in features:
        if feature not in baseline:
            log.debug("Feature '%s' not in baseline for '%s', skipping", feature, model_name)
            details[feature] = {"skipped": True, "reason": "Not in baseline"}
            continue

        if feature not in current_df.columns:
            log.debug("Feature '%s' missing from current data for '%s', skipping", feature, model_name)
            details[feature] = {"skipped": True, "reason": "Missing from current data"}
            continue

        result = _ks_test_feature(
            baseline_samples=baseline[feature]["samples"],
            current_samples=current_df[feature].dropna().values,
            alpha=alpha,
        )
        details[feature] = result

        if result.get("drifted"):
            drifted_features.append(feature)

    total_tested = sum(1 for v in details.values() if not v.get("skipped"))
    drift_ratio  = len(drifted_features) / max(total_tested, 1)

    report["drift_detected"]   = drift_ratio >= KS_DRIFT_RATIO_TRIGGER
    report["drift_ratio"]      = drift_ratio
    report["drifted_features"] = drifted_features
    report["details"]          = details

    log.info(
        "KS drift report for '%s': drift_ratio=%.2f (%d/%d features), trigger=%.2f, retrain=%s",
        model_name, drift_ratio, len(drifted_features), total_tested,
        KS_DRIFT_RATIO_TRIGGER, report["drift_detected"],
    )

    return report


def run_ks_tests_all_models(
    bucket: str,
    model_names: list[str] | None = None,
    alpha: float = KS_ALPHA,
    min_samples: int = KS_MIN_SAMPLE_SIZE,
    minio_client=None,
) -> dict[str, Any]:
    """
    Run KS tests for every model (or a subset) and return a combined report.

    Returns
    -------
    {
        "models_with_drift":    list[str],
        "models_without_drift": list[str],
        "models_with_errors":   list[str],
        "any_drift_detected":   bool,
        "per_model":            { model_name: <report_from_run_ks_tests_for_model> },
    }
    """
    client = minio_client or _get_minio_client()
    names  = model_names or list(MODEL_FEATURE_MAP.keys())

    per_model: dict[str, Any] = {}
    for name in names:
        per_model[name] = run_ks_tests_for_model(
            model_name=name,
            bucket=bucket,
            alpha=alpha,
            min_samples=min_samples,
            minio_client=client,
        )

    models_with_drift    = [n for n, r in per_model.items() if r["drift_detected"]]
    models_without_drift = [n for n, r in per_model.items() if not r["drift_detected"] and not r["error"]]
    models_with_errors   = [n for n, r in per_model.items() if r["error"]]

    summary = {
        "models_with_drift":    models_with_drift,
        "models_without_drift": models_without_drift,
        "models_with_errors":   models_with_errors,
        "any_drift_detected":   bool(models_with_drift),
        "per_model":            per_model,
    }

    log.info(
        "KS summary: %d models with drift, %d without, %d errors",
        len(models_with_drift), len(models_without_drift), len(models_with_errors),
    )

    return summary
