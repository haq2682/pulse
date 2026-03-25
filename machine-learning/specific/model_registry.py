import json
from datetime import datetime


def hdfs_path_exists(spark, path: str) -> bool:
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI.create(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False


def save_best_model_manifest(
    spark,
    model_dir: str,
    best_model: str,
    metric_name: str,
    metric_value: float,
    model_scores: dict | None = None,
    filename: str = "_best_model_manifest",
):
    manifest = {
        "best_model": best_model,
        "metric": metric_name,
        "metric_value": float(metric_value),
        "generated_at": datetime.now().isoformat(),
        "model_scores": model_scores or {},
    }

    payload = json.dumps(manifest)
    manifest_path = f"{model_dir.rstrip('/')}/{filename}"
    spark.createDataFrame([(payload,)], ["value"]).coalesce(1).write.mode("overwrite").text(manifest_path)
    return manifest_path


def load_best_model_manifest(spark, model_dir: str, filename: str = "_best_model_manifest"):
    manifest_path = f"{model_dir.rstrip('/')}/{filename}"
    if not hdfs_path_exists(spark, manifest_path):
        return None

    try:
        row = spark.read.text(manifest_path).limit(1).collect()
        if row and row[0]["value"]:
            return json.loads(row[0]["value"])
    except Exception:
        return None

    return None


def resolve_best_model(spark, model_dir: str, candidate_models: list[str], preferred_model: str | None = None):
    manifest = load_best_model_manifest(spark, model_dir)

    if manifest:
        best_model = manifest.get("best_model")
        if best_model in candidate_models:
            return best_model, "manifest", manifest

    existing = [
        model_name
        for model_name in candidate_models
        if hdfs_path_exists(spark, f"{model_dir.rstrip('/')}/{model_name}")
    ]

    if preferred_model and preferred_model in existing:
        return preferred_model, "preferred", manifest or {}

    if len(existing) == 1:
        return existing[0], "single_available", manifest or {}

    if existing:
        return existing[0], "first_available", manifest or {}

    if preferred_model:
        return preferred_model, "preferred_fallback", manifest or {}

    if candidate_models:
        return candidate_models[0], "default_fallback", manifest or {}

    return None, "none", manifest or {}


def resolve_best_affinity_model_type(spark, model_dir: str, preferred_type: str = "kmeans"):
    metrics_path = f"{model_dir.rstrip('/')}/product_affinity_metrics.json"

    try:
        metrics_row = spark.read.json(metrics_path).select("best_models").first()
        best_models = metrics_row["best_models"] if metrics_row else None

        if not best_models:
            return preferred_type, "preferred_fallback"

        candidates = []
        for model_type in ["kmeans", "gmm", "bisecting_kmeans"]:
            info = best_models.get(model_type)
            if info is not None and info.get("silhouette") is not None:
                candidates.append((model_type, float(info["silhouette"])))

        if not candidates:
            return preferred_type, "preferred_fallback"

        best_model_type = sorted(candidates, key=lambda item: item[1], reverse=True)[0][0]
        return best_model_type, "metrics"
    except Exception:
        return preferred_type, "preferred_fallback"
