import os
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_EXPORT_DIR = "/app/logs_for_report"


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def _timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _sanitize(name: str) -> str:
    return "".join(c if c.isalnum() or c in {"-", "_"} else "_" for c in name)


def _to_slug(name: str) -> str:
    value = str(name or "unknown")
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = value.strip().replace(" ", "_").replace("-", "_")
    value = re.sub(r"_+", "_", value)
    return _sanitize(value.lower()).strip("_") or "unknown"


def _numeric_metrics(row: Dict[str, float], ignored_keys: Optional[Iterable[str]] = None) -> Dict[str, float]:
    ignored = set(ignored_keys or [])
    return {
        key: float(value)
        for key, value in row.items()
        if key not in ignored and isinstance(value, (int, float))
    }


def export_training_metrics_plot(
    model_name: str,
    metrics: List[Dict[str, float]],
    export_plots: bool = False,
    export_dir: str = DEFAULT_EXPORT_DIR,
    script_name: Optional[str] = None,
) -> Optional[str]:
    if not export_plots:
        return None

    if not metrics:
        print("⚠️  Plot export skipped: no training metrics available")
        return None

    out_dir = _ensure_dir(export_dir)
    script_slug = _to_slug(script_name or model_name)
    grouped: Dict[str, List[Dict[str, float]]] = {}

    for row in metrics:
        run_name = row.get("model_name") or row.get("model") or row.get("type") or model_name
        run_slug = _to_slug(run_name)
        grouped.setdefault(run_slug, []).append(row)

    generated_paths: List[str] = []

    for run_slug, rows in grouped.items():
        rows_with_k = [r for r in rows if isinstance(r.get("k"), (int, float))]

        for row in rows_with_k:
            metric_values = _numeric_metrics(row, ignored_keys=["k"])
            if not metric_values:
                continue
            labels = list(metric_values.keys())
            values = [metric_values[k] for k in labels]
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.bar(labels, values)
            ax.set_title(f"{run_slug} metrics (k={int(row['k'])})")
            ax.tick_params(axis="x", rotation=25)
            for x, value in enumerate(values):
                ax.text(x, value, f"{value:.3f}", ha="center", va="bottom", fontsize=8)
            fig.tight_layout()
            k_path = os.path.join(out_dir, f"{script_slug}-{run_slug}-k{int(row['k'])}.png")
            fig.savefig(k_path, dpi=180, bbox_inches="tight")
            plt.close(fig)
            generated_paths.append(k_path)

        if rows_with_k:
            metric_keys = sorted(
                {
                    key
                    for row in rows_with_k
                    for key, value in row.items()
                    if key not in {"k", "model_name", "model", "type"}
                    and isinstance(value, (int, float))
                }
            )
            if metric_keys:
                fig, axes = plt.subplots(1, len(metric_keys), figsize=(5 * len(metric_keys), 5))
                if len(metric_keys) == 1:
                    axes = [axes]
                k_values = [int(r["k"]) for r in rows_with_k]
                for idx, metric_key in enumerate(metric_keys):
                    ax = axes[idx]
                    y_values = [float(r.get(metric_key, 0.0) or 0.0) for r in rows_with_k]
                    ax.plot(k_values, y_values, marker="o")
                    ax.set_title(metric_key)
                    ax.set_xlabel("k")
                    ax.set_xticks(k_values)
                fig.suptitle(f"{run_slug} across k")
                fig.tight_layout()
                trend_path = os.path.join(out_dir, f"{script_slug}-{run_slug}.png")
                fig.savefig(trend_path, dpi=180, bbox_inches="tight")
                plt.close(fig)
                generated_paths.append(trend_path)
            continue

        row = rows[0]
        metric_values = _numeric_metrics(row)
        if not metric_values:
            continue
        labels = list(metric_values.keys())
        values = [metric_values[k] for k in labels]
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(labels, values)
        ax.set_title(run_slug)
        ax.tick_params(axis="x", rotation=25)
        for x, value in enumerate(values):
            ax.text(x, value, f"{value:.3f}", ha="center", va="bottom", fontsize=8)
        fig.tight_layout()
        output_path = os.path.join(out_dir, f"{script_slug}-{run_slug}.png")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        generated_paths.append(output_path)

    if generated_paths:
        print(f"✓ Exported training metrics plot(s): {', '.join(generated_paths)}")
        return generated_paths[0]

    print("⚠️  Plot export skipped: no numeric metrics found")
    return None


def export_inference_outputs_plot(
    model_name: str,
    predictions_df,
    label_column: str,
    numeric_columns: Optional[Iterable[str]] = None,
    export_plots: bool = False,
    export_dir: str = DEFAULT_EXPORT_DIR,
    sample_limit: int = 5000,
    script_name: Optional[str] = None,
    run_name: Optional[str] = None,
) -> Optional[List[str]]:
    if not export_plots:
        return None

    paths: List[str] = []
    out_dir = _ensure_dir(export_dir)
    script_slug = _to_slug(script_name or model_name)
    run_slug = _to_slug(run_name or model_name)

    # Label distribution plot
    counts = (
        predictions_df.groupBy(label_column)
        .count()
        .orderBy("count", ascending=False)
        .collect()
    )

    if counts:
        labels = [str(row[label_column]) for row in counts]
        values = [int(row["count"]) for row in counts]
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(labels, values)
        ax.set_title(f"Prediction Distribution - {model_name}")
        ax.set_ylabel("Count")
        ax.tick_params(axis="x", rotation=25)
        for x, value in enumerate(values):
            ax.text(x, value, str(value), ha="center", va="bottom", fontsize=8)
        fig.tight_layout()
        dist_path = os.path.join(out_dir, f"{script_slug}-{run_slug}-distribution.png")
        fig.savefig(dist_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        paths.append(dist_path)

    # Numeric outputs histogram(s)
    for numeric_col in (numeric_columns or []):
        if numeric_col not in predictions_df.columns:
            continue
        values = [
            float(row[numeric_col])
            for row in predictions_df.select(numeric_col).limit(sample_limit).collect()
            if row[numeric_col] is not None
        ]
        if not values:
            continue
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(values, bins=30)
        ax.set_title(f"{numeric_col} Distribution - {model_name}")
        ax.set_xlabel(numeric_col)
        ax.set_ylabel("Frequency")
        fig.tight_layout()
        metric_path = os.path.join(
            out_dir,
            f"{script_slug}-{run_slug}-{_to_slug(numeric_col)}.png",
        )
        fig.savefig(metric_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        paths.append(metric_path)

    if paths:
        print(f"✓ Exported inference plot(s): {', '.join(paths)}")
        return paths

    print("⚠️  Plot export skipped: no plottable inference outputs found")
    return None