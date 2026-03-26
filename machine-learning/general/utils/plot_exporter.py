import os
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import matplotlib
import numpy as np

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


def _detect_training_task(metrics: List[Dict[str, float]]) -> str:
    keys = {str(k).lower() for row in metrics for k in row.keys()}

    regression_keys = {"rmse", "mae", "r2", "mape"}
    classification_keys = {"accuracy", "precision", "recall", "f1", "f1_score", "auc", "auc_roc", "auc_pr", "log_loss"}
    clustering_keys = {"silhouette", "wssse", "log_likelihood", "stability_ari", "k"}

    if keys & regression_keys:
        return "regression"
    if keys & classification_keys:
        return "classification"
    if keys & clustering_keys:
        return "clustering"
    return "generic"


def _detect_inference_task(model_name: str, label_column: str, numeric_columns: Iterable[str]) -> str:
    model_hint = f"{model_name} {label_column}".lower()
    numeric_hints = " ".join(str(c).lower() for c in (numeric_columns or []))
    full_hint = f"{model_hint} {numeric_hints}"

    if any(token in full_hint for token in ["cluster", "persona", "segment", "centroid", "distance"]):
        return "clustering"
    if any(token in full_hint for token in ["probability", "confidence", "risk", "class", "churn", "sentiment", "status"]):
        return "classification"
    if any(token in full_hint for token in ["revenue", "price", "forecast", "days", "amount", "quantity", "score", "time"]):
        return "regression"
    return "classification"


def _save_figure(fig, output_path: str, paths: List[str]) -> None:
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    paths.append(output_path)


def _convex_hull(points: np.ndarray) -> np.ndarray:
    if points.shape[0] <= 2:
        return points

    pts = sorted({(float(x), float(y)) for x, y in points.tolist()})
    if len(pts) <= 2:
        return np.array(pts)

    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    lower = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    upper = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    hull = lower[:-1] + upper[:-1]
    return np.array(hull)


def _plot_decision_regions(ax, x_vals: np.ndarray, y_vals: np.ndarray, labels: List[str], alpha: float = 0.28) -> Dict[str, int]:
    unique_labels = sorted(set(labels))
    if not unique_labels:
        return {}

    label_to_idx = {label: idx for idx, label in enumerate(unique_labels)}

    centroids = {}
    for label in unique_labels:
        mask = np.array([lbl == label for lbl in labels])
        if not np.any(mask):
            continue
        centroids[label] = np.array([np.mean(x_vals[mask]), np.mean(y_vals[mask])])

    if not centroids:
        return label_to_idx

    x_margin = max((x_vals.max() - x_vals.min()) * 0.15, 1e-6)
    y_margin = max((y_vals.max() - y_vals.min()) * 0.15, 1e-6)
    xx, yy = np.meshgrid(
        np.linspace(x_vals.min() - x_margin, x_vals.max() + x_margin, 220),
        np.linspace(y_vals.min() - y_margin, y_vals.max() + y_margin, 220),
    )
    grid = np.c_[xx.ravel(), yy.ravel()]

    centroid_labels = list(centroids.keys())
    centroid_matrix = np.vstack([centroids[lbl] for lbl in centroid_labels])
    distances = np.sum((grid[:, None, :] - centroid_matrix[None, :, :]) ** 2, axis=2)
    nearest = np.argmin(distances, axis=1)
    z = np.array([label_to_idx[centroid_labels[idx]] for idx in nearest]).reshape(xx.shape)

    ax.contourf(xx, yy, z, alpha=alpha, cmap="coolwarm")
    return label_to_idx


def _plot_regression_scatter_with_fit(ax, x_vals: np.ndarray, y_vals: np.ndarray, xlabel: str, ylabel: str, title: str) -> None:
    ax.scatter(x_vals, y_vals, s=34, alpha=0.65, edgecolor="black", linewidth=0.5, label="Sample data")

    if len(x_vals) >= 2 and np.std(x_vals) > 0:
        slope, intercept = np.polyfit(x_vals, y_vals, 1)
        x_line = np.linspace(x_vals.min(), x_vals.max(), 150)
        y_line = slope * x_line + intercept
        ax.plot(x_line, y_line, color="black", linewidth=2.0, label="Regression model")

        corr = np.corrcoef(x_vals, y_vals)[0, 1] if np.std(y_vals) > 0 else 0.0
        r2 = float(np.clip(corr ** 2 if np.isfinite(corr) else 0.0, 0.0, 1.0))
        ax.set_title(f"{title} (R²={r2:.2f})")
    else:
        ax.set_title(title)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(loc="best")


def _plot_training_regression(rows: List[Dict[str, float]], out_dir: str, script_slug: str, run_slug: str, paths: List[str]) -> None:
    metric_keys = [m for m in ["r2", "rmse", "mae", "mape"] if any(isinstance(r.get(m), (int, float)) for r in rows)]
    if not metric_keys:
        return

    if len(rows) == 1 and len(metric_keys) >= 2:
        row = rows[0]
        x_vals = np.arange(len(metric_keys), dtype=float)
        y_vals = np.array([float(row.get(m, np.nan)) for m in metric_keys], dtype=float)
        valid = np.isfinite(y_vals)
        if np.any(valid):
            fig, ax = plt.subplots(figsize=(10, 5))
            _plot_regression_scatter_with_fit(
                ax,
                x_vals[valid],
                y_vals[valid],
                xlabel="Metric Index",
                ylabel="Metric Value",
                title="Regression Metric Samples + Fit",
            )
            ax.set_xticks(x_vals[valid])
            ax.set_xticklabels([metric_keys[int(i)].upper() for i in x_vals[valid]], rotation=20, ha="right")
            _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-regression-fit.png"), paths)
        return

    run_names = [str(r.get("model_name") or r.get("model") or r.get("type") or f"run_{i+1}") for i, r in enumerate(rows)]
    if "rmse" in metric_keys and "r2" in metric_keys:
        x_vals = np.array([float(r.get("rmse", np.nan)) for r in rows], dtype=float)
        y_vals = np.array([float(r.get("r2", np.nan)) for r in rows], dtype=float)
        valid = np.isfinite(x_vals) & np.isfinite(y_vals)
        if np.any(valid):
            fig, ax = plt.subplots(figsize=(8, 6))
            _plot_regression_scatter_with_fit(ax, x_vals[valid], y_vals[valid], "RMSE", "R²", "Regression Model Frontier")
            for idx, name in enumerate(run_names):
                if valid[idx]:
                    ax.annotate(name, (x_vals[idx], y_vals[idx]), fontsize=8, xytext=(4, 4), textcoords="offset points")
            _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-regression-frontier-fit.png"), paths)


def _plot_training_classification(rows: List[Dict[str, float]], out_dir: str, script_slug: str, run_slug: str, paths: List[str]) -> None:
    run_names = [str(r.get("model_name") or r.get("model") or r.get("type") or f"run_{i+1}") for i, r in enumerate(rows)]
    x_candidates = ["recall", "accuracy", "auc", "auc_roc", "f1", "f1_score"]
    y_candidates = ["precision", "f1", "f1_score", "auc", "auc_roc", "accuracy"]

    x_key = next((k for k in x_candidates if any(isinstance(r.get(k), (int, float)) for r in rows)), None)
    y_key = next((k for k in y_candidates if any(isinstance(r.get(k), (int, float)) for r in rows) and k != x_key), None)
    if not x_key or not y_key:
        return

    x_vals = np.array([float(r.get(x_key, np.nan)) for r in rows], dtype=float)
    y_vals = np.array([float(r.get(y_key, np.nan)) for r in rows], dtype=float)
    valid = np.isfinite(x_vals) & np.isfinite(y_vals)
    if not np.any(valid):
        return

    fig, ax = plt.subplots(figsize=(8, 6))
    labels = [run_names[i] for i, ok in enumerate(valid) if ok]
    label_to_idx = _plot_decision_regions(ax, x_vals[valid], y_vals[valid], labels)
    colors = [label_to_idx[label] for label in labels]
    sc = ax.scatter(x_vals[valid], y_vals[valid], c=colors, cmap="coolwarm", edgecolor="black", s=52)
    for i, name in enumerate(labels):
        ax.annotate(name, (x_vals[valid][i], y_vals[valid][i]), fontsize=8, xytext=(4, 4), textcoords="offset points")
    ax.set_xlabel(x_key.upper())
    ax.set_ylabel(y_key.upper())
    ax.set_title("Classification Decision Regions (metric space)")
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label("Model index")
    _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-classification-decision-regions.png"), paths)


def _plot_training_clustering(rows: List[Dict[str, float]], out_dir: str, script_slug: str, run_slug: str, paths: List[str]) -> None:
    rows_with_k = [r for r in rows if isinstance(r.get("k"), (int, float))]
    if not rows_with_k:
        return

    grouped: Dict[str, List[Dict[str, float]]] = {}
    for row in rows_with_k:
        algo = str(row.get("type") or row.get("model") or row.get("model_name") or "model")
        grouped.setdefault(algo, []).append(row)

    fig, ax = plt.subplots(figsize=(9, 6))
    cmap = plt.get_cmap("tab10")
    markers = ["o", "^", "s", "D", "v", "P", "X", "*"]
    plotted = False

    for idx, (algo, algo_rows) in enumerate(grouped.items()):
        algo_rows = sorted(algo_rows, key=lambda r: float(r["k"]))
        k_vals = np.array([float(r["k"]) for r in algo_rows], dtype=float)
        sil_vals = np.array([float(r.get("silhouette", np.nan)) for r in algo_rows], dtype=float)
        valid = np.isfinite(k_vals) & np.isfinite(sil_vals)
        if not np.any(valid):
            continue

        x = k_vals[valid]
        y = sil_vals[valid]
        color = cmap(idx % 10)
        marker = markers[idx % len(markers)]

        ax.scatter(x, y, color=color, marker=marker, s=64, alpha=0.9, label=algo)
        if len(x) >= 3:
            hull = _convex_hull(np.column_stack([x, y]))
            if len(hull) >= 3:
                closed = np.vstack([hull, hull[0]])
                ax.plot(closed[:, 0], closed[:, 1], color=color, linewidth=1.4)
                ax.fill(closed[:, 0], closed[:, 1], color=color, alpha=0.18)
        elif len(x) >= 2:
            ax.plot(x, y, color=color, linewidth=1.2)
        plotted = True

    if plotted:
        ax.set_title("Cluster Plot (k vs Silhouette with polygons)")
        ax.set_xlabel("k")
        ax.set_ylabel("Silhouette")
        ax.legend(title="algorithm")
        _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-clustering-polygons.png"), paths)
    else:
        plt.close(fig)

    if any(isinstance(r.get("wssse"), (int, float)) for r in rows_with_k):
        fig, ax = plt.subplots(figsize=(9, 5))
        for algo, algo_rows in grouped.items():
            algo_rows = sorted(algo_rows, key=lambda r: float(r["k"]))
            k_vals = [int(r["k"]) for r in algo_rows]
            w_vals = [float(r.get("wssse", np.nan)) for r in algo_rows]
            if not any(np.isfinite(w_vals)):
                continue
            ax.plot(k_vals, w_vals, marker="o", linewidth=2, label=algo)
        ax.set_title("Elbow Trend (WSSSE vs k)")
        ax.set_xlabel("Number of Clusters (k)")
        ax.set_ylabel("WSSSE")
        ax.legend()
        _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-clustering-elbow.png"), paths)


def _plot_training_generic(rows: List[Dict[str, float]], out_dir: str, script_slug: str, run_slug: str, paths: List[str]) -> None:
    row = rows[0]
    metric_values = _numeric_metrics(row)
    if not metric_values:
        return
    labels = list(metric_values.keys())
    values = [metric_values[k] for k in labels]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(np.arange(len(values)), values, marker="o", linewidth=2)
    ax.set_xticks(np.arange(len(labels)))
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_title("Training Metrics")
    _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-generic.png"), paths)


def _collect_numeric_values(predictions_df, numeric_col: str, sample_limit: int) -> List[float]:
    return [
        float(row[numeric_col])
        for row in predictions_df.select(numeric_col).limit(sample_limit).collect()
        if row[numeric_col] is not None
    ]


def _collect_xy_label(predictions_df, x_col: str, y_col: str, label_col: str, sample_limit: int):
    sample = predictions_df.select(label_col, x_col, y_col).limit(sample_limit).collect()
    labels = []
    x_vals = []
    y_vals = []
    for row in sample:
        if row[x_col] is None or row[y_col] is None or row[label_col] is None:
            continue
        x_vals.append(float(row[x_col]))
        y_vals.append(float(row[y_col]))
        labels.append(str(row[label_col]))
    return np.array(x_vals, dtype=float), np.array(y_vals, dtype=float), labels


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
    task = _detect_training_task(metrics)

    generated_paths: List[str] = []
    run_slug = _to_slug(model_name)

    if task == "regression":
        _plot_training_regression(metrics, out_dir, script_slug, run_slug, generated_paths)
    elif task == "classification":
        _plot_training_classification(metrics, out_dir, script_slug, run_slug, generated_paths)
    elif task == "clustering":
        _plot_training_clustering(metrics, out_dir, script_slug, run_slug, generated_paths)
    else:
        _plot_training_generic(metrics, out_dir, script_slug, run_slug, generated_paths)

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
    task = _detect_inference_task(model_name, label_column, numeric_columns or [])

    # Label distribution plot
    counts = (
        predictions_df.groupBy(label_column)
        .count()
        .orderBy("count", ascending=False)
        .collect()
    )

    if counts:
        labels = [str(row[label_column]) for row in counts][:15]
        values = [int(row["count"]) for row in counts][:15]

        if task == "classification":
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
            ax.set_title(f"Class Share - {model_name}")
            _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-class-share.png"), paths)
        else:
            fig, ax = plt.subplots(figsize=(9, 5))
            ax.plot(np.arange(len(values)), values, marker="o", linewidth=2)
            ax.set_xticks(np.arange(len(labels)))
            ax.set_xticklabels(labels, rotation=25, ha="right")
            ax.set_title(f"Label Distribution - {model_name}")
            ax.set_ylabel("Count")
            _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-distribution.png"), paths)

    valid_numeric = [c for c in (numeric_columns or []) if c in predictions_df.columns]

    if task == "regression":
        for numeric_col in valid_numeric[:4]:
            values = _collect_numeric_values(predictions_df, numeric_col, sample_limit)
            if not values:
                continue
            fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
            axes[0].hist(values, bins=30)
            axes[0].set_title(f"{numeric_col} Histogram")
            axes[1].boxplot(values, vert=True)
            axes[1].set_title(f"{numeric_col} Box Plot")
            _save_figure(
                fig,
                os.path.join(out_dir, f"{script_slug}-{run_slug}-{_to_slug(numeric_col)}-regression.png"),
                paths,
            )

        if len(valid_numeric) >= 2:
            x_col, y_col = valid_numeric[0], valid_numeric[1]
            sample = predictions_df.select(x_col, y_col).limit(sample_limit).collect()
            x_vals = [float(r[x_col]) for r in sample if r[x_col] is not None and r[y_col] is not None]
            y_vals = [float(r[y_col]) for r in sample if r[x_col] is not None and r[y_col] is not None]
            if x_vals and y_vals:
                fig, ax = plt.subplots(figsize=(7, 6))
                x_arr = np.array(x_vals, dtype=float)
                y_arr = np.array(y_vals, dtype=float)
                _plot_regression_scatter_with_fit(ax, x_arr, y_arr, x_col, y_col, "Regression Output Relationship")
                _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-regression-scatter.png"), paths)

    elif task == "classification":
        score_col = next((c for c in valid_numeric if any(tok in c.lower() for tok in ["prob", "confidence", "score"])), None)
        if score_col:
            values = _collect_numeric_values(predictions_df, score_col, sample_limit)
            if values:
                fig, ax = plt.subplots(figsize=(8, 5))
                ax.hist(values, bins=25)
                ax.set_title(f"{score_col} Distribution")
                ax.set_xlabel(score_col)
                ax.set_ylabel("Frequency")
                _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-classification-confidence.png"), paths)

        if score_col and counts:
            top_labels = [str(r[label_column]) for r in counts[:8]]
            sample = predictions_df.select(label_column, score_col).limit(sample_limit).collect()
            grouped_scores = {label: [] for label in top_labels}
            for row in sample:
                lbl = str(row[label_column])
                val = row[score_col]
                if lbl in grouped_scores and val is not None:
                    grouped_scores[lbl].append(float(val))
            labels = [k for k, v in grouped_scores.items() if v]
            data = [grouped_scores[k] for k in labels]
            if data:
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.boxplot(data, labels=labels, showfliers=False)
                ax.set_title(f"{score_col} by Class")
                ax.set_xticklabels(labels, rotation=25, ha="right")
                _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-classification-by-label.png"), paths)

        if len(valid_numeric) >= 2 and label_column in predictions_df.columns:
            x_col, y_col = valid_numeric[0], valid_numeric[1]
            x_arr, y_arr, cls_labels = _collect_xy_label(predictions_df, x_col, y_col, label_column, sample_limit)
            if len(cls_labels) >= 3 and len(set(cls_labels)) >= 2:
                fig, ax = plt.subplots(figsize=(8, 6))
                label_to_idx = _plot_decision_regions(ax, x_arr, y_arr, cls_labels, alpha=0.30)
                color_vals = [label_to_idx[lbl] for lbl in cls_labels]
                sc = ax.scatter(x_arr, y_arr, c=color_vals, cmap="coolwarm", s=30, edgecolor="black", linewidth=0.35)
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                ax.set_title("Classification Decision Boundary")
                cbar = plt.colorbar(sc, ax=ax)
                cbar.set_label("Class index")
                _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-classification-boundary.png"), paths)

    else:  # clustering and fallback
        if valid_numeric:
            for numeric_col in valid_numeric[:3]:
                values = _collect_numeric_values(predictions_df, numeric_col, sample_limit)
                if not values:
                    continue
                fig, ax = plt.subplots(figsize=(8, 5))
                ax.hist(values, bins=30)
                ax.set_title(f"{numeric_col} Distribution")
                _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-{_to_slug(numeric_col)}-cluster-metric.png"), paths)

        if len(valid_numeric) >= 2 and label_column in predictions_df.columns:
            x_col, y_col = valid_numeric[0], valid_numeric[1]
            x_arr, y_arr, cluster_labels = _collect_xy_label(predictions_df, x_col, y_col, label_column, sample_limit)
            if len(cluster_labels) >= 3:
                uniq = sorted(set(cluster_labels))
                cmap = plt.get_cmap("tab10")
                markers = ["o", "^", "s", "D", "v", "P", "X", "*"]

                fig, ax = plt.subplots(figsize=(8, 6))
                for idx, cluster in enumerate(uniq):
                    mask = np.array([lbl == cluster for lbl in cluster_labels])
                    if not np.any(mask):
                        continue
                    cx = x_arr[mask]
                    cy = y_arr[mask]
                    color = cmap(idx % 10)
                    marker = markers[idx % len(markers)]
                    ax.scatter(cx, cy, color=color, marker=marker, s=28, alpha=0.75, label=str(cluster))

                    if len(cx) >= 3:
                        hull = _convex_hull(np.column_stack([cx, cy]))
                        if len(hull) >= 3:
                            closed = np.vstack([hull, hull[0]])
                            ax.plot(closed[:, 0], closed[:, 1], color=color, linewidth=1.2)
                            ax.fill(closed[:, 0], closed[:, 1], color=color, alpha=0.15)
                    elif len(cx) == 2:
                        ax.plot(cx, cy, color=color, linewidth=1.0)

                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                ax.set_title("Cluster Plot")
                ax.legend(title="cluster", loc="best")
                _save_figure(fig, os.path.join(out_dir, f"{script_slug}-{run_slug}-clustering-hulls.png"), paths)

    if paths:
        print(f"✓ Exported inference plot(s): {', '.join(paths)}")
        return paths

    print("⚠️  Plot export skipped: no plottable inference outputs found")
    return None