from __future__ import annotations

import json
import logging
from collections import Counter
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .config import PipelineConfig
from .operations import atomic_write_json, read_json, utc_now
from .stage3_attributes import clean


LOGGER = logging.getLogger("rag_ht_pipeline.validation")
VALIDATION_BATCH_SIZE = 25_000


def _ratio_report(config: PipelineConfig, filename: str) -> dict[str, Any]:
    return read_json(config.output.reports / filename) or {}


def _quality_metrics(config: PipelineConfig, rows: int) -> dict[str, float | int]:
    category = _ratio_report(config, "category_join_report.json")
    location = _ratio_report(config, "location_join_report.json")
    attributes = _ratio_report(config, "attribute_mapping_report.json")
    metrics: dict[str, float | int] = {"rows": rows}
    if category.get("input_rows"):
        metrics["category_resolution_ratio"] = float(category.get("resolved_rows", 0)) / int(category["input_rows"])
    if location.get("input_rows"):
        denominator = int(location["input_rows"])
        metrics["city_resolution_ratio"] = float(location.get("resolved_city", 0)) / denominator
        metrics["locality_resolution_ratio"] = float(location.get("resolved_locality", 0)) / denominator
    if attributes.get("input_rows"):
        metrics["attribute_mapping_ratio"] = float(attributes.get("mapped_ads", 0)) / int(attributes["input_rows"])
        metrics["attribute_schema_mismatch_rows"] = int(attributes.get("schema_mismatch_rows", 0))
    return metrics


def _quality_failures(config: PipelineConfig, metrics: dict[str, float | int]) -> list[str]:
    settings = config.quality
    failures: list[str] = []
    minimums = {
        "category_resolution_ratio": float(settings.get("min_category_resolution_ratio", 0.0)),
        "city_resolution_ratio": float(settings.get("min_city_resolution_ratio", 0.0)),
        "locality_resolution_ratio": float(settings.get("min_locality_resolution_ratio", 0.0)),
        "attribute_mapping_ratio": float(settings.get("min_attribute_mapping_ratio", 0.0)),
    }
    for metric, minimum in minimums.items():
        if metric in metrics and float(metrics[metric]) < minimum:
            failures.append(f"{metric}={float(metrics[metric]):.4f} is below minimum {minimum:.4f}")

    baseline_path = config.output.reports / "quality_baseline.json"
    baseline = read_json(baseline_path) or {}
    previous = dict(baseline.get("metrics", {}))
    previous_rows = int(previous.get("rows") or 0)
    maximum_drop = float(settings.get("max_row_drop_fraction", 0.10))
    if previous_rows and int(metrics["rows"]) < previous_rows * (1 - maximum_drop):
        failures.append(
            f"row count dropped from {previous_rows} to {metrics['rows']}, beyond allowed {maximum_drop:.2%}"
        )
    maximum_ratio_regression = float(settings.get("max_ratio_regression", 0.05))
    for metric in minimums:
        if metric in metrics and metric in previous:
            regression = float(previous[metric]) - float(metrics[metric])
            if regression > maximum_ratio_regression:
                failures.append(
                    f"{metric} regressed by {regression:.4f}, beyond allowed {maximum_ratio_regression:.4f}"
                )
    return failures


def run_final_verification(
    config: PipelineConfig,
    *,
    sample_size: int | None = None,
    max_mismatch_rows: int = 5000,
) -> dict[str, Any]:
    config.output.reports.mkdir(parents=True, exist_ok=True)
    path = config.output.final / f"{config.artifact_prefix}_search_ready.parquet"
    legacy_path = config.output.final / f"{config.artifact_prefix}_embedding_ready.parquet"
    if not path.exists() and legacy_path.exists():
        path = legacy_path
    required = ["company_id", "id", "title", "description", "embedding_content", "bm25_content", "extras_json"]
    available = set(pq.read_schema(path).names)
    missing = sorted(set(required) - available)
    if missing:
        raise ValueError(f"Final output is missing canonical columns: {missing}")

    source = pq.ParquetFile(path)
    id_counts: Counter[Any] = Counter()
    rows_checked = 0
    empty_embedding = 0
    empty_bm25 = 0
    wrong_company = 0
    empty_id = 0
    remaining = sample_size
    for batch_number, batch in enumerate(
        source.iter_batches(
            batch_size=VALIDATION_BATCH_SIZE,
            columns=required,
        ),
        start=1,
    ):
        if remaining is not None:
            if remaining <= 0:
                break
            if len(batch) > remaining:
                batch = batch.slice(0, remaining)
            remaining -= len(batch)
        frame = batch.to_pandas()
        rows_checked += len(frame)
        id_counts.update(
            None
            if pd.isna(value)
            else value.item()
            if hasattr(value, "item")
            else value
            for value in frame["id"]
        )
        empty_embedding += int(
            frame["embedding_content"].map(clean).eq("").sum()
        )
        empty_bm25 += int(frame["bm25_content"].map(clean).eq("").sum())
        wrong_company += int(
            frame["company_id"].map(clean).ne(config.company_id).sum()
        )
        empty_id += int(frame["id"].map(clean).eq("").sum())
        LOGGER.info(
            "Validation batch %s complete: rows=%s total=%s",
            batch_number,
            len(frame),
            rows_checked,
        )

    duplicate_rows = sum(count for count in id_counts.values() if count > 1)
    metrics = _quality_metrics(config, rows_checked)
    quality_failures = _quality_failures(config, metrics)
    status = "PASS" if duplicate_rows == 0 and empty_id == 0 and empty_embedding == 0 and empty_bm25 == 0 and wrong_company == 0 and not quality_failures else "FAIL"
    report = {
        "status": status,
        "input_file": str(path),
        "rows_checked": rows_checked,
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_ad_id_rows": empty_id,
        "empty_embedding_content_rows": empty_embedding,
        "empty_bm25_content_rows": empty_bm25,
        "wrong_company_id_rows": wrong_company,
        "batch_size": VALIDATION_BATCH_SIZE,
        "quality_metrics": metrics,
        "quality_failures": quality_failures,
    }
    (config.output.reports / "final_output_correctness_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    if status == "PASS" and sample_size is None:
        atomic_write_json(
            config.output.reports / "quality_baseline.json",
            {
                "company_id": config.company_id,
                "accepted_at": utc_now(),
                "metrics": metrics,
            },
        )
    return report
