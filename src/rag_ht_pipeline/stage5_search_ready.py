from __future__ import annotations

import json
import logging
from time import perf_counter
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage3_attributes import clean


LOGGER = logging.getLogger("rag_ht_pipeline.stage5_search_ready")


REQUIRED_COLUMNS = {
    "company_id",
    "id",
    "title",
    "description",
    "embedding_content",
    "bm25_content",
    "extras_json",
}
INTEGER_COLUMNS = {"id", "main_category_id", "subcategory_id", "state_id", "city_id", "locality_id"}
FLOAT_COLUMNS = {"rental_fee", "city_latitude", "city_longitude", "locality_latitude", "locality_longitude"}
DATETIME_COLUMNS = {"created_at", "updated_at"}


def cast_search_ready_types(df: pd.DataFrame, config: PipelineConfig | None = None) -> pd.DataFrame:
    typed = df.copy()
    integer_columns = set(config.search_ready_types.get("integer", [])) if config else INTEGER_COLUMNS
    float_columns = set(config.search_ready_types.get("float", [])) if config else FLOAT_COLUMNS
    datetime_columns = set(config.search_ready_types.get("datetime", [])) if config else DATETIME_COLUMNS
    for column in integer_columns & set(typed.columns):
        typed[column] = pd.to_numeric(typed[column], errors="coerce").astype("Int64")
    for column in float_columns & set(typed.columns):
        typed[column] = pd.to_numeric(typed[column], errors="coerce")
    for column in datetime_columns & set(typed.columns):
        typed[column] = pd.to_datetime(typed[column], errors="coerce")
    for column in set(typed.columns) - integer_columns - float_columns - datetime_columns:
        typed[column] = typed[column].map(clean).astype("string")
    return typed


def run(config: PipelineConfig, *, sample_size: int | None = None, no_csv: bool = False) -> dict[str, Any]:
    started = perf_counter()
    input_path = config.output.final / f"{config.artifact_prefix}_embedding_ready.parquet"
    if not input_path.exists():
        raise FileNotFoundError(f"Missing Stage 4 output: {input_path}")

    config.output.final.mkdir(parents=True, exist_ok=True)

    columns = config.search_ready_columns
    if not columns:
        raise RuntimeError("Missing search_ready.columns in configs/pipeline.yaml.")
    available_columns = set(pq.read_schema(input_path).names)
    missing = [column for column in columns if column not in available_columns]
    required_missing = sorted(REQUIRED_COLUMNS - available_columns)
    if required_missing:
        raise ValueError(f"Missing required search-ready columns: {required_missing}")

    selected_columns = [column for column in columns if column in available_columns]
    selected = pd.read_parquet(input_path, columns=selected_columns)
    if sample_size is not None:
        selected = selected.head(sample_size).copy()
    LOGGER.info(
        "Loaded %s search-ready rows using %s of %s available columns",
        len(selected),
        len(selected_columns),
        len(available_columns),
    )
    selected = cast_search_ready_types(selected, config)

    duplicate_rows = int(selected["id"].duplicated(keep=False).sum()) if "id" in selected.columns else 0
    empty_embedding_rows = int(selected["embedding_content"].map(clean).eq("").sum())
    empty_bm25_rows = int(selected["bm25_content"].map(clean).eq("").sum())

    parquet = config.output.final / f"{config.artifact_prefix}_search_ready.parquet"
    csv = config.output.final / f"{config.artifact_prefix}_search_ready.csv"
    selected.to_parquet(parquet, index=False)
    if not no_csv:
        selected.to_csv(csv, index=False)
    else:
        csv.unlink(missing_ok=True)

    report = {
        "input_file": str(input_path),
        "output_rows": int(len(selected)),
        "output_columns": int(len(selected.columns)),
        "configured_columns": int(len(columns)),
        "missing_optional_columns": missing,
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_embedding_content_rows": empty_embedding_rows,
        "empty_bm25_content_rows": empty_bm25_rows,
        "duration_seconds": round(perf_counter() - started, 2),
        "output_files": {"parquet": str(parquet), "csv": str(csv) if not no_csv else ""},
    }
    (config.output.final / "search_ready_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return report
