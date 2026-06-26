from __future__ import annotations

import json
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .stage3_attributes import clean


REQUIRED_COLUMNS = {"id", "embedding_content", "bm25_content"}
INTEGER_COLUMNS = {"id", "main_category_id", "subcategory_id", "state_id", "city_id", "locality_id"}
FLOAT_COLUMNS = {"rental_fee", "city_latitude", "city_longitude", "locality_latitude", "locality_longitude"}
DATETIME_COLUMNS = {"created_at", "updated_at"}


def cast_search_ready_types(df: pd.DataFrame) -> pd.DataFrame:
    typed = df.copy()
    for column in INTEGER_COLUMNS & set(typed.columns):
        typed[column] = pd.to_numeric(typed[column], errors="coerce").astype("Int64")
    for column in FLOAT_COLUMNS & set(typed.columns):
        typed[column] = pd.to_numeric(typed[column], errors="coerce")
    for column in DATETIME_COLUMNS & set(typed.columns):
        typed[column] = pd.to_datetime(typed[column], errors="coerce")
    for column in set(typed.columns) - INTEGER_COLUMNS - FLOAT_COLUMNS - DATETIME_COLUMNS:
        typed[column] = typed[column].map(clean).astype("string")
    return typed


def run(config: PipelineConfig, *, sample_size: int | None = None, no_csv: bool = False) -> dict[str, Any]:
    input_path = config.output.final / "ads_embedding_ready.parquet"
    if not input_path.exists():
        raise FileNotFoundError(f"Missing Stage 4 output: {input_path}")

    df = pd.read_parquet(input_path)
    if sample_size is not None:
        df = df.head(sample_size).copy()

    columns = config.search_ready_columns
    if not columns:
        raise RuntimeError("Missing search_ready.columns in configs/pipeline.yaml.")
    missing = [column for column in columns if column not in df.columns]
    required_missing = sorted(REQUIRED_COLUMNS - set(df.columns))
    if required_missing:
        raise ValueError(f"Missing required search-ready columns: {required_missing}")

    selected = df[[column for column in columns if column in df.columns]].copy()
    selected = cast_search_ready_types(selected)

    duplicate_rows = int(selected["id"].duplicated(keep=False).sum()) if "id" in selected.columns else 0
    empty_embedding_rows = int(selected["embedding_content"].map(clean).eq("").sum())
    empty_bm25_rows = int(selected["bm25_content"].map(clean).eq("").sum())

    parquet = config.output.final / "ads_search_ready.parquet"
    csv = config.output.final / "ads_search_ready.csv"
    selected.to_parquet(parquet, index=False)
    if not no_csv:
        selected.to_csv(csv, index=False)

    report = {
        "input_file": str(input_path),
        "output_rows": int(len(selected)),
        "output_columns": int(len(selected.columns)),
        "configured_columns": int(len(columns)),
        "missing_optional_columns": missing,
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_embedding_content_rows": empty_embedding_rows,
        "empty_bm25_content_rows": empty_bm25_rows,
        "output_files": {"parquet": str(parquet), "csv": str(csv) if not no_csv else ""},
    }
    (config.output.final / "search_ready_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return report
