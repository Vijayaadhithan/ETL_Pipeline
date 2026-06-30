from __future__ import annotations

import json
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .stage3_attributes import clean, dedupe


LABELS = {
    "title": "Title",
    "description": "Description",
    "meta_title": "Listing meta title",
    "meta_description": "Listing meta description",
    "meta_keywords": "Listing meta keywords",
    "keywords": "Ad keywords",
    "custom_cat_value": "Custom text",
    "main_category_name": "Main category",
    "main_category_meta_title": "Main category meta title",
    "main_category_meta_description": "Main category meta description",
    "main_category_meta_keywords": "Main category meta keywords",
    "subcategory_name": "Subcategory",
    "subcategory_meta_title": "Subcategory meta title",
    "subcategory_meta_description": "Subcategory meta description",
    "subcategory_meta_keywords": "Subcategory meta keywords",
    "rental_duration": "Listing rental duration",
    "state_name": "State",
    "city_name": "City",
    "locality_name": "Locality",
    "locality_district": "District",
    "attributes_text": "Selected attributes",
    "attribute_values_text": "Selected attribute values",
    "attribute_keywords_text": "Selected attribute keywords",
}


def labeled(row: pd.Series, columns: list[str]) -> str:
    parts = []
    for column in columns:
        value = clean(row.get(column))
        if value:
            parts.append(f"{LABELS.get(column, column)}: {value}")
    return "\n".join(dedupe(parts))


def plain(row: pd.Series, columns: list[str]) -> str:
    return " ".join(dedupe([clean(row.get(column)) for column in columns]))


def run(config: PipelineConfig, *, sample_size: int | None = None, no_csv: bool = False) -> dict[str, Any]:
    input_path = config.output.intermediate / f"{config.artifact_prefix}_stage_03_attributes_enriched.parquet"
    df = pd.read_parquet(input_path)
    config.output.final.mkdir(parents=True, exist_ok=True)
    if sample_size is not None:
        df = df.head(sample_size).copy()
    missing = [c for c in config.embedding_source_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing embedding source columns: {missing}")
    df["embedding_content"] = df.apply(lambda row: labeled(row, config.embedding_source_columns), axis=1)
    df["bm25_content"] = df.apply(lambda row: plain(row, config.bm25_source_columns), axis=1)
    df["embedding_source_columns_json"] = json.dumps(config.embedding_source_columns, ensure_ascii=False)
    df["embedding_source_column_count"] = len(config.embedding_source_columns)
    df["embedding_non_empty_source_column_count"] = df.apply(
        lambda row: sum(1 for c in config.embedding_source_columns if clean(row.get(c))), axis=1
    )
    df["embedding_content_char_count"] = df["embedding_content"].str.len().fillna(0).astype(int)
    df["embedding_content_token_estimate"] = (df["embedding_content_char_count"] / 4).round().astype(int)
    parquet = config.output.final / f"{config.artifact_prefix}_embedding_ready.parquet"
    csv = config.output.final / f"{config.artifact_prefix}_embedding_ready.csv"
    df.to_parquet(parquet, index=False)
    if not no_csv:
        df.to_csv(csv, index=False)
    report = {
        "input_rows": int(len(df)),
        "embedding_source_column_count": len(config.embedding_source_columns),
        "bm25_source_column_count": len(config.bm25_source_columns),
        "rows_with_embedding_content": int(df["embedding_content"].map(clean).ne("").sum()),
        "average_embedding_content_char_count": round(float(df["embedding_content_char_count"].mean()), 2),
        "output_files": {"parquet": str(parquet), "csv": str(csv)},
    }
    (config.output.final / "embedding_ready_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
