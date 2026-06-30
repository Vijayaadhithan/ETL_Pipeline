from __future__ import annotations

import json
import logging
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage3_attributes import clean, dedupe


LOGGER = logging.getLogger("rag_ht_pipeline.stage4_embedding_ready")
BATCH_SIZE = 25_000


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


def enrich_content_frame(df: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    enriched = df.copy()
    enriched["embedding_content"] = enriched.apply(
        lambda row: labeled(row, config.embedding_source_columns),
        axis=1,
    )
    enriched["bm25_content"] = enriched.apply(
        lambda row: plain(row, config.bm25_source_columns),
        axis=1,
    )
    enriched["embedding_source_columns_json"] = json.dumps(
        config.embedding_source_columns,
        ensure_ascii=False,
    )
    enriched["embedding_source_column_count"] = len(config.embedding_source_columns)
    enriched["embedding_non_empty_source_column_count"] = enriched.apply(
        lambda row: sum(1 for column in config.embedding_source_columns if clean(row.get(column))),
        axis=1,
    )
    enriched["embedding_content_char_count"] = (
        enriched["embedding_content"].str.len().fillna(0).astype(int)
    )
    enriched["embedding_content_token_estimate"] = (
        enriched["embedding_content_char_count"] / 4
    ).round().astype(int)
    return enriched


def run(config: PipelineConfig, *, sample_size: int | None = None, no_csv: bool = False) -> dict[str, Any]:
    started = perf_counter()
    input_path = config.output.intermediate / f"{config.artifact_prefix}_stage_03_attributes_enriched.parquet"
    config.output.final.mkdir(parents=True, exist_ok=True)
    source = pq.ParquetFile(input_path)
    missing = [column for column in config.embedding_source_columns if column not in source.schema.names]
    if missing:
        raise ValueError(f"Missing embedding source columns: {missing}")

    parquet = config.output.final / f"{config.artifact_prefix}_embedding_ready.parquet"
    csv = config.output.final / f"{config.artifact_prefix}_embedding_ready.csv"
    temp_parquet = Path(f"{parquet}.tmp")
    temp_csv = Path(f"{csv}.tmp")
    temp_parquet.unlink(missing_ok=True)
    temp_csv.unlink(missing_ok=True)

    writer: pq.ParquetWriter | None = None
    writer_schema: pa.Schema | None = None
    rows_written = 0
    rows_with_content = 0
    total_characters = 0
    csv_header = True
    remaining = sample_size
    try:
        for batch_number, batch in enumerate(source.iter_batches(batch_size=BATCH_SIZE), start=1):
            if remaining is not None:
                if remaining <= 0:
                    break
                if len(batch) > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= len(batch)
            frame = enrich_content_frame(batch.to_pandas(), config)
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer_schema = table.schema
                writer = pq.ParquetWriter(temp_parquet, writer_schema, compression="snappy")
            elif table.schema != writer_schema:
                table = table.cast(writer_schema)
            writer.write_table(table)
            if not no_csv:
                frame.to_csv(
                    temp_csv,
                    mode="w" if csv_header else "a",
                    header=csv_header,
                    index=False,
                )
                csv_header = False
            rows_written += len(frame)
            rows_with_content += int(frame["embedding_content"].map(clean).ne("").sum())
            total_characters += int(frame["embedding_content_char_count"].sum())
            LOGGER.info(
                "Embedding-ready batch %s complete: rows=%s total=%s",
                batch_number,
                len(frame),
                rows_written,
            )
    except Exception:
        temp_parquet.unlink(missing_ok=True)
        temp_csv.unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()

    if rows_written == 0:
        raise RuntimeError(f"No rows were read from Stage 3 output: {input_path}")
    temp_parquet.replace(parquet)
    if not no_csv:
        temp_csv.replace(csv)
    else:
        csv.unlink(missing_ok=True)

    report = {
        "input_rows": rows_written,
        "embedding_source_column_count": len(config.embedding_source_columns),
        "bm25_source_column_count": len(config.bm25_source_columns),
        "rows_with_embedding_content": rows_with_content,
        "average_embedding_content_char_count": round(total_characters / rows_written, 2),
        "batch_size": BATCH_SIZE,
        "duration_seconds": round(perf_counter() - started, 2),
        "output_files": {"parquet": str(parquet), "csv": str(csv) if not no_csv else ""},
    }
    (config.output.final / "embedding_ready_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
