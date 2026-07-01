from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage3_attributes import clean


LOGGER = logging.getLogger("rag_ht_pipeline.stage5_search_ready")
BATCH_SIZE = 25_000


REQUIRED_COLUMNS = {
    "company_id",
    "id",
    "title",
    "description",
    "embedding_content",
    "bm25_content",
    "extras_json",
}
INTEGER_COLUMNS = {
    "id",
    "type",
    "is_rent_negotiable",
    "main_category_id",
    "subcategory_id",
    "state_id",
    "city_id",
    "locality_id",
}
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
    parquet = config.output.final / f"{config.artifact_prefix}_search_ready.parquet"
    csv = config.output.final / f"{config.artifact_prefix}_search_ready.csv"
    temp_parquet = Path(f"{parquet}.tmp")
    temp_csv = Path(f"{csv}.tmp")
    temp_parquet.unlink(missing_ok=True)
    temp_csv.unlink(missing_ok=True)

    source = pq.ParquetFile(input_path)
    writer: pq.ParquetWriter | None = None
    writer_schema: pa.Schema | None = None
    rows_written = 0
    empty_embedding_rows = 0
    empty_bm25_rows = 0
    id_counts: Counter[Any] = Counter()
    csv_header = True
    remaining = sample_size
    try:
        for batch_number, batch in enumerate(
            source.iter_batches(
                batch_size=BATCH_SIZE,
                columns=selected_columns,
            ),
            start=1,
        ):
            if remaining is not None:
                if remaining <= 0:
                    break
                if len(batch) > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= len(batch)
            selected = cast_search_ready_types(batch.to_pandas(), config)
            empty_embedding_rows += int(
                selected["embedding_content"].map(clean).eq("").sum()
            )
            empty_bm25_rows += int(
                selected["bm25_content"].map(clean).eq("").sum()
            )
            if "id" in selected:
                id_counts.update(
                    None
                    if pd.isna(value)
                    else value.item()
                    if hasattr(value, "item")
                    else value
                    for value in selected["id"]
                )

            table = pa.Table.from_pandas(selected, preserve_index=False)
            if writer is None:
                writer_schema = table.schema
                writer = pq.ParquetWriter(
                    temp_parquet,
                    writer_schema,
                    compression="snappy",
                )
            elif table.schema != writer_schema:
                table = table.cast(writer_schema)
            writer.write_table(table)
            if not no_csv:
                selected.to_csv(
                    temp_csv,
                    mode="w" if csv_header else "a",
                    header=csv_header,
                    index=False,
                )
                csv_header = False
            rows_written += len(selected)
            LOGGER.info(
                "Search-ready batch %s complete: rows=%s total=%s",
                batch_number,
                len(selected),
                rows_written,
            )
    except Exception:
        if writer is not None:
            writer.close()
            writer = None
        temp_parquet.unlink(missing_ok=True)
        temp_csv.unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()

    if rows_written == 0:
        temp_parquet.unlink(missing_ok=True)
        temp_csv.unlink(missing_ok=True)
        raise RuntimeError(f"No rows were read from Stage 4 output: {input_path}")

    duplicate_rows = sum(count for count in id_counts.values() if count > 1)
    temp_parquet.replace(parquet)
    if not no_csv:
        temp_csv.replace(csv)
    else:
        csv.unlink(missing_ok=True)

    LOGGER.info(
        "Built %s search-ready rows using %s of %s available columns",
        rows_written,
        len(selected_columns),
        len(available_columns),
    )

    report = {
        "input_file": str(input_path),
        "output_rows": rows_written,
        "output_columns": len(selected_columns),
        "configured_columns": int(len(columns)),
        "missing_optional_columns": missing,
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_embedding_content_rows": empty_embedding_rows,
        "empty_bm25_content_rows": empty_bm25_rows,
        "batch_size": BATCH_SIZE,
        "duration_seconds": round(perf_counter() - started, 2),
        "output_files": {"parquet": str(parquet), "csv": str(csv) if not no_csv else ""},
    }
    (config.output.final / "search_ready_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return report
