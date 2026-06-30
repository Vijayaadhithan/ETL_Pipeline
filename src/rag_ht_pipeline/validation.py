from __future__ import annotations

import json
import logging
from collections import Counter
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage3_attributes import clean


LOGGER = logging.getLogger("rag_ht_pipeline.validation")
VALIDATION_BATCH_SIZE = 25_000


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
        LOGGER.info(
            "Validation batch %s complete: rows=%s total=%s",
            batch_number,
            len(frame),
            rows_checked,
        )

    duplicate_rows = sum(count for count in id_counts.values() if count > 1)
    status = "PASS" if duplicate_rows == 0 and empty_embedding == 0 and empty_bm25 == 0 and wrong_company == 0 else "FAIL"
    report = {
        "status": status,
        "input_file": str(path),
        "rows_checked": rows_checked,
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_embedding_content_rows": empty_embedding,
        "empty_bm25_content_rows": empty_bm25,
        "wrong_company_id_rows": wrong_company,
        "batch_size": VALIDATION_BATCH_SIZE,
    }
    (config.output.reports / "final_output_correctness_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report
