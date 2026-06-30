from __future__ import annotations

import json
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage3_attributes import clean


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
    df = pd.read_parquet(path, columns=required)
    if sample_size is not None:
        df = df.head(sample_size)
    duplicate_rows = int(df["id"].duplicated(keep=False).sum())
    empty_embedding = int(df["embedding_content"].map(clean).eq("").sum())
    empty_bm25 = int(df["bm25_content"].map(clean).eq("").sum())
    wrong_company = int(df["company_id"].map(clean).ne(config.company_id).sum())
    status = "PASS" if duplicate_rows == 0 and empty_embedding == 0 and empty_bm25 == 0 and wrong_company == 0 else "FAIL"
    report = {
        "status": status,
        "input_file": str(path),
        "rows_checked": int(len(df)),
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_embedding_content_rows": empty_embedding,
        "empty_bm25_content_rows": empty_bm25,
        "wrong_company_id_rows": wrong_company,
    }
    (config.output.reports / "final_output_correctness_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report
