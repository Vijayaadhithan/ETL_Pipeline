from __future__ import annotations

import json
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .stage3_attributes import clean


def run_final_verification(
    config: PipelineConfig,
    *,
    sample_size: int | None = None,
    max_mismatch_rows: int = 5000,
) -> dict[str, Any]:
    path = config.output.final / "ads_embedding_ready.parquet"
    legacy_path = config.output.final / "ads_stage_04_embedding_ready.parquet"
    if not path.exists() and legacy_path.exists():
        path = legacy_path
    df = pd.read_parquet(path, columns=["id", "embedding_content"])
    if sample_size is not None:
        df = df.head(sample_size)
    duplicate_rows = int(df["id"].duplicated(keep=False).sum())
    empty_embedding = int(df["embedding_content"].map(clean).eq("").sum())
    status = "PASS" if duplicate_rows == 0 and empty_embedding == 0 else "FAIL"
    report = {
        "status": status,
        "input_file": str(path),
        "rows_checked": int(len(df)),
        "duplicate_ad_id_rows": duplicate_rows,
        "empty_embedding_content_rows": empty_embedding,
    }
    (config.output.reports / "final_output_correctness_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report
