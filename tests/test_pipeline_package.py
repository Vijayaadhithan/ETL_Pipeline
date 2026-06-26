from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

from rag_ht_pipeline.config import load_config  # noqa: E402
from rag_ht_pipeline.postgres_loader import read_input  # noqa: E402
from rag_ht_pipeline.stage3_attributes import clean, dedupe  # noqa: E402


def test_config_has_full_embedding_and_bm25_columns() -> None:
    config = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    assert len(config.embedding_source_columns) == 23
    assert "attributes_text" in config.embedding_source_columns
    assert "attribute_values_text" in config.embedding_source_columns
    assert len(config.bm25_source_columns) == 21


def test_final_embedding_ready_file_is_readable_if_present() -> None:
    final_file = PROJECT_ROOT / "output/final/ads_embedding_ready.parquet"
    if not final_file.exists():
        return
    df = pd.read_parquet(final_file, columns=["id", "embedding_content"])
    assert len(df) > 0
    assert df["id"].notna().all()
    assert df["embedding_content"].map(clean).ne("").all()


def test_postgres_loader_reads_parquet() -> None:
    final_file = PROJECT_ROOT / "output/final/ads_embedding_ready.parquet"
    if not final_file.exists():
        return
    df = read_input(final_file)
    assert len(df) > 0
    assert "embedding_content" in df.columns


def test_clean_and_dedupe_helpers() -> None:
    assert clean("  NULL ") == ""
    assert clean(" hello   world ") == "hello world"
    assert dedupe(["A", "a", "", "B"]) == ["A", "B"]
