from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

from rag_ht_pipeline.config import load_config  # noqa: E402
from rag_ht_pipeline.mysql_loader import mysql_url_from_env  # noqa: E402
from rag_ht_pipeline.mysql_source_loader import load_sources_to_mysql  # noqa: E402
from rag_ht_pipeline.postgres_loader import read_input  # noqa: E402
from rag_ht_pipeline.source_sync import compare_snapshots  # noqa: E402
from rag_ht_pipeline.stage3_attributes import clean, dedupe  # noqa: E402


def test_config_has_full_embedding_and_bm25_columns() -> None:
    config = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    assert len(config.embedding_source_columns) == 23
    assert "attributes_text" in config.embedding_source_columns
    assert "attribute_values_text" in config.embedding_source_columns
    assert len(config.bm25_source_columns) == 21
    assert len(config.source_sync["tables"]) == 9
    assert {table["filename"] for table in config.source_sync["tables"]} >= {"ads.csv", "ads_attributes.csv"}


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


def test_mysql_url_uses_env_credentials(monkeypatch) -> None:
    monkeypatch.setenv("MYSQL_HOST", "mysql.example.local")
    monkeypatch.setenv("MYSQL_PORT", "3307")
    monkeypatch.setenv("MYSQL_DATABASE", "rag_ht")
    monkeypatch.setenv("MYSQL_USER", "user name")
    monkeypatch.setenv("MYSQL_PASSWORD", "pass word")

    url = mysql_url_from_env()

    assert url == "mysql+pymysql://user+name:pass+word@mysql.example.local:3307/rag_ht?charset=utf8mb4"


def test_mysql_source_loader_dry_run_reads_configured_csvs() -> None:
    config = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    result = load_sources_to_mysql(config, dry_run=True, sample_size=2)

    assert result["dry_run"] is True
    assert "ads" in result["tables"]
    assert "ads_attributes" in result["tables"]
    if result["tables"]["ads"]["status"] != "missing_csv":
        assert result["tables"]["ads"]["rows"] <= 2


def test_clean_and_dedupe_helpers() -> None:
    assert clean("  NULL ") == ""
    assert clean(" hello   world ") == "hello world"
    assert dedupe(["A", "a", "", "B"]) == ["A", "B"]


def test_source_sync_detects_added_removed_and_updated_rows() -> None:
    current = pd.DataFrame(
        [
            {"id": "1", "name": "old"},
            {"id": "2", "name": "same"},
            {"id": "3", "name": "removed"},
        ]
    )
    incoming = pd.DataFrame(
        [
            {"id": "1", "name": "new"},
            {"id": "2", "name": "same"},
            {"id": "4", "name": "added"},
        ]
    )

    report = compare_snapshots(current, incoming, primary_key="id")

    assert report["added_rows"] == 1
    assert report["removed_rows"] == 1
    assert report["updated_rows"] == 1
    assert report["sample_added_keys"] == ["4"]
    assert report["sample_removed_keys"] == ["3"]
    assert report["sample_updated_keys"] == ["1"]
