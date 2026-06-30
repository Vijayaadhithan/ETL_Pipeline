from __future__ import annotations

import json
import sys
from argparse import Namespace
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

from rag_ht_pipeline.adapters import get_adapter  # noqa: E402
from rag_ht_pipeline.config import (  # noqa: E402
    OutputLayout,
    discover_company_profiles,
    load_company_config,
    load_config,
    validate_company_slug,
)
import rag_ht_pipeline.source_sync as source_sync_module  # noqa: E402
from rag_ht_pipeline.mysql_loader import mysql_url_from_env  # noqa: E402
from rag_ht_pipeline.mysql_source_loader import load_sources_to_mysql  # noqa: E402
from rag_ht_pipeline.incremental import merge_parquet_delta  # noqa: E402
from rag_ht_pipeline.pipeline import run_batch, validate_company_isolation  # noqa: E402
from rag_ht_pipeline.postgres_loader import read_input  # noqa: E402
from rag_ht_pipeline.publisher import (  # noqa: E402
    _mysql_publish,
    credential_value,
    publish_company,
    validate_publish_file,
    validate_publish_frame,
)
from rag_ht_pipeline.source_sync import (  # noqa: E402
    compare_snapshot_changes,
    compare_snapshots,
    database_url,
    qualified_table_name,
    related_record_ids,
    resolve_source_backend,
)
from rag_ht_pipeline.stage3_attributes import clean, dedupe  # noqa: E402
from rag_ht_pipeline.stage4_embedding_ready import run as build_retrieval_content  # noqa: E402
from rag_ht_pipeline.stage5_search_ready import run as build_search_ready  # noqa: E402
from rag_ht_pipeline.stage5_search_ready import cast_search_ready_types  # noqa: E402
from rag_ht_pipeline.validation import run_final_verification  # noqa: E402


def test_config_has_full_embedding_and_bm25_columns() -> None:
    config = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    assert len(config.embedding_source_columns) == 23
    assert "attributes_text" in config.embedding_source_columns
    assert "attribute_values_text" in config.embedding_source_columns
    assert len(config.bm25_source_columns) == 21
    assert len(config.search_ready_columns) == 38
    assert config.company_id == "gainr"
    assert config.adapter == "gainr"
    assert "company_id" in config.search_ready_columns
    assert "extras_json" in config.search_ready_columns
    assert "embedding_content" in config.search_ready_columns
    assert "bm25_content" in config.search_ready_columns
    assert "embedding_source_columns_json" not in config.search_ready_columns
    assert "embedding_content_char_count" not in config.search_ready_columns
    assert "embedding_content_token_estimate" not in config.search_ready_columns
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


def test_search_ready_type_casts_numeric_and_datetime_columns() -> None:
    df = pd.DataFrame(
        [
            {
                "id": "10",
                "city_id": "20",
                "rental_fee": "1500.50",
                "city_latitude": "13.0827",
                "created_at": "2026-06-26 10:00:00",
                "title": "  Sample  ",
                "embedding_content": "content",
                "bm25_content": "bm25",
            }
        ]
    )

    typed = cast_search_ready_types(df)

    assert str(typed["id"].dtype) == "Int64"
    assert str(typed["city_id"].dtype) == "Int64"
    assert pd.api.types.is_float_dtype(typed["rental_fee"])
    assert pd.api.types.is_float_dtype(typed["city_latitude"])
    assert pd.api.types.is_datetime64_any_dtype(typed["created_at"])
    assert typed.loc[0, "title"] == "Sample"


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


def test_dependent_table_changes_resolve_affected_ad_ids() -> None:
    current = pd.DataFrame(
        [
            {"id": "10", "ads_id": "1", "value": "old"},
            {"id": "20", "ads_id": "2", "value": "removed"},
        ]
    )
    incoming = pd.DataFrame(
        [
            {"id": "10", "ads_id": "3", "value": "new"},
            {"id": "30", "ads_id": "4", "value": "added"},
        ]
    )

    _, changes = compare_snapshot_changes(current, incoming, primary_key="id")
    affected = related_record_ids(
        current,
        primary_key="id",
        row_keys=changes["removed"] | changes["updated"],
        record_key="ads_id",
    )
    affected.update(
        related_record_ids(
            incoming,
            primary_key="id",
            row_keys=changes["added"] | changes["updated"],
            record_key="ads_id",
        )
    )

    assert changes == {
        "added": {"30"},
        "removed": {"20"},
        "updated": {"10"},
    }
    assert affected == {"1", "2", "3", "4"}


def test_incremental_parquet_merge_updates_adds_and_removes_rows(
    tmp_path: Path,
) -> None:
    baseline_path = tmp_path / "baseline.parquet"
    delta_path = tmp_path / "delta.parquet"
    output_path = tmp_path / "merged.parquet"
    baseline = pd.DataFrame(
        [
            {"id": 1, "title": "one"},
            {"id": 2, "title": "old two"},
            {"id": 3, "title": "remove"},
        ]
    )
    delta = pd.DataFrame(
        [
            {"id": 2, "title": "new two"},
            {"id": 4, "title": "four"},
        ]
    )
    baseline.to_parquet(baseline_path, index=False)
    delta.to_parquet(delta_path, index=False)

    report = merge_parquet_delta(
        baseline_path,
        delta_path,
        output_path,
        removed_ids={"3"},
    )
    merged = pd.read_parquet(output_path)

    assert report == {
        "baseline_rows": 3,
        "delta_rows": 2,
        "removed_rows": 1,
        "output_rows": 3,
    }
    assert merged.to_dict("records") == [
        {"id": 1, "title": "one"},
        {"id": 2, "title": "new two"},
        {"id": 4, "title": "four"},
    ]
    assert merged.dtypes.astype(str).to_dict() == baseline.dtypes.astype(str).to_dict()


def test_source_sync_writes_exact_incremental_change_set(
    tmp_path: Path,
    monkeypatch,
) -> None:
    current_dir = tmp_path / "current"
    incoming_dir = tmp_path / "incoming"
    output_root = tmp_path / "output"
    current_dir.mkdir()
    incoming_dir.mkdir()
    pd.DataFrame(
        [
            {"id": "1", "title": "old"},
            {"id": "2", "title": "removed"},
        ]
    ).to_csv(current_dir / "ads.csv", index=False)
    pd.DataFrame(
        [
            {"id": "10", "ads_id": "1", "value": "old"},
            {"id": "20", "ads_id": "2", "value": "removed"},
        ]
    ).to_csv(current_dir / "ads_attributes.csv", index=False)
    pd.DataFrame(
        [
            {"id": "1", "title": "new"},
            {"id": "3", "title": "added"},
        ]
    ).to_csv(incoming_dir / "ads.csv", index=False)
    pd.DataFrame(
        [
            {"id": "10", "ads_id": "1", "value": "new"},
            {"id": "30", "ads_id": "3", "value": "added"},
        ]
    ).to_csv(incoming_dir / "ads_attributes.csv", index=False)
    old_backup_one = output_root / "backups" / "20260101T000000Z"
    old_backup_two = output_root / "backups" / "20260201T000000Z"
    old_backup_one.mkdir(parents=True)
    old_backup_two.mkdir(parents=True)
    (old_backup_one / "ads.csv").write_text("old backup one", encoding="utf-8")
    (old_backup_two / "ads.csv").write_text("old backup two", encoding="utf-8")

    base = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    config = replace(
        base,
        input_dir=current_dir,
        data_dir=current_dir,
        output=OutputLayout(
            root=output_root,
            intermediate=output_root / "intermediate",
            final=output_root / "final",
            reports=output_root / "reports",
            diagnostics=output_root / "diagnostics",
        ),
        source={"backend": "mysql"},
        source_sync={
            "staging_dir": str(incoming_dir),
            "backup_dir": str(output_root / "backups"),
            "tables": [
                {
                    "name": "ads",
                    "db_table": "ads",
                    "filename": "ads.csv",
                    "primary_key": "id",
                },
                {
                    "name": "ads_attributes",
                    "db_table": "ads_attributes",
                    "filename": "ads_attributes.csv",
                    "primary_key": "id",
                },
            ],
        },
        incremental={
            "record_table": "ads",
            "record_key": "id",
            "dependent_tables": {"ads_attributes": "ads_id"},
            "full_rebuild_tables": [],
        },
    )
    monkeypatch.setattr(
        source_sync_module,
        "export_database_tables",
        lambda *args, **kwargs: {
            "ads.csv": incoming_dir / "ads.csv",
            "ads_attributes.csv": incoming_dir / "ads_attributes.csv",
        },
    )

    report = source_sync_module.run_source_sync(
        config,
        source="mysql",
        apply=True,
    )
    change_set = json.loads(
        Path(report["incremental"]["change_set_path"]).read_text(encoding="utf-8")
    )

    assert change_set["mode"] == "incremental"
    assert change_set["changed_ids"] == ["1", "3"]
    assert change_set["removed_ids"] == ["2"]
    assert pd.read_csv(current_dir / "ads.csv")["id"].tolist() == [1, 3]
    retained_backups = [
        path for path in (output_root / "backups").iterdir() if path.is_dir()
    ]
    assert len(retained_backups) == 1
    assert retained_backups[0].name not in {
        old_backup_one.name,
        old_backup_two.name,
    }
    assert sorted(report["pruned_backup_directories"]) == sorted(
        [str(old_backup_one), str(old_backup_two)]
    )

    pd.DataFrame([{"id": "100", "name": "old"}]).to_csv(
        current_dir / "categories.csv",
        index=False,
    )
    pd.DataFrame([{"id": "100", "name": "new"}]).to_csv(
        incoming_dir / "categories.csv",
        index=False,
    )
    config_with_lookup = replace(
        config,
        source_sync={
            **config.source_sync,
            "tables": [
                *config.source_sync["tables"],
                {
                    "name": "categories",
                    "db_table": "categories",
                    "filename": "categories.csv",
                    "primary_key": "id",
                },
            ],
        },
        incremental={
            **config.incremental,
            "full_rebuild_tables": ["categories"],
        },
    )
    monkeypatch.setattr(
        source_sync_module,
        "export_database_tables",
        lambda *args, **kwargs: {
            "ads.csv": incoming_dir / "ads.csv",
            "ads_attributes.csv": incoming_dir / "ads_attributes.csv",
            "categories.csv": incoming_dir / "categories.csv",
        },
    )
    full_report = source_sync_module.run_source_sync(
        config_with_lookup,
        source="mysql",
        apply=True,
    )
    full_change_set = json.loads(
        Path(full_report["incremental"]["change_set_path"]).read_text(
            encoding="utf-8"
        )
    )

    assert full_change_set["mode"] == "full"
    assert full_change_set["invalidating_tables"] == ["categories"]


def _write_flat_profile(tmp_path: Path, slug: str, output_root: Path) -> Path:
    companies = tmp_path / "configs" / "companies"
    companies.mkdir(parents=True, exist_ok=True)
    profile = companies / f"{slug}.yaml"
    profile.write_text(
        f"""
company:
  id: {slug}
  adapter: flat_catalog
  artifact_prefix: catalog
paths:
  input_dir: {tmp_path / "data" / slug}
  data_dir: {tmp_path / "data" / slug}
  output_root: {output_root}
  intermediate_dir: {output_root / "intermediate"}
  final_dir: {output_root / "final"}
  reports_dir: {output_root / "reports"}
  diagnostics_dir: {output_root / "diagnostics"}
source:
  backend: csv
adapter_config:
  filename: products.csv
  extra_columns:
    - source_reference
  column_map:
    id: product_code
    title: product_name
    description: details
    status: availability
search_ready:
  columns:
    - company_id
    - id
    - title
    - description
    - status
    - brand
    - embedding_content
    - bm25_content
    - extras_json
  filter_columns:
    - brand
embedding:
  source_columns: [title, description]
bm25:
  source_columns: [title, description, brand]
credentials:
  env_file: {tmp_path / f".env.{slug}"}
destination:
  backend: mysql
  database_env: {slug.upper()}_MYSQL_DATABASE
  user_env: {slug.upper()}_MYSQL_USER
  password_env: {slug.upper()}_MYSQL_PASSWORD
  table: search_ready
""",
        encoding="utf-8",
    )
    return companies


def test_company_profile_loading_and_slug_validation(tmp_path: Path) -> None:
    companies = _write_flat_profile(tmp_path, "acme", tmp_path / "output" / "acme")
    profiles = discover_company_profiles(companies)
    config = load_company_config("acme", companies_dir=companies)

    assert profiles == {"acme": companies / "acme.yaml"}
    assert config.company_id == "acme"
    assert config.adapter == "flat_catalog"
    assert resolve_source_backend(config, "configured") == "csv"
    assert config.output.root == tmp_path / "output" / "acme"
    with pytest.raises(ValueError, match="Unsafe company slug"):
        validate_company_slug("../acme")


def test_gainr_profile_selects_mysql_source() -> None:
    config = load_company_config("gainr", companies_dir=PROJECT_ROOT / "configs" / "companies")

    assert config.source["backend"] == "mysql"
    assert config.incremental["record_table"] == "ads"
    assert config.incremental["dependent_tables"] == {
        "ads_attributes": "ads_id"
    }
    assert resolve_source_backend(config, "configured") == "mysql"
    assert resolve_source_backend(config, "postgres") == "postgres"


def test_postgres_source_uses_profile_credentials_and_qualified_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    companies = _write_flat_profile(tmp_path, "acme", tmp_path / "output" / "acme")
    config = load_company_config("acme", companies_dir=companies)
    object.__setattr__(
        config,
        "source",
        {
            "backend": "postgres",
            "schema": "inventory",
            "host_env": "ACME_SOURCE_HOST",
            "port_env": "ACME_SOURCE_PORT",
            "database_env": "ACME_SOURCE_DATABASE",
            "user_env": "ACME_SOURCE_USER",
            "password_env": "ACME_SOURCE_PASSWORD",
        },
    )
    env_file = tmp_path / ".env.source"
    env_file.write_text(
        "\n".join(
            [
                "ACME_SOURCE_HOST=postgres.internal",
                "ACME_SOURCE_PORT=5433",
                "ACME_SOURCE_DATABASE=acme catalog",
                "ACME_SOURCE_USER=source user",
                "ACME_SOURCE_PASSWORD=source pass",
            ]
        ),
        encoding="utf-8",
    )
    for name in [
        "ACME_SOURCE_HOST",
        "ACME_SOURCE_PORT",
        "ACME_SOURCE_DATABASE",
        "ACME_SOURCE_USER",
        "ACME_SOURCE_PASSWORD",
    ]:
        monkeypatch.delenv(name, raising=False)

    url = database_url(config, "postgres", env_file=env_file)
    qualified = qualified_table_name(
        {"db_table": "inventory_items"},
        "postgres",
        default_schema=config.source["schema"],
    )

    assert url == (
        "postgresql+psycopg://source+user:source+pass@"
        "postgres.internal:5433/acme+catalog"
    )
    assert qualified == '"inventory"."inventory_items"'


def test_flat_catalog_adapter_emits_canonical_isolated_artifacts(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / "acme"
    data_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "product_code": "P-1",
                "product_name": "Cordless Drill",
                "details": "18V drill",
                "availability": "active",
                "brand": "Example",
                "source_reference": "SRC-99",
                "private_note": "must not be retained",
            }
        ]
    ).to_csv(data_dir / "products.csv", index=False)
    output_root = tmp_path / "output" / "acme"
    companies = _write_flat_profile(tmp_path, "acme", output_root)
    config = load_company_config("acme", companies_dir=companies)

    report = get_adapter(config.adapter).normalize(config, no_csv=True)
    build_retrieval_content(config, no_csv=True)
    build_search_ready(config, no_csv=True)
    verification = run_final_verification(config)
    normalized = pd.read_parquet(output_root / "intermediate" / "catalog_stage_03_attributes_enriched.parquet")
    final = pd.read_parquet(output_root / "final" / "catalog_search_ready.parquet")

    assert report["normalization"]["output_rows"] == 1
    assert normalized.loc[0, "company_id"] == "acme"
    assert normalized.loc[0, "id"] == "P-1"
    assert normalized.loc[0, "brand"] == "Example"
    assert "source_reference" in normalized.loc[0, "extras_json"]
    assert "private_note" not in normalized.loc[0, "extras_json"]
    assert final.loc[0, "id"] == "P-1"
    assert final.loc[0, "embedding_content"] == "Title: Cordless Drill Description: 18V drill"
    assert verification["status"] == "PASS"
    assert not (output_root / "intermediate" / "catalog_stage_03_attributes_enriched.csv").exists()
    assert not (tmp_path / "output" / "gainr").exists()


def test_company_credentials_are_resolved_from_separate_files(tmp_path: Path, monkeypatch) -> None:
    companies = _write_flat_profile(tmp_path, "acme", tmp_path / "output" / "acme")
    env_file = tmp_path / ".env.acme"
    env_file.write_text(
        "ACME_MYSQL_DATABASE=acme_db\nACME_MYSQL_USER=acme_user\nACME_MYSQL_PASSWORD=secret\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("ACME_MYSQL_DATABASE", raising=False)
    config = load_company_config("acme", companies_dir=companies)

    assert credential_value(config, "database_env", "MYSQL_DATABASE") == "acme_db"
    assert credential_value(config, "user_env", "MYSQL_USER") == "acme_user"


def test_publish_dry_run_validates_without_database_connection(tmp_path: Path) -> None:
    companies = _write_flat_profile(tmp_path, "acme", tmp_path / "output" / "acme")
    (tmp_path / ".env.acme").write_text(
        "ACME_MYSQL_DATABASE=acme_db\nACME_MYSQL_USER=acme_user\nACME_MYSQL_PASSWORD=secret\n",
        encoding="utf-8",
    )
    config = load_company_config("acme", companies_dir=companies)
    config.output.final.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "company_id": "acme",
                "id": "P-1",
                "title": "Drill",
                "description": "18V",
                "embedding_content": "Title: Drill",
                "bm25_content": "Drill 18V",
                "extras_json": "{}",
            }
        ]
    ).to_parquet(config.output.final / "catalog_search_ready.parquet", index=False)

    report = publish_company(config, dry_run=True)

    assert report["published"] is False
    assert report["validation"]["rows"] == 1
    assert validate_publish_file(
        config,
        config.output.final / "catalog_search_ready.parquet",
    ) == report["validation"]

    invalid = pd.DataFrame(
        [
            {
                "company_id": "another-company",
                "id": "P-1",
                "title": "Drill",
                "description": "18V",
                "embedding_content": "",
                "bm25_content": "Drill",
                "extras_json": "{}",
            }
        ]
    )
    with pytest.raises(ValueError, match="Cannot publish invalid data"):
        validate_publish_frame(config, invalid)


def test_isolation_rejects_shared_output_or_destination(tmp_path: Path) -> None:
    companies = _write_flat_profile(tmp_path, "acme", tmp_path / "shared")
    first = load_company_config("acme", companies_dir=companies)
    second = load_company_config("acme", companies_dir=companies)
    object.__setattr__(second, "company_id", "other")

    with pytest.raises(ValueError, match="share output root"):
        validate_company_isolation([first, second])


def test_mysql_publish_uses_one_atomic_swap_statement(monkeypatch) -> None:
    statements: list[str] = []

    class Result:
        def scalar_one(self) -> int:
            return 1

    class Connection:
        def execute(self, statement: object) -> Result:
            statements.append(str(statement))
            return Result()

    class Inspector:
        def has_table(self, table: str) -> bool:
            return table == "search_ready"

    monkeypatch.setattr("sqlalchemy.inspect", lambda connection: Inspector())
    monkeypatch.setattr(pd.DataFrame, "to_sql", lambda self, *args, **kwargs: None)
    frame = pd.DataFrame([{"id": "P-1"}])

    _mysql_publish(
        Connection(),
        frame,
        "search_ready",
        "search_ready__staging_1234",
        "search_ready__backup_1234",
    )

    rename = [statement for statement in statements if statement.startswith("RENAME TABLE")]
    assert rename == [
        "RENAME TABLE `search_ready` TO `search_ready__backup_1234`, "
        "`search_ready__staging_1234` TO `search_ready`"
    ]
    assert statements[-1] == "DROP TABLE `search_ready__backup_1234`"


def test_batch_continues_after_company_failure(tmp_path: Path, monkeypatch) -> None:
    first = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    second = load_config(PROJECT_ROOT / "configs/pipeline.yaml")
    object.__setattr__(first, "company_id", "first")
    object.__setattr__(second, "company_id", "second")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("rag_ht_pipeline.pipeline.selected_configs", lambda args: [first, second])

    def fake_run(args: Namespace, config: object) -> dict[str, object]:
        if config.company_id == "first":
            raise ValueError("bad source")
        return {"validate": {"status": "PASS"}}

    monkeypatch.setattr("rag_ht_pipeline.pipeline.run_company_pipeline", fake_run)
    result = run_batch(Namespace())

    assert result["status"] == "FAIL"
    assert result["companies"]["first"]["status"] == "FAIL"
    assert result["companies"]["second"]["status"] == "PASS"
    assert (tmp_path / "output" / "reports" / "company_batch_report.json").exists()
