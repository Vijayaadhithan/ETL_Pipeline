from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd

from .config import DEFAULT_CONFIG_PATH, PipelineConfig, ensure_output_dirs, load_config
from .postgres_loader import load_env_file
from .stage1_category import NULL_VALUES


LOGGER = logging.getLogger("rag_ht_pipeline.source_sync")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh or inspect pipeline source CSV snapshots.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Pipeline YAML config.")
    parser.add_argument("--source", choices=["csv", "mysql", "postgres"], default="csv")
    parser.add_argument("--apply", action="store_true", help="Replace local source CSVs with the exported snapshot.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--staging-dir", type=Path, default=None)
    parser.add_argument("--sample-size", type=int, default=None, help="Limit rows per source table for a dry run.")
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")


def read_source_csv(path: Path, *, nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=NULL_VALUES, nrows=nrows, low_memory=False)


def clean_key(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def row_hashes(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="string")
    normalized = df[columns].fillna("").astype("string")
    return normalized.apply(
        lambda row: hashlib.sha256("\x1f".join(row.tolist()).encode("utf-8")).hexdigest(),
        axis=1,
    )


def compare_snapshots(
    current: pd.DataFrame | None,
    incoming: pd.DataFrame,
    *,
    primary_key: str,
    sample_limit: int = 20,
) -> dict[str, Any]:
    current_rows = 0 if current is None else len(current)
    incoming_rows = len(incoming)
    report: dict[str, Any] = {
        "current_rows": int(current_rows),
        "incoming_rows": int(incoming_rows),
        "row_delta": int(incoming_rows - current_rows),
        "primary_key": primary_key,
        "primary_key_available": primary_key in incoming.columns and (current is None or primary_key in current.columns),
        "current_duplicate_key_rows": 0,
        "incoming_duplicate_key_rows": 0,
        "added_rows": None,
        "removed_rows": None,
        "updated_rows": None,
        "sample_added_keys": [],
        "sample_removed_keys": [],
        "sample_updated_keys": [],
    }
    if current is None:
        report["added_rows"] = int(incoming_rows)
        return report
    if primary_key not in incoming.columns or primary_key not in current.columns:
        return report

    current_work = current.copy()
    incoming_work = incoming.copy()
    current_work["__sync_key"] = current_work[primary_key].map(clean_key)
    incoming_work["__sync_key"] = incoming_work[primary_key].map(clean_key)
    current_work = current_work[current_work["__sync_key"] != ""]
    incoming_work = incoming_work[incoming_work["__sync_key"] != ""]

    report["current_duplicate_key_rows"] = int(current_work["__sync_key"].duplicated(keep=False).sum())
    report["incoming_duplicate_key_rows"] = int(incoming_work["__sync_key"].duplicated(keep=False).sum())

    current_work = current_work.drop_duplicates("__sync_key", keep="last")
    incoming_work = incoming_work.drop_duplicates("__sync_key", keep="last")
    current_keys = set(current_work["__sync_key"])
    incoming_keys = set(incoming_work["__sync_key"])
    added = sorted(incoming_keys - current_keys)
    removed = sorted(current_keys - incoming_keys)
    common = sorted(current_keys & incoming_keys)

    common_columns = sorted(
        column for column in set(current_work.columns) & set(incoming_work.columns) if column != "__sync_key"
    )
    current_indexed = current_work.set_index("__sync_key")
    incoming_indexed = incoming_work.set_index("__sync_key")
    current_hash = row_hashes(current_indexed.loc[common], common_columns)
    incoming_hash = row_hashes(incoming_indexed.loc[common], common_columns)
    updated = [key for key, old_hash, new_hash in zip(common, current_hash, incoming_hash, strict=False) if old_hash != new_hash]

    report.update(
        {
            "added_rows": int(len(added)),
            "removed_rows": int(len(removed)),
            "updated_rows": int(len(updated)),
            "sample_added_keys": added[:sample_limit],
            "sample_removed_keys": removed[:sample_limit],
            "sample_updated_keys": updated[:sample_limit],
        }
    )
    return report


def source_tables(config: PipelineConfig) -> list[dict[str, str]]:
    tables = config.source_sync.get("tables", [])
    if not tables:
        raise RuntimeError("No source_sync.tables entries found in configs/pipeline.yaml.")
    return [dict(table) for table in tables]


def configured_path(config: PipelineConfig, key: str, default: str) -> Path:
    value = config.source_sync.get(key, default)
    path = Path(value)
    return path if path.is_absolute() else config.project_root / path


def find_current_file(config: PipelineConfig, filename: str) -> Path | None:
    for base in [config.data_dir, config.input_dir, config.project_root]:
        path = base / filename
        if path.exists():
            return path
    return None


def target_file(config: PipelineConfig, filename: str) -> Path:
    current = find_current_file(config, filename)
    if current is not None:
        return current
    return config.input_dir / filename


def env_value(env_name: str, *, default: str | None = None, required: bool = True) -> str:
    value = os.environ.get(env_name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {env_name}")
    return value or ""


def database_url(config: PipelineConfig, source: str) -> str:
    if source == "mysql":
        mysql = config.mysql
        host = env_value(mysql.get("host_env", "MYSQL_HOST"), default="localhost")
        port = env_value(mysql.get("port_env", "MYSQL_PORT"), default="3306", required=False) or "3306"
        database = os.environ.get("MYSQL_DATABASE") or mysql.get("database")
        if not database:
            raise RuntimeError("MYSQL_DATABASE or mysql.database must be configured.")
        user = env_value(mysql.get("user_env", "MYSQL_USER"))
        password = env_value(mysql.get("password_env", "MYSQL_PASSWORD"))
        return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
    if source == "postgres":
        postgres = config.postgres
        host = env_value(postgres.get("host_env", "POSTGRES_HOST"), default="localhost")
        port = env_value(postgres.get("port_env", "POSTGRES_PORT"), default="5432", required=False) or "5432"
        database = env_value(postgres.get("database_env", "POSTGRES_DATABASE"), default="rag_ht")
        user = env_value(postgres.get("user_env", "POSTGRES_USER"))
        password = env_value(postgres.get("password_env", "POSTGRES_PASSWORD"))
        return f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
    raise ValueError(f"Unsupported database source: {source}")


def quote_identifier(identifier: str, source: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"Unsafe table identifier: {identifier}")
    quote = "`" if source == "mysql" else '"'
    return f"{quote}{identifier}{quote}"


def export_database_tables(
    config: PipelineConfig,
    *,
    source: str,
    staging_dir: Path,
    sample_size: int | None = None,
) -> dict[str, Path]:
    try:
        from sqlalchemy import create_engine
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy is required for database source sync. Install requirements.txt first.") from exc

    engine = create_engine(database_url(config, source))
    exported: dict[str, Path] = {}
    staging_dir.mkdir(parents=True, exist_ok=True)
    limit_clause = f" LIMIT {int(sample_size)}" if sample_size is not None else ""
    for table in source_tables(config):
        db_table = table["db_table"]
        filename = table["filename"]
        query = f"SELECT * FROM {quote_identifier(db_table, source)}{limit_clause}"
        LOGGER.info("Exporting %s to %s", db_table, filename)
        df = pd.read_sql_query(query, engine)
        path = staging_dir / filename
        df.to_csv(path, index=False)
        exported[filename] = path
    return exported


def backup_and_apply(config: PipelineConfig, exported: dict[str, Path], backup_dir: Path) -> list[dict[str, str]]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_root = backup_dir / timestamp
    actions: list[dict[str, str]] = []
    for filename, incoming_path in exported.items():
        destination = target_file(config, filename)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            backup_root.mkdir(parents=True, exist_ok=True)
            backup_path = backup_root / filename
            shutil.copy2(destination, backup_path)
        else:
            backup_path = Path("")
        shutil.copy2(incoming_path, destination)
        actions.append(
            {
                "filename": filename,
                "destination": str(destination),
                "backup": str(backup_path) if str(backup_path) else "",
            }
        )
    return actions


def write_reports(config: PipelineConfig, report: dict[str, Any]) -> None:
    config.output.reports.mkdir(parents=True, exist_ok=True)
    json_path = config.output.reports / "source_sync_report.json"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    rows = []
    for table_name, table_report in report.get("tables", {}).items():
        rows.append(
            {
                "table": table_name,
                "filename": table_report.get("filename"),
                "current_rows": table_report.get("current_rows"),
                "incoming_rows": table_report.get("incoming_rows"),
                "row_delta": table_report.get("row_delta"),
                "added_rows": table_report.get("added_rows"),
                "removed_rows": table_report.get("removed_rows"),
                "updated_rows": table_report.get("updated_rows"),
                "current_duplicate_key_rows": table_report.get("current_duplicate_key_rows"),
                "incoming_duplicate_key_rows": table_report.get("incoming_duplicate_key_rows"),
                "status": table_report.get("status"),
            }
        )
    pd.DataFrame(rows).to_csv(config.output.reports / "source_table_changes.csv", index=False)


def run_source_sync(
    config: PipelineConfig,
    *,
    source: str = "csv",
    apply: bool = False,
    env_file: Path = Path(".env"),
    staging_dir: Path | None = None,
    sample_size: int | None = None,
) -> dict[str, Any]:
    ensure_output_dirs(config)
    load_env_file(env_file)
    staging_dir = staging_dir or configured_path(config, "staging_dir", "output/source_sync/latest")
    backup_dir = configured_path(config, "backup_dir", "output/source_sync/backups")

    exported: dict[str, Path] = {}
    if source in {"mysql", "postgres"}:
        exported = export_database_tables(config, source=source, staging_dir=staging_dir, sample_size=sample_size)

    report: dict[str, Any] = {
        "source": source,
        "applied": bool(apply),
        "sample_size": sample_size,
        "staging_dir": str(staging_dir),
        "tables": {},
        "apply_actions": [],
    }
    for table in source_tables(config):
        name = table["name"]
        filename = table["filename"]
        primary_key = table.get("primary_key", "id")
        current_path = find_current_file(config, filename)
        incoming_path = exported.get(filename) if exported else current_path

        table_report: dict[str, Any] = {
            "filename": filename,
            "db_table": table.get("db_table"),
            "current_path": str(current_path) if current_path else "",
            "incoming_path": str(incoming_path) if incoming_path else "",
            "primary_key": primary_key,
        }
        if incoming_path is None or not incoming_path.exists():
            table_report.update({"status": "missing_incoming_source"})
            report["tables"][name] = table_report
            continue
        incoming = read_source_csv(incoming_path, nrows=sample_size if source == "csv" else None)
        current = read_source_csv(current_path, nrows=sample_size) if current_path and current_path.exists() else None
        table_report.update(compare_snapshots(current, incoming, primary_key=primary_key))
        table_report["status"] = "ok"
        report["tables"][name] = table_report

    if apply and exported:
        report["apply_actions"] = backup_and_apply(config, exported, backup_dir)
    elif apply and not exported:
        report["apply_actions"] = []
        report["apply_note"] = "No files were replaced because source=csv uses the existing local snapshots."

    write_reports(config, report)
    return report


def print_summary(report: dict[str, Any]) -> None:
    print("\nSource sync complete")
    print(f"Source: {report['source']}")
    print(f"Applied: {report['applied']}")
    for table_name, table_report in report["tables"].items():
        print(
            f"{table_name}: rows {table_report.get('current_rows')} -> {table_report.get('incoming_rows')}, "
            f"added={table_report.get('added_rows')}, "
            f"updated={table_report.get('updated_rows')}, "
            f"removed={table_report.get('removed_rows')}"
        )
    print("Reports:")
    print("  output/reports/source_sync_report.json")
    print("  output/reports/source_table_changes.csv")


def main() -> None:
    configure_logging()
    args = parse_args()
    config = load_config(args.config)
    report = run_source_sync(
        config,
        source=args.source,
        apply=args.apply,
        env_file=args.env_file,
        staging_dir=args.staging_dir,
        sample_size=args.sample_size,
    )
    print_summary(report)


if __name__ == "__main__":
    main()
