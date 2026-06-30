from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd

from .config import DEFAULT_CONFIG_PATH, PipelineConfig, ensure_output_dirs, load_config
from .credentials import resolve_env_value
from .stage1_category import NULL_VALUES


LOGGER = logging.getLogger("rag_ht_pipeline.source_sync")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh or inspect pipeline source CSV snapshots.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Pipeline YAML config.")
    parser.add_argument("--source", choices=["configured", "csv", "mysql", "postgres"], default="csv")
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
    report, _ = compare_snapshot_changes(
        current,
        incoming,
        primary_key=primary_key,
        sample_limit=sample_limit,
    )
    return report


def compare_snapshot_changes(
    current: pd.DataFrame | None,
    incoming: pd.DataFrame,
    *,
    primary_key: str,
    sample_limit: int = 20,
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    current_rows = 0 if current is None else len(current)
    incoming_rows = len(incoming)
    changes = {"added": set(), "removed": set(), "updated": set()}
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
        if primary_key in incoming.columns:
            changes["added"] = {
                key for key in incoming[primary_key].map(clean_key) if key
            }
            report["sample_added_keys"] = sorted(changes["added"])[:sample_limit]
        return report, changes
    if primary_key not in incoming.columns or primary_key not in current.columns:
        return report, changes

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
    changes = {
        "added": set(added),
        "removed": set(removed),
        "updated": set(updated),
    }

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
    return report, changes


def related_record_ids(
    frame: pd.DataFrame | None,
    *,
    primary_key: str,
    row_keys: set[str],
    record_key: str,
) -> set[str]:
    if frame is None or not row_keys:
        return set()
    if primary_key not in frame.columns or record_key not in frame.columns:
        return set()
    normalized_keys = frame[primary_key].map(clean_key)
    values = frame.loc[normalized_keys.isin(row_keys), record_key].map(clean_key)
    return {value for value in values if value}


def source_tables(config: PipelineConfig) -> list[dict[str, str]]:
    tables = config.source_sync.get("tables", [])
    if not tables:
        raise RuntimeError(
            f"No source_sync.tables entries found for company {config.company_id!r} "
            f"in {config.config_path}."
        )
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


def configured_source_backend(config: PipelineConfig) -> str:
    backend = str(config.source.get("backend", "csv")).lower()
    if backend not in {"csv", "mysql", "postgres"}:
        raise ValueError(
            f"Unsupported source backend {backend!r} for company {config.company_id!r}; "
            "expected csv, mysql, or postgres."
        )
    return backend


def resolve_source_backend(config: PipelineConfig, requested: str) -> str:
    return configured_source_backend(config) if requested == "configured" else requested


def source_connection_settings(config: PipelineConfig, source: str) -> dict[str, Any]:
    legacy = config.mysql if source == "mysql" else config.postgres
    configured = config.source if configured_source_backend(config) == source else {}
    return {**legacy, **configured}


def database_url(config: PipelineConfig, source: str, *, env_file: Path = Path(".env")) -> str:
    settings = source_connection_settings(config, source)
    context = f"{config.company_id!r} {source} source"
    if source == "mysql":
        host = resolve_env_value(
            str(settings.get("host_env", "MYSQL_HOST")),
            env_file=env_file,
            default="localhost",
            context=context,
        )
        port = resolve_env_value(
            str(settings.get("port_env", "MYSQL_PORT")),
            env_file=env_file,
            default="3306",
            context=context,
        )
        database = resolve_env_value(
            str(settings.get("database_env", "MYSQL_DATABASE")),
            env_file=env_file,
            default=settings.get("database"),
            context=context,
        )
        user = resolve_env_value(
            str(settings.get("user_env", "MYSQL_USER")),
            env_file=env_file,
            context=context,
        )
        password = resolve_env_value(
            str(settings.get("password_env", "MYSQL_PASSWORD")),
            env_file=env_file,
            context=context,
        )
        return (
            f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@"
            f"{host}:{port}/{quote_plus(database)}"
        )
    if source == "postgres":
        host = resolve_env_value(
            str(settings.get("host_env", "POSTGRES_HOST")),
            env_file=env_file,
            default="localhost",
            context=context,
        )
        port = resolve_env_value(
            str(settings.get("port_env", "POSTGRES_PORT")),
            env_file=env_file,
            default="5432",
            context=context,
        )
        database = resolve_env_value(
            str(settings.get("database_env", "POSTGRES_DATABASE")),
            env_file=env_file,
            default=settings.get("database", "rag_ht"),
            context=context,
        )
        user = resolve_env_value(
            str(settings.get("user_env", "POSTGRES_USER")),
            env_file=env_file,
            context=context,
        )
        password = resolve_env_value(
            str(settings.get("password_env", "POSTGRES_PASSWORD")),
            env_file=env_file,
            context=context,
        )
        return (
            f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}@"
            f"{host}:{port}/{quote_plus(database)}"
        )
    raise ValueError(f"Unsupported database source: {source}")


def quote_identifier(identifier: str, source: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"Unsafe table identifier: {identifier}")
    quote = "`" if source == "mysql" else '"'
    return f"{quote}{identifier}{quote}"


def qualified_table_name(
    table: dict[str, str],
    source: str,
    *,
    default_schema: str | None = None,
) -> str:
    db_table = table["db_table"]
    schema = table.get("db_schema") or default_schema
    quoted_table = quote_identifier(db_table, source)
    if not schema:
        return quoted_table
    return f"{quote_identifier(schema, source)}.{quoted_table}"


def export_database_tables(
    config: PipelineConfig,
    *,
    source: str,
    staging_dir: Path,
    env_file: Path = Path(".env"),
    sample_size: int | None = None,
) -> dict[str, Path]:
    try:
        from sqlalchemy import create_engine
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy is required for database source sync. Install requirements.txt first.") from exc

    engine = create_engine(database_url(config, source, env_file=env_file))
    exported: dict[str, Path] = {}
    staging_dir.mkdir(parents=True, exist_ok=True)
    limit_clause = f" LIMIT {int(sample_size)}" if sample_size is not None else ""
    default_schema = str(config.source.get("schema", "")).strip() or None
    for table in source_tables(config):
        db_table = table["db_table"]
        filename = table["filename"]
        qualified_table = qualified_table_name(table, source, default_schema=default_schema)
        query = f"SELECT * FROM {qualified_table}{limit_clause}"
        LOGGER.info("Exporting %s to %s", qualified_table, filename)
        df = pd.read_sql_query(query, engine)
        path = staging_dir / filename
        df.to_csv(path, index=False)
        exported[filename] = path
    return exported


def prune_backup_runs(
    backup_dir: Path,
    *,
    current_backup: Path,
    retention: int,
) -> list[str]:
    if retention < 1:
        raise ValueError("source_sync.backup_retention must be at least 1.")
    if not current_backup.exists():
        return []
    runs = sorted(
        (
            path
            for path in backup_dir.iterdir()
            if path.is_dir() and not path.is_symlink()
        ),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    keep = {current_backup}
    for path in runs:
        if len(keep) >= retention:
            break
        keep.add(path)
    removed: list[str] = []
    for path in runs:
        if path in keep:
            continue
        shutil.rmtree(path)
        removed.append(str(path))
    return removed


def backup_and_apply(
    config: PipelineConfig,
    exported: dict[str, Path],
    backup_dir: Path,
    *,
    retention: int = 1,
) -> tuple[list[dict[str, str]], list[str]]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
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
    pruned = prune_backup_runs(
        backup_dir,
        current_backup=backup_root,
        retention=retention,
    )
    return actions, pruned


def write_reports(config: PipelineConfig, report: dict[str, Any]) -> None:
    config.output.reports.mkdir(parents=True, exist_ok=True)
    json_path = config.output.reports / "source_sync_report.json"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    rows = []
    for table_name, table_report in report.get("tables", {}).items():
        rows.append(
            {
                "table": table_name,
                "db_schema": table_report.get("db_schema"),
                "db_table": table_report.get("db_table"),
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


def write_incremental_change_set(
    config: PipelineConfig,
    *,
    source: str,
    applied: bool,
    mode: str,
    changed_ids: set[str],
    removed_ids: set[str],
    invalidating_tables: list[str],
    reason: str,
) -> tuple[Path, dict[str, Any]]:
    path = config.output.reports / "incremental_change_set.json"
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "company_id": config.company_id,
        "source": source,
        "applied": applied,
        "mode": mode,
        "reason": reason,
        "changed_ids": sorted(changed_ids),
        "removed_ids": sorted(removed_ids),
        "changed_id_count": len(changed_ids),
        "removed_id_count": len(removed_ids),
        "invalidating_tables": invalidating_tables,
    }
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path, {
        key: value
        for key, value in payload.items()
        if key not in {"changed_ids", "removed_ids"}
    } | {"change_set_path": str(path)}


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
    source = resolve_source_backend(config, source)
    staging_dir = staging_dir or configured_path(config, "staging_dir", "output/source_sync/latest")
    backup_dir = configured_path(config, "backup_dir", "output/source_sync/backups")

    exported: dict[str, Path] = {}
    if source in {"mysql", "postgres"}:
        exported = export_database_tables(
            config,
            source=source,
            staging_dir=staging_dir,
            env_file=env_file,
            sample_size=sample_size,
        )

    report: dict[str, Any] = {
        "source": source,
        "source_schema": config.source.get("schema", ""),
        "applied": bool(apply),
        "sample_size": sample_size,
        "staging_dir": str(staging_dir),
        "tables": {},
        "apply_actions": [],
        "backup_retention": int(config.source_sync.get("backup_retention", 1)),
        "pruned_backup_directories": [],
    }
    incremental = config.incremental
    record_table = str(incremental.get("record_table", "")).strip()
    record_key = str(incremental.get("record_key", "id")).strip()
    dependent_tables = {
        str(table): str(parent_key)
        for table, parent_key in dict(incremental.get("dependent_tables", {})).items()
    }
    full_rebuild_tables = {
        str(table) for table in incremental.get("full_rebuild_tables", [])
    }
    changed_ids: set[str] = set()
    removed_ids: set[str] = set()
    invalidating_tables: set[str] = set()
    missing_previous_snapshot = False
    for table in source_tables(config):
        name = table["name"]
        filename = table["filename"]
        primary_key = table.get("primary_key", "id")
        current_path = find_current_file(config, filename)
        incoming_path = exported.get(filename) if exported else current_path

        table_report: dict[str, Any] = {
            "filename": filename,
            "db_table": table.get("db_table"),
            "db_schema": table.get("db_schema") or config.source.get("schema", ""),
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
        comparison, changes = compare_snapshot_changes(
            current,
            incoming,
            primary_key=primary_key,
        )
        table_report.update(comparison)
        table_report["status"] = "ok"
        report["tables"][name] = table_report

        table_changed = any(changes.values())
        if not table_changed:
            continue
        if current is None:
            missing_previous_snapshot = True
        if (
            comparison["current_duplicate_key_rows"]
            or comparison["incoming_duplicate_key_rows"]
            or not comparison["primary_key_available"]
        ):
            invalidating_tables.add(name)
            continue
        if name == record_table:
            if primary_key != record_key:
                invalidating_tables.add(name)
                continue
            changed_ids.update(changes["added"] | changes["updated"])
            removed_ids.update(changes["removed"])
            continue
        if name in dependent_tables:
            parent_key = dependent_tables[name]
            changed_ids.update(
                related_record_ids(
                    incoming,
                    primary_key=primary_key,
                    row_keys=changes["added"] | changes["updated"],
                    record_key=parent_key,
                )
            )
            changed_ids.update(
                related_record_ids(
                    current,
                    primary_key=primary_key,
                    row_keys=changes["removed"] | changes["updated"],
                    record_key=parent_key,
                )
            )
            continue
        if name in full_rebuild_tables or name not in dependent_tables:
            invalidating_tables.add(name)

    if apply and exported:
        actions, pruned = backup_and_apply(
            config,
            exported,
            backup_dir,
            retention=report["backup_retention"],
        )
        report["apply_actions"] = actions
        report["pruned_backup_directories"] = pruned
        if pruned:
            LOGGER.info(
                "Pruned %s old source snapshot backup director%s; retaining %s.",
                len(pruned),
                "y" if len(pruned) == 1 else "ies",
                report["backup_retention"],
            )
    elif apply and not exported:
        report["apply_actions"] = []
        report["apply_note"] = "No files were replaced because source=csv uses the existing local snapshots."

    incremental_available = bool(incremental) and source in {"mysql", "postgres"} and apply and sample_size is None
    if not incremental_available:
        mode = "unavailable"
        reason = (
            "Incremental detection requires profile settings, a full database refresh, "
            "--apply-source-refresh, and no --sample-size."
        )
    elif missing_previous_snapshot:
        mode = "full"
        reason = "At least one changed table has no previous snapshot."
    elif invalidating_tables:
        mode = "full"
        reason = "Shared lookup/catalog changes can affect records beyond directly changed ads."
    elif changed_ids or removed_ids:
        mode = "incremental"
        reason = "Only record rows or configured dependent rows changed."
    else:
        mode = "no_changes"
        reason = "No source rows changed."
    changed_ids.difference_update(removed_ids)
    change_set_path, incremental_summary = write_incremental_change_set(
        config,
        source=source,
        applied=bool(apply),
        mode=mode,
        changed_ids=changed_ids,
        removed_ids=removed_ids,
        invalidating_tables=sorted(invalidating_tables),
        reason=reason,
    )
    report["incremental"] = incremental_summary
    report["incremental"]["change_set_path"] = str(change_set_path)
    write_reports(config, report)
    return report


def print_summary(report: dict[str, Any]) -> None:
    print("\nSource sync complete")
    print(f"Source: {report['source']}")
    print(f"Applied: {report['applied']}")
    print(
        f"Backup retention: {report.get('backup_retention', 1)}, "
        f"pruned={len(report.get('pruned_backup_directories', []))}"
    )
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
