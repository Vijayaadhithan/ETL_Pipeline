from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd

from .config import DEFAULT_CONFIG_PATH, PipelineConfig, load_config
from .mysql_loader import mysql_url_from_env
from .postgres_loader import load_env_file
from .source_sync import find_current_file, source_tables
from .stage1_category import NULL_VALUES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load raw source CSV snapshots into MySQL/MariaDB source tables.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Pipeline YAML config.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="replace")
    parser.add_argument("--chunksize", type=int, default=2000)
    parser.add_argument("--sample-size", type=int, default=None, help="Load only the first N rows per CSV.")
    parser.add_argument("--dry-run", action="store_true", help="Inspect source CSVs without writing to MySQL.")
    parser.add_argument(
        "--create-database",
        action="store_true",
        help="Create MYSQL_DATABASE first if it does not exist. Requires credentials with create permission.",
    )
    return parser.parse_args()


def read_source_csv(path: Path, *, sample_size: int | None = None) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype="string",
        keep_default_na=True,
        na_values=NULL_VALUES,
        nrows=sample_size,
        low_memory=False,
    )


def mysql_server_url_from_env() -> str:
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    user = os.environ.get("MYSQL_USER")
    password = os.environ.get("MYSQL_PASSWORD")
    if not user or not password:
        raise RuntimeError("MYSQL_USER and MYSQL_PASSWORD must be set in .env or the environment.")
    return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/?charset=utf8mb4"


def create_database_if_needed() -> None:
    database = os.environ.get("MYSQL_DATABASE")
    if not database:
        raise RuntimeError("MYSQL_DATABASE must be set in .env or the environment.")
    if not database.replace("_", "").isalnum():
        raise ValueError(f"Unsafe MYSQL_DATABASE value: {database}")

    try:
        from sqlalchemy import create_engine, text
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and PyMySQL are required. Install requirements.txt first.") from exc

    engine = create_engine(mysql_server_url_from_env())
    with engine.begin() as connection:
        connection.execute(
            text(f"CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        )


def source_dtype_map(df: pd.DataFrame) -> dict[str, Any]:
    try:
        from sqlalchemy.dialects.mysql import LONGTEXT
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and PyMySQL are required. Install requirements.txt first.") from exc
    return {column: LONGTEXT(charset="utf8mb4", collation="utf8mb4_unicode_ci") for column in df.columns}


def load_sources_to_mysql(
    config: PipelineConfig,
    *,
    if_exists: str = "replace",
    chunksize: int = 2000,
    sample_size: int | None = None,
    dry_run: bool = False,
    create_database: bool = False,
) -> dict[str, Any]:
    if create_database and not dry_run:
        create_database_if_needed()

    result: dict[str, Any] = {
        "dry_run": bool(dry_run),
        "if_exists": if_exists,
        "sample_size": sample_size,
        "tables": {},
    }
    loaded_frames: list[tuple[str, pd.DataFrame]] = []
    for table in source_tables(config):
        filename = table["filename"]
        db_table = table["db_table"]
        path = find_current_file(config, filename)
        if path is None:
            result["tables"][db_table] = {
                "filename": filename,
                "status": "missing_csv",
                "rows": 0,
                "columns": 0,
            }
            continue
        df = read_source_csv(path, sample_size=sample_size)
        result["tables"][db_table] = {
            "filename": filename,
            "path": str(path),
            "status": "ready" if dry_run else "loaded",
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
        }
        loaded_frames.append((db_table, df))

    if dry_run:
        return result

    try:
        from sqlalchemy import create_engine, text
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and PyMySQL are required. Install requirements.txt first.") from exc

    engine = create_engine(mysql_url_from_env())
    with engine.begin() as connection:
        connection.execute(text("SET NAMES utf8mb4"))
        for db_table, df in loaded_frames:
            df.to_sql(
                db_table,
                connection,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi",
                dtype=source_dtype_map(df),
            )
    return result


def print_summary(result: dict[str, Any]) -> None:
    print("MySQL source load dry run complete" if result["dry_run"] else "MySQL source load complete")
    print(f"if_exists: {result['if_exists']}")
    if result.get("sample_size") is not None:
        print(f"sample_size: {result['sample_size']}")
    for table_name, table in result["tables"].items():
        print(f"{table_name}: {table['status']}, rows={table['rows']}, columns={table['columns']}")


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)
    config = load_config(args.config)
    result = load_sources_to_mysql(
        config,
        if_exists=args.if_exists,
        chunksize=args.chunksize,
        sample_size=args.sample_size,
        dry_run=args.dry_run,
        create_database=args.create_database,
    )
    print_summary(result)


if __name__ == "__main__":
    main()
