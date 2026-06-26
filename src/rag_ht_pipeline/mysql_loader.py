from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd

from .postgres_loader import load_env_file, read_input


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load the clean search-ready file into MySQL/MariaDB.")
    parser.add_argument("--input-file", type=Path, default=Path("output/final/ads_search_ready.parquet"))
    parser.add_argument("--table", default="ads_search_ready")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="replace")
    parser.add_argument("--chunksize", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true", help="Validate config and input without writing.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def mysql_url_from_env() -> str:
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    database = os.environ.get("MYSQL_DATABASE")
    user = os.environ.get("MYSQL_USER")
    password = os.environ.get("MYSQL_PASSWORD")
    if not database:
        raise RuntimeError("MYSQL_DATABASE must be set in .env or the environment.")
    if not user or not password:
        raise RuntimeError("MYSQL_USER and MYSQL_PASSWORD must be set in .env or the environment.")
    return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}?charset=utf8mb4"


def mysql_dtype_map(df: pd.DataFrame) -> dict[str, Any]:
    try:
        from sqlalchemy.dialects.mysql import BIGINT, DATETIME, DOUBLE, LONGTEXT
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and PyMySQL are required for MySQL loading. Install requirements.txt first.") from exc

    dtype: dict[str, Any] = {}
    for column, pandas_dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(pandas_dtype):
            dtype[column] = BIGINT()
        elif pd.api.types.is_float_dtype(pandas_dtype):
            dtype[column] = DOUBLE()
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            dtype[column] = DATETIME()
        else:
            dtype[column] = LONGTEXT(charset="utf8mb4", collation="utf8mb4_unicode_ci")
    return dtype


def load_to_mysql(args: argparse.Namespace) -> dict[str, Any]:
    load_env_file(args.env_file)
    df = read_input(args.input_file)
    result = {"input_file": str(args.input_file), "rows": int(len(df)), "columns": int(len(df.columns))}
    if args.dry_run:
        return result

    try:
        from sqlalchemy import create_engine, text
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and PyMySQL are required for MySQL loading. Install requirements.txt first.") from exc

    engine = create_engine(mysql_url_from_env())
    with engine.begin() as connection:
        connection.execute(text("SET NAMES utf8mb4"))
        df.to_sql(
            args.table,
            connection,
            if_exists=args.if_exists,
            index=False,
            chunksize=args.chunksize,
            method="multi",
            dtype=mysql_dtype_map(df),
        )
    result.update({"table": args.table, "if_exists": args.if_exists})
    return result


def main() -> None:
    args = parse_args()
    result = load_to_mysql(args)
    print("MySQL load complete" if not args.dry_run else "MySQL dry run complete")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
