from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load the clean search-ready file into PostgreSQL.")
    parser.add_argument("--input-file", type=Path, default=Path("output/final/ads_search_ready.parquet"))
    parser.add_argument("--table", default="ads_search_ready")
    parser.add_argument("--schema", default="public")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="replace")
    parser.add_argument("--chunksize", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true", help="Validate config and input without writing.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def postgres_url_from_env() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DATABASE", "rag_ht")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    if not user or not password:
        raise RuntimeError("POSTGRES_USER and POSTGRES_PASSWORD must be set in .env or the environment.")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def read_input(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def load_to_postgres(args: argparse.Namespace) -> dict[str, Any]:
    load_env_file(args.env_file)
    df = read_input(args.input_file)
    result = {"input_file": str(args.input_file), "rows": int(len(df)), "columns": int(len(df.columns))}
    if args.dry_run:
        return result

    try:
        from sqlalchemy import create_engine
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "SQLAlchemy is required for Postgres loading. Install requirements.txt first."
        ) from exc

    engine = create_engine(postgres_url_from_env())
    with engine.begin() as connection:
        df.to_sql(
            args.table,
            connection,
            schema=args.schema,
            if_exists=args.if_exists,
            index=False,
            chunksize=args.chunksize,
            method="multi",
        )
    result.update({"table": args.table, "schema": args.schema, "if_exists": args.if_exists})
    return result


def main() -> None:
    args = parse_args()
    result = load_to_postgres(args)
    print("Postgres load complete" if not args.dry_run else "Postgres dry run complete")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
