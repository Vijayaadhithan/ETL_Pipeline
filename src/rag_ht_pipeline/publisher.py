from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from uuid import uuid4

import pandas as pd

from .config import PipelineConfig
from .credentials import resolve_env_value
from .mysql_loader import mysql_dtype_map
from .stage3_attributes import clean


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
CANONICAL_REQUIRED = {
    "company_id",
    "id",
    "title",
    "description",
    "embedding_content",
    "bm25_content",
    "extras_json",
}
LOGGER = logging.getLogger("rag_ht_pipeline.publisher")


def safe_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def credential_value(
    config: PipelineConfig,
    key: str,
    default_env: str,
    *,
    default: str | None = None,
    required: bool = True,
) -> str:
    env_name = str(config.destination.get(key, default_env))
    env_file_value = config.credentials.get("env_file", ".env")
    env_file = Path(env_file_value)
    if not env_file.is_absolute():
        env_file = config.project_root / env_file
    return resolve_env_value(
        env_name,
        env_file=env_file,
        default=default,
        required=required,
        context=f"company {config.company_id!r}",
    )


def destination_url(config: PipelineConfig) -> tuple[str, str]:
    backend = str(config.destination.get("backend", "mysql")).lower()
    if backend == "mysql":
        host = credential_value(config, "host_env", "MYSQL_HOST", default="localhost")
        port = credential_value(config, "port_env", "MYSQL_PORT", default="3306")
        database = credential_value(config, "database_env", "MYSQL_DATABASE")
        user = credential_value(config, "user_env", "MYSQL_USER")
        password = credential_value(config, "password_env", "MYSQL_PASSWORD")
        return (
            backend,
            f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@"
            f"{host}:{port}/{quote_plus(database)}?charset=utf8mb4",
        )
    if backend == "postgres":
        host = credential_value(config, "host_env", "POSTGRES_HOST", default="localhost")
        port = credential_value(config, "port_env", "POSTGRES_PORT", default="5432")
        database = credential_value(config, "database_env", "POSTGRES_DATABASE")
        user = credential_value(config, "user_env", "POSTGRES_USER")
        password = credential_value(config, "password_env", "POSTGRES_PASSWORD")
        return (
            backend,
            f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}@"
            f"{host}:{port}/{quote_plus(database)}",
        )
    raise ValueError(f"Unsupported destination backend {backend!r} for company {config.company_id!r}.")


def validate_publish_frame(config: PipelineConfig, df: pd.DataFrame) -> dict[str, Any]:
    missing = sorted(CANONICAL_REQUIRED - set(df.columns))
    if missing:
        raise ValueError(f"Cannot publish; canonical columns are missing: {missing}")
    duplicate_ids = int(df["id"].duplicated(keep=False).sum())
    wrong_company = int(df["company_id"].map(clean).ne(config.company_id).sum())
    empty_embedding = int(df["embedding_content"].map(clean).eq("").sum())
    empty_bm25 = int(df["bm25_content"].map(clean).eq("").sum())
    if duplicate_ids or wrong_company or empty_embedding or empty_bm25:
        raise ValueError(
            "Cannot publish invalid data: "
            f"duplicate_ids={duplicate_ids}, wrong_company={wrong_company}, "
            f"empty_embedding={empty_embedding}, empty_bm25={empty_bm25}"
        )
    return {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "duplicate_ids": duplicate_ids,
        "wrong_company_rows": wrong_company,
        "empty_embedding_rows": empty_embedding,
        "empty_bm25_rows": empty_bm25,
    }


def _mysql_publish(connection: Any, df: pd.DataFrame, table: str, staging: str, backup: str) -> None:
    from sqlalchemy import inspect, text

    df.to_sql(
        staging,
        connection,
        if_exists="fail",
        index=False,
        chunksize=2000,
        method="multi",
        dtype=mysql_dtype_map(df),
    )
    loaded = connection.execute(text(f"SELECT COUNT(*) FROM `{staging}`")).scalar_one()
    if loaded != len(df):
        raise RuntimeError(f"Staging row-count mismatch: expected {len(df)}, loaded {loaded}")
    if inspect(connection).has_table(table):
        connection.execute(text(f"RENAME TABLE `{table}` TO `{backup}`, `{staging}` TO `{table}`"))
        try:
            connection.execute(text(f"DROP TABLE `{backup}`"))
        except Exception:
            LOGGER.warning("Published %s but could not remove backup table %s", table, backup, exc_info=True)
    else:
        connection.execute(text(f"RENAME TABLE `{staging}` TO `{table}`"))


def _postgres_publish(
    connection: Any,
    df: pd.DataFrame,
    table: str,
    staging: str,
    backup: str,
    schema: str,
) -> None:
    from sqlalchemy import inspect, text

    df.to_sql(
        staging,
        connection,
        schema=schema,
        if_exists="fail",
        index=False,
        chunksize=5000,
        method="multi",
    )
    loaded = connection.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{staging}"')).scalar_one()
    if loaded != len(df):
        raise RuntimeError(f"Staging row-count mismatch: expected {len(df)}, loaded {loaded}")
    if inspect(connection).has_table(table, schema=schema):
        connection.execute(text(f'ALTER TABLE "{schema}"."{table}" RENAME TO "{backup}"'))
        connection.execute(text(f'ALTER TABLE "{schema}"."{staging}" RENAME TO "{table}"'))
        connection.execute(text(f'DROP TABLE "{schema}"."{backup}"'))
    else:
        connection.execute(text(f'ALTER TABLE "{schema}"."{staging}" RENAME TO "{table}"'))


def publish_company(config: PipelineConfig, *, dry_run: bool = False) -> dict[str, Any]:
    input_path = config.output.final / f"{config.artifact_prefix}_search_ready.parquet"
    if not input_path.exists():
        raise FileNotFoundError(f"Missing validated search-ready artifact: {input_path}")
    df = pd.read_parquet(input_path)
    validation = validate_publish_frame(config, df)
    table = safe_identifier(str(config.destination.get("table", "search_ready")), "destination table")
    schema = safe_identifier(str(config.destination.get("schema", "public")), "destination schema")
    report: dict[str, Any] = {
        "company_id": config.company_id,
        "input_file": str(input_path),
        "destination_backend": str(config.destination.get("backend", "mysql")),
        "destination_table": table,
        "destination_schema": schema,
        "dry_run": bool(dry_run),
        "validation": validation,
        "published": False,
        "published_at": "",
    }
    backend, url = destination_url(config)
    if dry_run:
        return report

    try:
        from sqlalchemy import create_engine
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and the destination database driver are required for publishing.") from exc

    suffix = uuid4().hex[:8]
    staging = safe_identifier(f"{table[:45]}__staging_{suffix}", "staging table")
    backup = safe_identifier(f"{table[:46]}__backup_{suffix}", "backup table")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            if backend == "mysql":
                _mysql_publish(connection, df, table, staging, backup)
            else:
                _postgres_publish(connection, df, table, staging, backup, schema)
    except Exception:
        try:
            from sqlalchemy import text

            with engine.begin() as connection:
                qualified = f"`{staging}`" if backend == "mysql" else f'"{schema}"."{staging}"'
                connection.execute(text(f"DROP TABLE IF EXISTS {qualified}"))
        except Exception:
            pass
        raise
    report["published"] = True
    report["published_at"] = datetime.now(timezone.utc).isoformat()
    config.output.reports.mkdir(parents=True, exist_ok=True)
    (config.output.reports / "publish_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return report
