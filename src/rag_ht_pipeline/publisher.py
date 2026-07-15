from __future__ import annotations

import json
import logging
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus
from uuid import uuid4

import pandas as pd
import pyarrow.parquet as pq

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
PUBLISH_BATCH_SIZE = 25_000


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


def iter_parquet_frames(
    path: Path,
    *,
    columns: list[str] | None = None,
    batch_size: int = PUBLISH_BATCH_SIZE,
) -> Iterator[pd.DataFrame]:
    source = pq.ParquetFile(path)
    for batch in source.iter_batches(batch_size=batch_size, columns=columns):
        yield batch.to_pandas()


def validate_publish_file(config: PipelineConfig, path: Path) -> dict[str, Any]:
    schema = pq.read_schema(path)
    missing = sorted(CANONICAL_REQUIRED - set(schema.names))
    if missing:
        raise ValueError(f"Cannot publish; canonical columns are missing: {missing}")

    id_counts: Counter[Any] = Counter()
    rows = 0
    wrong_company = 0
    empty_embedding = 0
    empty_bm25 = 0
    required_columns = sorted(CANONICAL_REQUIRED)
    for frame in iter_parquet_frames(path, columns=required_columns):
        rows += len(frame)
        id_counts.update(
            None if pd.isna(value) else value.item() if hasattr(value, "item") else value
            for value in frame["id"]
        )
        wrong_company += int(frame["company_id"].map(clean).ne(config.company_id).sum())
        empty_embedding += int(frame["embedding_content"].map(clean).eq("").sum())
        empty_bm25 += int(frame["bm25_content"].map(clean).eq("").sum())
    if rows == 0:
        raise ValueError("Cannot publish an empty search-ready artifact.")
    duplicate_ids = sum(count for count in id_counts.values() if count > 1)
    if duplicate_ids or wrong_company or empty_embedding or empty_bm25:
        raise ValueError(
            "Cannot publish invalid data: "
            f"duplicate_ids={duplicate_ids}, wrong_company={wrong_company}, "
            f"empty_embedding={empty_embedding}, empty_bm25={empty_bm25}"
        )
    return {
        "rows": rows,
        "columns": len(schema.names),
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


def _mysql_publish_file(
    connection: Any,
    input_path: Path,
    *,
    expected_rows: int,
    table: str,
    staging: str,
    backup: str,
    retain_previous: bool = False,
    indexes: list[list[str]] | None = None,
) -> None:
    from sqlalchemy import inspect, text

    rows_written = 0
    for batch_number, frame in enumerate(iter_parquet_frames(input_path), start=1):
        frame.to_sql(
            staging,
            connection,
            if_exists="fail" if batch_number == 1 else "append",
            index=False,
            chunksize=2000,
            method="multi",
            dtype=mysql_dtype_map(frame) if batch_number == 1 else None,
        )
        rows_written += len(frame)
        LOGGER.info(
            "MySQL publish batch %s complete: rows=%s total=%s",
            batch_number,
            len(frame),
            rows_written,
        )
    if rows_written != expected_rows:
        raise RuntimeError(
            f"Staging write mismatch: expected {expected_rows}, wrote {rows_written}"
        )
    loaded = connection.execute(text(f"SELECT COUNT(*) FROM `{staging}`")).scalar_one()
    if loaded != expected_rows:
        raise RuntimeError(f"Staging row-count mismatch: expected {expected_rows}, loaded {loaded}")
    for position, columns in enumerate(indexes or [], start=1):
        safe_columns = [safe_identifier(str(column), "index column") for column in columns]
        index_name = safe_identifier(f"idx_{staging[:42]}_{position}", "index name")
        unique = "UNIQUE " if safe_columns == ["id"] else ""
        column_sql = ", ".join(f"`{column}`" for column in safe_columns)
        connection.execute(text(f"CREATE {unique}INDEX `{index_name}` ON `{staging}` ({column_sql})"))
    if inspect(connection).has_table(table):
        if retain_previous:
            if inspect(connection).has_table(backup):
                retired = safe_identifier(
                    f"{table[:45]}__retired_{uuid4().hex[:8]}",
                    "retired table",
                )
                connection.execute(
                    text(
                        f"RENAME TABLE `{backup}` TO `{retired}`, "
                        f"`{table}` TO `{backup}`, `{staging}` TO `{table}`"
                    )
                )
                connection.execute(text(f"DROP TABLE `{retired}`"))
            else:
                connection.execute(text(f"RENAME TABLE `{table}` TO `{backup}`, `{staging}` TO `{table}`"))
        else:
            connection.execute(text(f"RENAME TABLE `{table}` TO `{backup}`, `{staging}` TO `{table}`"))
        if not retain_previous:
            try:
                connection.execute(text(f"DROP TABLE `{backup}`"))
            except Exception:
                LOGGER.warning("Published %s but could not remove backup table %s", table, backup, exc_info=True)
    else:
        connection.execute(text(f"RENAME TABLE `{staging}` TO `{table}`"))


def _postgres_publish_file(
    connection: Any,
    input_path: Path,
    *,
    expected_rows: int,
    table: str,
    staging: str,
    backup: str,
    schema: str,
    retain_previous: bool = False,
    indexes: list[list[str]] | None = None,
) -> None:
    from sqlalchemy import inspect, text

    rows_written = 0
    for batch_number, frame in enumerate(iter_parquet_frames(input_path), start=1):
        frame.to_sql(
            staging,
            connection,
            schema=schema,
            if_exists="fail" if batch_number == 1 else "append",
            index=False,
            chunksize=5000,
            method="multi",
        )
        rows_written += len(frame)
        LOGGER.info(
            "PostgreSQL publish batch %s complete: rows=%s total=%s",
            batch_number,
            len(frame),
            rows_written,
        )
    if rows_written != expected_rows:
        raise RuntimeError(
            f"Staging write mismatch: expected {expected_rows}, wrote {rows_written}"
        )
    loaded = connection.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{staging}"')).scalar_one()
    if loaded != expected_rows:
        raise RuntimeError(f"Staging row-count mismatch: expected {expected_rows}, loaded {loaded}")
    for position, columns in enumerate(indexes or [], start=1):
        safe_columns = [safe_identifier(str(column), "index column") for column in columns]
        index_name = safe_identifier(f"idx_{staging[:42]}_{position}", "index name")
        unique = "UNIQUE " if safe_columns == ["id"] else ""
        column_sql = ", ".join(f'"{column}"' for column in safe_columns)
        connection.execute(text(f'CREATE {unique}INDEX "{index_name}" ON "{schema}"."{staging}" ({column_sql})'))
    if inspect(connection).has_table(table, schema=schema):
        if retain_previous:
            connection.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{backup}"'))
        connection.execute(text(f'ALTER TABLE "{schema}"."{table}" RENAME TO "{backup}"'))
        connection.execute(text(f'ALTER TABLE "{schema}"."{staging}" RENAME TO "{table}"'))
        if not retain_previous:
            connection.execute(text(f'DROP TABLE "{schema}"."{backup}"'))
    else:
        connection.execute(text(f'ALTER TABLE "{schema}"."{staging}" RENAME TO "{table}"'))


def publish_company(config: PipelineConfig, *, dry_run: bool = False) -> dict[str, Any]:
    input_path = config.output.final / f"{config.artifact_prefix}_search_ready.parquet"
    if not input_path.exists():
        raise FileNotFoundError(f"Missing validated search-ready artifact: {input_path}")
    validation = validate_publish_file(config, input_path)
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
        "batch_size": PUBLISH_BATCH_SIZE,
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
    retain_previous = bool(config.destination.get("retain_previous_table", True))
    backup = safe_identifier(
        f"{table[:52]}__previous" if retain_previous else f"{table[:46]}__backup_{suffix}",
        "backup table",
    )
    indexes = [list(item) for item in config.destination.get("indexes", [])]
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            if backend == "mysql":
                _mysql_publish_file(
                    connection,
                    input_path,
                    expected_rows=validation["rows"],
                    table=table,
                    staging=staging,
                    backup=backup,
                    retain_previous=retain_previous,
                    indexes=indexes,
                )
            else:
                _postgres_publish_file(
                    connection,
                    input_path,
                    expected_rows=validation["rows"],
                    table=table,
                    staging=staging,
                    backup=backup,
                    schema=schema,
                    retain_previous=retain_previous,
                    indexes=indexes,
                )
            from sqlalchemy import inspect, text

            if backend == "mysql":
                live = connection.execute(
                    text(
                        f"SELECT COUNT(*) AS rows, COUNT(DISTINCT `id`) AS distinct_ids, "
                        f"SUM(CASE WHEN `company_id` <> :company_id THEN 1 ELSE 0 END) AS wrong_company "
                        f"FROM `{table}`"
                    ),
                    {"company_id": config.company_id},
                ).mappings().one()
            else:
                live = connection.execute(
                    text(
                        f'SELECT COUNT(*) AS rows, COUNT(DISTINCT "id") AS distinct_ids, '
                        f'SUM(CASE WHEN "company_id" <> :company_id THEN 1 ELSE 0 END) AS wrong_company '
                        f'FROM "{schema}"."{table}"'
                    ),
                    {"company_id": config.company_id},
                ).mappings().one()
            live_rows = int(live["rows"])
            live_distinct = int(live["distinct_ids"])
            live_wrong_company = int(live["wrong_company"] or 0)
            if (
                live_rows != validation["rows"]
                or live_distinct != live_rows
                or live_wrong_company
            ):
                if backend == "mysql" and retain_previous and inspect(connection).has_table(backup):
                    failed = safe_identifier(
                        f"{table[:46]}__failed_{uuid4().hex[:8]}",
                        "failed publish table",
                    )
                    connection.execute(
                        text(
                            f"RENAME TABLE `{table}` TO `{failed}`, "
                            f"`{backup}` TO `{table}`"
                        )
                    )
                    connection.execute(text(f"DROP TABLE `{failed}`"))
                raise RuntimeError(
                    "Post-publish verification failed: "
                    f"expected_rows={validation['rows']}, live_rows={live_rows}, "
                    f"distinct_ids={live_distinct}, wrong_company={live_wrong_company}"
                )
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
    report["previous_table"] = backup if retain_previous else ""
    report["post_publish_verification"] = {
        "status": "PASS",
        "live_rows": live_rows,
        "distinct_ids": live_distinct,
        "wrong_company_rows": live_wrong_company,
    }
    config.output.reports.mkdir(parents=True, exist_ok=True)
    (config.output.reports / "publish_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return report


def rollback_company(config: PipelineConfig) -> dict[str, Any]:
    table = safe_identifier(str(config.destination.get("table", "search_ready")), "destination table")
    schema = safe_identifier(str(config.destination.get("schema", "public")), "destination schema")
    previous = safe_identifier(f"{table[:52]}__previous", "previous table")
    temporary = safe_identifier(f"{table[:46]}__rollback_{uuid4().hex[:8]}", "rollback table")
    backend, url = destination_url(config)
    try:
        from sqlalchemy import create_engine, inspect, text
    except ModuleNotFoundError as exc:
        raise RuntimeError("SQLAlchemy and the destination database driver are required for rollback.") from exc
    engine = create_engine(url)
    with engine.begin() as connection:
        if backend == "mysql":
            if not inspect(connection).has_table(previous):
                raise RuntimeError(f"No retained previous table exists: {previous}")
            connection.execute(
                text(f"RENAME TABLE `{table}` TO `{temporary}`, `{previous}` TO `{table}`, `{temporary}` TO `{previous}`")
            )
            rows = connection.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar_one()
        else:
            if not inspect(connection).has_table(previous, schema=schema):
                raise RuntimeError(f"No retained previous table exists: {schema}.{previous}")
            connection.execute(text(f'ALTER TABLE "{schema}"."{table}" RENAME TO "{temporary}"'))
            connection.execute(text(f'ALTER TABLE "{schema}"."{previous}" RENAME TO "{table}"'))
            connection.execute(text(f'ALTER TABLE "{schema}"."{temporary}" RENAME TO "{previous}"'))
            rows = connection.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar_one()
    report = {
        "company_id": config.company_id,
        "rolled_back": True,
        "rolled_back_at": datetime.now(timezone.utc).isoformat(),
        "destination_table": table,
        "previous_table": previous,
        "live_rows": int(rows),
    }
    config.output.reports.mkdir(parents=True, exist_ok=True)
    (config.output.reports / "rollback_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return report
