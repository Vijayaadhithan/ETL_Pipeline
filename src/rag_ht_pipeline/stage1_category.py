from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig

NULL_VALUES = ["", "NULL", "null", "None", "none", "NaN", "nan", "<NA>"]
BATCH_SIZE = 25_000
LOGGER = logging.getLogger("rag_ht_pipeline.stage1_category")


def read_csv(path: Path, nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=NULL_VALUES, nrows=nrows, low_memory=False)


def key(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype("string").str.strip(), errors="coerce").astype("Int64")


def active_rows(frame: pd.DataFrame, column: str = "deleted_at") -> pd.DataFrame:
    """Return rows whose soft-delete marker is NULL or blank."""
    if column not in frame.columns:
        return frame
    deleted = frame[column].astype("string").fillna("").str.strip().ne("")
    return frame.loc[~deleted].copy()


def active_ad_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Return non-deleted ads whose source status is explicitly active."""
    if "status" not in frame.columns:
        raise ValueError("Gainr ads snapshot is missing required status column.")
    non_deleted = active_rows(frame)
    status = (
        non_deleted["status"]
        .astype("string")
        .fillna("")
        .str.strip()
        .str.casefold()
    )
    numeric_one = pd.to_numeric(status, errors="coerce").eq(1)
    return non_deleted.loc[numeric_one | status.eq("true")].copy()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def source_file(config: PipelineConfig, name: str) -> Path:
    for base in [config.data_dir, config.input_dir, config.project_root]:
        path = base / name
        if path.exists():
            return path
    raise FileNotFoundError(f"Missing required source file: {name}")


def service_ad_counts(path: Path) -> dict[str, int]:
    """Count active service advertisements by user with bounded memory."""
    counts: dict[str, int] = {}
    for frame in pd.read_csv(
        path,
        usecols=["user_id", "category_type", "status", "deleted_at"],
        dtype="string",
        keep_default_na=True,
        na_values=NULL_VALUES,
        chunksize=BATCH_SIZE,
        low_memory=False,
    ):
        active = active_ad_rows(frame)
        service = active[key(active["category_type"]).eq(2)]
        for user_id, count in service["user_id"].value_counts().items():
            normalized = clean_key(user_id)
            if normalized:
                counts[normalized] = counts.get(normalized, 0) + int(count)
    return counts


def clean_key(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"", "nan", "none", "null", "<na>"} else text


def run(
    config: PipelineConfig,
    *,
    sample_size: int | None = None,
    no_csv: bool = False,
    record_ids: set[str] | None = None,
) -> dict[str, Any]:
    ads_path = source_file(config, "ads.csv")
    categories_path = source_file(config, "categories.csv")
    subcategories_path = source_file(config, "sub_categories.csv")
    users_path = source_file(config, "users.csv")

    categories = read_csv(categories_path)
    subcategories = read_csv(subcategories_path)
    users = active_rows(read_csv(users_path))
    users["__user_key"] = key(users["id"])
    users = users[
        [
            "__user_key",
            "prosper_id",
            "name",
            "photo",
            "is_aadhaar_gst_verified",
            "gender",
        ]
    ].rename(
        columns={
            "prosper_id": "user_prosper_id",
            "name": "user_name",
            "photo": "user_photo",
            "is_aadhaar_gst_verified": "user_is_aadhaar_gst_verified",
            "gender": "user_gender",
        }
    )
    if users["__user_key"].duplicated().any():
        raise ValueError("Gainr users snapshot contains duplicate IDs.")
    counts_by_user = service_ad_counts(ads_path)

    subs = subcategories.copy()
    subs["__subcategory_key"] = key(subs["id"])
    subs["__main_category_key"] = key(subs["categoryId"])
    sub_cols = {
        "id": "subcategory_id",
        "name": "subcategory_name",
        "slug": "subcategory_slug",
        "meta_title": "subcategory_meta_title",
        "meta_description": "subcategory_meta_description",
        "meta_keywords": "subcategory_meta_keywords",
        "status": "subcategory_status",
        "created_at": "subcategory_created_at",
        "updated_at": "subcategory_updated_at",
        "deleted_at": "subcategory_deleted_at",
    }
    subs = subs[["__subcategory_key", "__main_category_key", *sub_cols.keys()]].rename(columns=sub_cols)

    cats = categories.copy()
    cats["__main_category_key"] = key(cats["id"])
    cat_cols = {
        "id": "main_category_id",
        "name": "main_category_name",
        "slug": "main_category_slug",
        "cat_group": "main_category_cat_group",
        "rental_duration": "main_category_rental_duration",
        "meta_title": "main_category_meta_title",
        "meta_description": "main_category_meta_description",
        "meta_keywords": "main_category_meta_keywords",
        "ad_title_label": "main_category_ad_title_label",
        "placeholder": "main_category_placeholder",
        "status": "main_category_status",
        "created_at": "main_category_created_at",
        "updated_at": "main_category_updated_at",
        "deleted_at": "main_category_deleted_at",
    }
    cats = cats[["__main_category_key", *cat_cols.keys()]].rename(columns=cat_cols)

    csv_output = config.output.intermediate / "ads_stage_01_category_enriched.csv"
    parquet_output = config.output.intermediate / "ads_stage_01_category_enriched.parquet"
    parquet_output.parent.mkdir(parents=True, exist_ok=True)
    temp_parquet = Path(f"{parquet_output}.tmp")
    temp_csv = Path(f"{csv_output}.tmp")
    temp_parquet.unlink(missing_ok=True)
    temp_csv.unlink(missing_ok=True)
    wanted = None if record_ids is None else {str(value).strip() for value in record_ids}
    writer: pq.ParquetWriter | None = None
    writer_schema: pa.Schema | None = None
    rows_in = 0
    source_rows_read = 0
    soft_deleted_rows_filtered = 0
    inactive_status_rows_filtered = 0
    resolved_rows = 0
    csv_header = True
    remaining = sample_size
    try:
        reader = pd.read_csv(
            ads_path,
            dtype="string",
            keep_default_na=True,
            na_values=NULL_VALUES,
            chunksize=BATCH_SIZE,
            low_memory=False,
        )
        for batch_number, ads in enumerate(reader, start=1):
            source_rows_read += len(ads)
            non_deleted_ads = active_rows(ads)
            soft_deleted_rows_filtered += len(ads) - len(non_deleted_ads)
            active_ads = active_ad_rows(non_deleted_ads)
            inactive_status_rows_filtered += len(non_deleted_ads) - len(active_ads)
            ads = active_ads
            if wanted is not None:
                ads = ads[ads["id"].astype("string").str.strip().isin(wanted)]
            if remaining is not None:
                if remaining <= 0:
                    break
                ads = ads.head(remaining)
                remaining -= len(ads)
            if ads.empty:
                continue
            ads_out = ads.copy()
            ads_out["raw_category_id"] = ads_out["category_id"]
            ads_out["__subcategory_key"] = key(ads_out["category_id"])
            enriched = ads_out.merge(subs, how="left", on="__subcategory_key", validate="m:1")
            enriched = enriched.merge(cats, how="left", on="__main_category_key", validate="m:1")
            enriched["__user_key"] = key(enriched["user_id"])
            enriched = enriched.merge(
                users,
                how="left",
                on="__user_key",
                validate="m:1",
            )
            enriched["service_ad_count"] = (
                enriched["user_id"]
                .map(lambda value: counts_by_user.get(clean_key(value), 0))
                .astype("int64")
            )
            enriched["category_join_status"] = enriched["subcategory_id"].notna().map(
                {True: "resolved_via_subcategory", False: "unresolved_category_id"}
            )
            enriched.loc[key(enriched["raw_category_id"]).isna(), "category_join_status"] = "missing_category_id"
            enriched["category_join_mapping_used"] = "ads.category_id -> sub_categories.id -> categories.id"
            enriched["category_join_confidence"] = enriched["subcategory_id"].notna().astype(float)
            enriched = enriched.drop(
                columns=[
                    "__subcategory_key",
                    "__main_category_key",
                    "__user_key",
                ],
                errors="ignore",
            )
            table = pa.Table.from_pandas(enriched, preserve_index=False)
            if writer is None:
                writer_schema = table.schema
                writer = pq.ParquetWriter(temp_parquet, writer_schema, compression="snappy")
            elif table.schema != writer_schema:
                table = table.cast(writer_schema)
            writer.write_table(table)
            if not no_csv:
                enriched.to_csv(temp_csv, mode="w" if csv_header else "a", header=csv_header, index=False)
                csv_header = False
            rows_in += len(enriched)
            resolved_rows += int(enriched["subcategory_id"].notna().sum())
            LOGGER.info("Category batch %s complete: rows=%s total=%s", batch_number, len(enriched), rows_in)
    except Exception:
        temp_parquet.unlink(missing_ok=True)
        temp_csv.unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()
    if rows_in == 0:
        raise RuntimeError(f"No ads were selected from {ads_path}")
    temp_parquet.replace(parquet_output)
    if not no_csv:
        temp_csv.replace(csv_output)
    else:
        csv_output.unlink(missing_ok=True)

    report = {
        "input_rows": rows_in,
        "output_rows": rows_in,
        "source_rows_read": source_rows_read,
        "soft_deleted_rows_filtered": soft_deleted_rows_filtered,
        "inactive_status_rows_filtered": inactive_status_rows_filtered,
        "mapping_selected": "ads.category_id -> sub_categories.id -> categories.id",
        "resolved_rows": resolved_rows,
        "unresolved_rows": rows_in - resolved_rows,
        "batch_size": BATCH_SIZE,
        "output_files": {
            "enriched_csv": str(csv_output) if not no_csv else "",
            "enriched_parquet": str(parquet_output),
        },
    }
    write_json(config.output.reports / "category_join_report.json", report)
    return report
