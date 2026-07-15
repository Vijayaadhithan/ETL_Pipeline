from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage1_category import NULL_VALUES, key, source_file, write_json


BATCH_SIZE = 25_000
LOGGER = logging.getLogger("rag_ht_pipeline.stage2_location")


def read_csv(path: Path, nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=NULL_VALUES, nrows=nrows, low_memory=False)


def _lookups(config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    states = read_csv(source_file(config, "states.csv"))
    cities = read_csv(source_file(config, "location.csv"))
    localities = read_csv(source_file(config, "locations.csv"))

    city = cities.copy()
    city["__city_key"] = key(city["id"])
    city["__state_key"] = key(city["state_id"])
    city = city.rename(columns={
        "city": "city_name", "state_id": "city_state_id", "latitude": "city_latitude",
        "longitude": "city_longitude", "price": "city_listing_price",
        "top_ads_price": "city_top_ads_price", "premium_ads_price": "city_premium_ads_price",
    })
    city = city[[
        "__city_key", "__state_key", "city_name", "city_state_id", "city_latitude",
        "city_longitude", "city_listing_price", "city_top_ads_price", "city_premium_ads_price",
    ]]

    locality = localities.copy()
    locality["__locality_key"] = key(locality["id"])
    locality["__locality_city_key"] = key(locality["city_id"])
    locality = locality.rename(columns={
        "area": "locality_name", "pincode": "locality_pincode", "district": "locality_district",
        "city_id": "locality_city_id", "latitude": "locality_latitude", "longitude": "locality_longitude",
        "is_typeable": "locality_is_typeable", "is_trip_status": "locality_is_trip_status",
        "created_at": "locality_created_at", "updated_at": "locality_updated_at", "deleted_at": "locality_deleted_at",
    })
    locality = locality[[
        "__locality_key", "__locality_city_key", "locality_name", "locality_pincode",
        "locality_district", "locality_city_id", "locality_latitude", "locality_longitude",
        "locality_is_typeable", "locality_is_trip_status", "locality_created_at",
        "locality_updated_at", "locality_deleted_at",
    ]]

    state = states.copy()
    state["__state_key"] = key(state["id"])
    state = state.rename(columns={"id": "state_id", "name": "state_name"})
    return city, locality, state[["__state_key", "state_id", "state_name"]]


def _enrich(ads: pd.DataFrame, city: pd.DataFrame, locality: pd.DataFrame, state: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    out = ads.astype("string").copy()
    out["raw_city_id"] = out["city_id"]
    out["raw_locality_id"] = out["locality_id"]
    out["__city_key"] = key(out["city_id"])
    out["__locality_key"] = key(out["locality_id"])
    out = out.merge(city, how="left", on="__city_key", validate="m:1")
    out = out.merge(locality, how="left", on="__locality_key", validate="m:1")
    out = out.merge(state, how="left", on="__state_key", validate="m:1")
    city_resolved = out["city_name"].notna()
    locality_resolved = out["locality_name"].notna()
    out["city_locality_consistency_status"] = "locality_missing"
    out.loc[~city_resolved, "city_locality_consistency_status"] = "city_unresolved"
    out.loc[~locality_resolved, "city_locality_consistency_status"] = "locality_unresolved"
    both = city_resolved & locality_resolved
    out.loc[both & (out["__city_key"] == out["__locality_city_key"]), "city_locality_consistency_status"] = "consistent"
    out.loc[both & (out["__city_key"] != out["__locality_city_key"]), "city_locality_consistency_status"] = "mismatch"
    out["location_join_status"] = "resolved_from_ad_city_and_locality"
    out.loc[city_resolved & ~locality_resolved, "location_join_status"] = "resolved_city_only"
    out.loc[~city_resolved & locality_resolved, "location_join_status"] = "resolved_locality_only"
    out.loc[~city_resolved & ~locality_resolved, "location_join_status"] = "missing_city_and_locality"
    out["location_join_confidence"] = (city_resolved | locality_resolved).astype(float)
    out["location_mapping_source"] = out["location_join_status"]
    out = out.drop(columns=[column for column in out.columns if column.startswith("__")], errors="ignore")
    return out, int(city_resolved.sum()), int(locality_resolved.sum())


def run(config: PipelineConfig, *, sample_size: int | None = None, no_csv: bool = False) -> dict[str, Any]:
    parquet_input = config.output.intermediate / "ads_stage_01_category_enriched.parquet"
    csv_input = config.output.intermediate / "ads_stage_01_category_enriched.csv"
    if not parquet_input.exists() and not csv_input.exists():
        raise FileNotFoundError("Missing Stage 1 category output.")
    city, locality, state = _lookups(config)
    csv_path = config.output.intermediate / "ads_stage_02_location_enriched.csv"
    parquet_path = config.output.intermediate / "ads_stage_02_location_enriched.parquet"
    temp_parquet = Path(f"{parquet_path}.tmp")
    temp_csv = Path(f"{csv_path}.tmp")
    temp_parquet.unlink(missing_ok=True)
    temp_csv.unlink(missing_ok=True)
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    rows = resolved_city = resolved_locality = 0
    remaining = sample_size
    csv_header = True
    if parquet_input.exists():
        batches = pq.ParquetFile(parquet_input).iter_batches(batch_size=BATCH_SIZE)
    else:
        batches = (pa.RecordBatch.from_pandas(frame) for frame in pd.read_csv(csv_input, chunksize=BATCH_SIZE, low_memory=False))
    try:
        for batch_number, batch in enumerate(batches, start=1):
            if remaining is not None:
                if remaining <= 0:
                    break
                if len(batch) > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= len(batch)
            out, city_count, locality_count = _enrich(batch.to_pandas(), city, locality, state)
            table = pa.Table.from_pandas(out, preserve_index=False)
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(temp_parquet, schema, compression="snappy")
            elif table.schema != schema:
                table = table.cast(schema)
            writer.write_table(table)
            if not no_csv:
                out.to_csv(temp_csv, mode="w" if csv_header else "a", header=csv_header, index=False)
                csv_header = False
            rows += len(out)
            resolved_city += city_count
            resolved_locality += locality_count
            LOGGER.info("Location batch %s complete: rows=%s total=%s", batch_number, len(out), rows)
    except Exception:
        temp_parquet.unlink(missing_ok=True)
        temp_csv.unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()
    if rows == 0:
        raise RuntimeError("Stage 2 received no rows.")
    temp_parquet.replace(parquet_path)
    if not no_csv:
        temp_csv.replace(csv_path)
    else:
        csv_path.unlink(missing_ok=True)
    report = {
        "input_rows": rows, "output_rows": rows, "resolved_city": resolved_city,
        "resolved_locality": resolved_locality, "batch_size": BATCH_SIZE,
        "output_files": {"enriched_csv": str(csv_path) if not no_csv else "", "enriched_parquet": str(parquet_path)},
    }
    write_json(config.output.reports / "location_join_report.json", report)
    return report
