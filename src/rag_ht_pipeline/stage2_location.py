from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .stage1_category import NULL_VALUES, key, source_file, write_json


def read_csv(path: Path, nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=NULL_VALUES, nrows=nrows, low_memory=False)


def run(config: PipelineConfig, *, sample_size: int | None = None) -> dict[str, Any]:
    input_file = config.output.intermediate / "ads_stage_01_category_enriched.csv"
    ads = read_csv(input_file, nrows=sample_size)
    states = read_csv(source_file(config, "states.csv"))
    cities = read_csv(source_file(config, "location.csv"))
    localities = read_csv(source_file(config, "locations.csv"))

    out = ads.copy()
    out["raw_city_id"] = out["city_id"]
    out["raw_locality_id"] = out["locality_id"]
    out["__city_key"] = key(out["city_id"])
    out["__locality_key"] = key(out["locality_id"])

    city = cities.copy()
    city["__city_key"] = key(city["id"])
    city["__state_key"] = key(city["state_id"])
    city = city.rename(
        columns={
            "city": "city_name",
            "state_id": "city_state_id",
            "latitude": "city_latitude",
            "longitude": "city_longitude",
            "price": "city_listing_price",
            "top_ads_price": "city_top_ads_price",
            "premium_ads_price": "city_premium_ads_price",
        }
    )
    out = out.merge(
        city[
            [
                "__city_key",
                "__state_key",
                "city_name",
                "city_state_id",
                "city_latitude",
                "city_longitude",
                "city_listing_price",
                "city_top_ads_price",
                "city_premium_ads_price",
            ]
        ],
        how="left",
        on="__city_key",
        validate="m:1",
    )

    locality = localities.copy()
    locality["__locality_key"] = key(locality["id"])
    locality["__locality_city_key"] = key(locality["city_id"])
    locality = locality.rename(
        columns={
            "area": "locality_name",
            "pincode": "locality_pincode",
            "district": "locality_district",
            "city_id": "locality_city_id",
            "latitude": "locality_latitude",
            "longitude": "locality_longitude",
            "is_typeable": "locality_is_typeable",
            "is_trip_status": "locality_is_trip_status",
            "created_at": "locality_created_at",
            "updated_at": "locality_updated_at",
            "deleted_at": "locality_deleted_at",
        }
    )
    out = out.merge(
        locality[
            [
                "__locality_key",
                "__locality_city_key",
                "locality_name",
                "locality_pincode",
                "locality_district",
                "locality_city_id",
                "locality_latitude",
                "locality_longitude",
                "locality_is_typeable",
                "locality_is_trip_status",
                "locality_created_at",
                "locality_updated_at",
                "locality_deleted_at",
            ]
        ],
        how="left",
        on="__locality_key",
        validate="m:1",
    )

    state = states.copy()
    state["__state_key"] = key(state["id"])
    state = state.rename(columns={"id": "state_id", "name": "state_name"})
    out = out.merge(state[["__state_key", "state_id", "state_name"]], how="left", on="__state_key", validate="m:1")

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
    out = out.drop(columns=[c for c in out.columns if c.startswith("__")], errors="ignore")

    csv_path = config.output.intermediate / "ads_stage_02_location_enriched.csv"
    parquet_path = config.output.intermediate / "ads_stage_02_location_enriched.parquet"
    out.to_csv(csv_path, index=False)
    out.to_parquet(parquet_path, index=False)
    report = {
        "input_rows": int(len(ads)),
        "output_rows": int(len(out)),
        "resolved_city": int(city_resolved.sum()),
        "resolved_locality": int(locality_resolved.sum()),
        "output_files": {"enriched_csv": str(csv_path), "enriched_parquet": str(parquet_path)},
    }
    write_json(config.output.reports / "location_join_report.json", report)
    return report
