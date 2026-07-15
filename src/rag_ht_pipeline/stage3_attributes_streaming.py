from __future__ import annotations

import gc
import logging
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig
from .stage1_category import key, write_json
from .stage3_attributes import (
    aggregate_usable_rows,
    build_catalog,
    read_bridge_for_ads,
    source_file,
)


BATCH_SIZE = 25_000
LOGGER = logging.getLogger("rag_ht_pipeline.stage3_attributes_streaming")


def _batches(parquet: Path, csv: Path) -> Iterator[pa.RecordBatch]:
    if parquet.exists():
        yield from pq.ParquetFile(parquet).iter_batches(batch_size=BATCH_SIZE)
        return
    for frame in pd.read_csv(csv, chunksize=BATCH_SIZE, low_memory=False):
        yield pa.RecordBatch.from_pandas(frame)


def _finish_frame(
    ads: pd.DataFrame,
    aggregated: pd.DataFrame,
    config: PipelineConfig,
) -> tuple[pd.DataFrame, pd.Series]:
    defaults = {
        "attribute_count": 0,
        "attribute_value_count": 0,
        "attribute_ids_json": "{}",
        "attribute_value_ids_json": "{}",
        "attributes_json": "{}",
        "attributes_text": "",
        "attribute_values_text": "",
        "attribute_keywords_text": "",
    }
    out = ads.copy()
    out["__ad"] = key(out["id"])
    if aggregated.empty:
        mapped = pd.Series(False, index=out.index)
        for column, default in defaults.items():
            out[column] = default
    else:
        if aggregated["__ad"].duplicated().any():
            raise ValueError("Attribute aggregation produced duplicate ad IDs.")
        aggregated = aggregated.set_index("__ad")
        mapped = out["__ad"].isin(aggregated.index)
        for column, default in defaults.items():
            out[column] = out["__ad"].map(aggregated[column]).fillna(default)
    out["attribute_count"] = pd.to_numeric(out["attribute_count"], errors="coerce").fillna(0).astype("int64")
    out["attribute_value_count"] = pd.to_numeric(out["attribute_value_count"], errors="coerce").fillna(0).astype("int64")
    out["attribute_mapping_source"] = mapped.map({True: "explicit_bridge_table", False: "unresolved"})
    out["attribute_mapping_status"] = mapped.map({True: "bridge_table_mapped", False: "no_ad_attribute_bridge"})
    out["attribute_mapping_confidence"] = mapped.astype(float)
    out["attribute_subcategory_consistency_status"] = mapped.map({True: "consistent", False: "not_applicable"})
    out["attribute_schema_mapping_status"] = mapped.map({True: "schema_matched", False: "schema_available"})
    out["custom_cat_value_raw"] = out.get("custom_cat_value", "")
    out["custom_cat_value_detected_format"] = "not_used_for_attribute_mapping"
    out["custom_cat_value_parse_status"] = "not_applicable"
    out["custom_cat_value_parsed_ids_json"] = "[]"
    out["custom_cat_value_unparsed_tokens_json"] = "[]"
    out["company_id"] = config.company_id
    out["extras_json"] = "{}"
    return out.drop(columns=["__ad"], errors="ignore"), mapped


def run(
    config: PipelineConfig,
    *,
    sample_size: int | None = None,
    strict_subcategory_consistency: bool = False,
    no_csv: bool = False,
) -> dict[str, Any]:
    del strict_subcategory_consistency
    started = perf_counter()
    parquet_input = config.output.intermediate / "ads_stage_02_location_enriched.parquet"
    csv_input = config.output.intermediate / "ads_stage_02_location_enriched.csv"
    if not parquet_input.exists() and not csv_input.exists():
        raise FileNotFoundError("Missing Stage 2 location output.")
    catalog = build_catalog(config)
    catalog_path = config.output.diagnostics / "subcategory_attribute_catalog.csv"
    mismatch_path = config.output.diagnostics / "ad_attribute_schema_mismatches.csv"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog.to_csv(catalog_path, index=False)
    cat = catalog.assign(
        __attr=key(catalog["attribute_id"]),
        __value=key(catalog["attribute_value_id"]),
        __attr_sub=key(catalog["subcategory_id"]),
    )
    bridge_path = source_file(config, "ads_attributes.csv")
    parquet_path = config.output.intermediate / "ads_stage_03_attributes_enriched.parquet"
    csv_path = config.output.intermediate / "ads_stage_03_attributes_enriched.csv"
    temp_parquet = Path(f"{parquet_path}.tmp")
    temp_csv = Path(f"{csv_path}.tmp")
    temp_mismatch = Path(f"{mismatch_path}.tmp")
    for path in (temp_parquet, temp_csv, temp_mismatch):
        path.unlink(missing_ok=True)
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    rows = mapped_ads = mismatch_rows = 0
    csv_header = mismatch_header = True
    remaining = sample_size
    try:
        for batch_number, batch in enumerate(_batches(parquet_input, csv_input), start=1):
            if remaining is not None:
                if remaining <= 0:
                    break
                if len(batch) > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= len(batch)
            ads = batch.to_pandas().astype("string")
            selected = {int(value) for value in key(ads["id"]).dropna().tolist()}
            bridge_source = read_bridge_for_ads(bridge_path, selected)
            bridge = bridge_source.assign(
                __ad=key(bridge_source["ads_id"]),
                __attr=key(bridge_source["attribute_id"]),
                __value=key(bridge_source["value"]),
            ).merge(cat, on=["__attr", "__value"], how="left")
            ad_keys = ads[["id", "subcategory_id"]].copy()
            ad_keys["__ad"] = key(ad_keys["id"])
            ad_keys["__ad_sub"] = key(ad_keys["subcategory_id"])
            bridge = bridge.merge(ad_keys[["__ad", "__ad_sub"]], on="__ad", how="left")
            bridge["__schema_match"] = (
                bridge["__attr_sub"].notna()
                & bridge["__ad_sub"].notna()
                & (bridge["__attr_sub"] == bridge["__ad_sub"])
            )
            mismatches = bridge[
                bridge["__attr_sub"].notna()
                & bridge["__ad_sub"].notna()
                & ~bridge["__schema_match"]
            ]
            usable = bridge[bridge["__schema_match"]].copy()
            aggregated = pd.DataFrame(aggregate_usable_rows(usable))
            out, mapped = _finish_frame(ads, aggregated, config)
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
            if not mismatches.empty:
                mismatches.to_csv(temp_mismatch, mode="w" if mismatch_header else "a", header=mismatch_header, index=False)
                mismatch_header = False
            rows += len(out)
            mapped_ads += int(mapped.sum())
            mismatch_rows += len(mismatches)
            LOGGER.info("Attribute batch %s complete: rows=%s total=%s", batch_number, len(out), rows)
            del bridge_source, bridge, ad_keys, mismatches, usable, aggregated, out, ads
            gc.collect()
    except Exception:
        temp_parquet.unlink(missing_ok=True)
        temp_csv.unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()
    if rows == 0:
        raise RuntimeError("Stage 3 received no rows.")
    temp_parquet.replace(parquet_path)
    if not no_csv:
        temp_csv.replace(csv_path)
    else:
        csv_path.unlink(missing_ok=True)
    if temp_mismatch.exists():
        temp_mismatch.replace(mismatch_path)
    else:
        pd.DataFrame().to_csv(mismatch_path, index=False)
    report = {
        "input_rows": rows,
        "output_rows": rows,
        "confirmed_bridge_table": "ads_attributes.csv",
        "mapped_ads": mapped_ads,
        "schema_mismatch_rows": mismatch_rows,
        "batch_size": BATCH_SIZE,
        "duration_seconds": round(perf_counter() - started, 2),
        "output_files": {"enriched_csv": str(csv_path) if not no_csv else "", "enriched_parquet": str(parquet_path)},
    }
    write_json(config.output.reports / "attribute_mapping_report.json", report)
    return report
