from __future__ import annotations

import gc
import json
import logging
from collections import defaultdict
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .stage1_category import NULL_VALUES, key, source_file, write_json


LOGGER = logging.getLogger("rag_ht_pipeline.stage3_attributes")


def read_csv(path: Path, nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=NULL_VALUES, nrows=nrows, low_memory=False)


def read_bridge_for_ads(path: Path, ad_ids: set[int]) -> pd.DataFrame:
    chunks = []
    for chunk in pd.read_csv(
        path,
        dtype="string",
        keep_default_na=True,
        na_values=NULL_VALUES,
        chunksize=100_000,
        low_memory=False,
    ):
        selected = chunk[key(chunk["ads_id"]).isin(ad_ids)]
        if not selected.empty:
            chunks.append(selected)
    if chunks:
        return pd.concat(chunks, ignore_index=True)
    return read_csv(path, nrows=0)


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else " ".join(text.split())


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = clean(value)
        if not item:
            continue
        folded = item.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        out.append(item)
    return out


def build_catalog(config: PipelineConfig) -> pd.DataFrame:
    categories = read_csv(source_file(config, "categories.csv"))
    subcategories = read_csv(source_file(config, "sub_categories.csv"))
    attributes = read_csv(source_file(config, "attributes.csv"))
    values = read_csv(source_file(config, "attribute_values.csv"))

    cats = categories.assign(__cat=key(categories["id"]))[["__cat", "id", "name"]].rename(
        columns={"id": "main_category_id", "name": "main_category_name"}
    )
    subs = subcategories.assign(__sub=key(subcategories["id"]), __cat=key(subcategories["categoryId"]))[
        ["__sub", "__cat", "id", "name", "slug"]
    ].rename(columns={"id": "subcategory_id", "name": "subcategory_name", "slug": "subcategory_slug"})
    attrs = attributes.assign(__attr=key(attributes["id"]), __sub=key(attributes["sub_category_id"]))[
        ["__attr", "__sub", "id", "name", "mandatory", "is_title", "title_prefix", "created_at", "updated_at", "deleted_at"]
    ].rename(
        columns={
            "id": "attribute_id",
            "name": "attribute_name",
            "mandatory": "attribute_mandatory",
            "is_title": "attribute_is_title",
            "title_prefix": "attribute_title_prefix",
            "created_at": "attribute_created_at",
            "updated_at": "attribute_updated_at",
            "deleted_at": "attribute_deleted_at",
        }
    )
    vals = values.assign(__attr=key(values["attributeId"]))[
        ["__attr", "id", "value", "keywords", "created_at", "updated_at", "deleted_at"]
    ].rename(
        columns={
            "id": "attribute_value_id",
            "value": "attribute_value",
            "keywords": "attribute_value_keywords",
            "created_at": "attribute_value_created_at",
            "updated_at": "attribute_value_updated_at",
            "deleted_at": "attribute_value_deleted_at",
        }
    )
    catalog = attrs.merge(vals, on="__attr", how="left").merge(subs, on="__sub", how="left").merge(cats, on="__cat", how="left")
    return catalog[
        [
            "main_category_id",
            "main_category_name",
            "subcategory_id",
            "subcategory_name",
            "subcategory_slug",
            "attribute_id",
            "attribute_name",
            "attribute_mandatory",
            "attribute_is_title",
            "attribute_title_prefix",
            "attribute_created_at",
            "attribute_updated_at",
            "attribute_deleted_at",
            "attribute_value_id",
            "attribute_value",
            "attribute_value_keywords",
            "attribute_value_created_at",
            "attribute_value_updated_at",
            "attribute_value_deleted_at",
        ]
    ]


def aggregate_group(rows: pd.DataFrame) -> dict[str, Any]:
    by_attr: dict[str, list[str]] = defaultdict(list)
    attr_ids: dict[str, list[int]] = defaultdict(list)
    value_ids: dict[str, list[int]] = defaultdict(list)
    keywords: list[str] = []
    columns = [
        "attribute_name",
        "attribute_value",
        "attribute_value_id",
        "attribute_value_keywords",
    ]
    for name_raw, value_raw, attribute_value_id, keywords_raw in rows[columns].itertuples(
        index=False,
        name=None,
    ):
        name = clean(name_raw) or "Unknown Attribute"
        value = clean(value_raw)
        if value:
            by_attr[name].append(value)
        if pd.notna(attribute_value_id):
            value_ids[name].append(int(attribute_value_id))
        keywords.extend([part.strip() for part in clean(keywords_raw).split(",") if part.strip()])
    by_attr = {k: dedupe(v) for k, v in by_attr.items()}
    attr_ids = {k: sorted(set(v)) for k, v in attr_ids.items()}
    value_ids = {k: sorted(set(v)) for k, v in value_ids.items()}
    values_flat = [v for values in by_attr.values() for v in values]
    return {
        "attribute_count": len(by_attr),
        "attribute_value_count": sum(len(v) for v in by_attr.values()),
        "attribute_ids_json": json.dumps(attr_ids, ensure_ascii=False),
        "attribute_value_ids_json": json.dumps(value_ids, ensure_ascii=False),
        "attributes_json": json.dumps(by_attr, ensure_ascii=False),
        "attributes_text": ". ".join(f"{k}: {', '.join(v)}" for k, v in by_attr.items()) + ("." if by_attr else ""),
        "attribute_values_text": ". ".join(dedupe(values_flat)) + ("." if values_flat else ""),
        "attribute_keywords_text": ", ".join(dedupe(keywords)),
    }


def aggregate_usable_rows(rows: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "__ad",
        "attribute_name",
        "attribute_value",
        "attribute_value_id",
        "attribute_value_keywords",
    ]
    ordered = rows
    if not rows["__ad"].is_monotonic_increasing:
        ordered = rows.sort_values("__ad", kind="stable")

    records: list[dict[str, Any]] = []
    current_ad: int | None = None
    by_attr: dict[str, list[str]] = defaultdict(list)
    value_ids: dict[str, list[int]] = defaultdict(list)
    keywords: list[str] = []

    def finish() -> None:
        if current_ad is None:
            return
        deduped_attributes = {
            name: dedupe(values) for name, values in by_attr.items()
        }
        deduped_value_ids = {
            name: sorted(set(values)) for name, values in value_ids.items()
        }
        values_flat = [
            value
            for values in deduped_attributes.values()
            for value in values
        ]
        records.append(
            {
                "__ad": current_ad,
                "__mapped": True,
                "attribute_count": len(deduped_attributes),
                "attribute_value_count": sum(
                    len(values) for values in deduped_attributes.values()
                ),
                "attribute_ids_json": "{}",
                "attribute_value_ids_json": json.dumps(
                    deduped_value_ids,
                    ensure_ascii=False,
                ),
                "attributes_json": json.dumps(
                    deduped_attributes,
                    ensure_ascii=False,
                ),
                "attributes_text": ". ".join(
                    f"{name}: {', '.join(values)}"
                    for name, values in deduped_attributes.items()
                )
                + ("." if deduped_attributes else ""),
                "attribute_values_text": ". ".join(dedupe(values_flat))
                + ("." if values_flat else ""),
                "attribute_keywords_text": ", ".join(dedupe(keywords)),
            }
        )

    for (
        ad_id_raw,
        name_raw,
        value_raw,
        attribute_value_id,
        keywords_raw,
    ) in ordered[columns].itertuples(index=False, name=None):
        if pd.isna(ad_id_raw):
            continue
        ad_id = int(ad_id_raw)
        if current_ad != ad_id:
            finish()
            current_ad = ad_id
            by_attr = defaultdict(list)
            value_ids = defaultdict(list)
            keywords = []

        name = clean(name_raw) or "Unknown Attribute"
        value = clean(value_raw)
        if value:
            by_attr[name].append(value)
        if pd.notna(attribute_value_id):
            value_ids[name].append(int(attribute_value_id))
        keywords.extend(
            part.strip()
            for part in clean(keywords_raw).split(",")
            if part.strip()
        )

    finish()
    return records


def run(
    config: PipelineConfig,
    *,
    sample_size: int | None = None,
    strict_subcategory_consistency: bool = False,
    no_csv: bool = False,
) -> dict[str, Any]:
    started = perf_counter()
    parquet_input = config.output.intermediate / "ads_stage_02_location_enriched.parquet"
    csv_input = config.output.intermediate / "ads_stage_02_location_enriched.csv"
    if parquet_input.exists():
        ads = pd.read_parquet(parquet_input).astype("string")
        if sample_size is not None:
            ads = ads.head(sample_size).copy()
    else:
        ads = read_csv(csv_input, nrows=sample_size)
    input_rows = len(ads)
    LOGGER.info("Loaded %s ads for attribute normalization", len(ads))

    catalog = build_catalog(config)
    bridge_path = source_file(config, "ads_attributes.csv")
    if input_rows < 100_000:
        selected_ad_ids = {
            int(value) for value in key(ads["id"]).dropna().tolist()
        }
        ads_attributes = read_bridge_for_ads(bridge_path, selected_ad_ids)
    else:
        ads_attributes = read_csv(bridge_path)
    LOGGER.info(
        "Loaded attribute bridge rows=%s and catalog rows=%s",
        len(ads_attributes),
        len(catalog),
    )

    bridge = ads_attributes.assign(
        __ad=key(ads_attributes["ads_id"]),
        __attr=key(ads_attributes["attribute_id"]),
        __value=key(ads_attributes["value"]),
    )
    cat = catalog.assign(__attr=key(catalog["attribute_id"]), __value=key(catalog["attribute_value_id"]), __attr_sub=key(catalog["subcategory_id"]))
    bridge = bridge.merge(cat, on=["__attr", "__value"], how="left")
    ad_keys = ads[["id", "subcategory_id"]].copy()
    ad_keys["__ad"] = key(ad_keys["id"])
    ad_keys["__ad_sub"] = key(ad_keys["subcategory_id"])
    bridge = bridge.merge(ad_keys[["__ad", "__ad_sub"]], on="__ad", how="left")
    bridge["__schema_match"] = bridge["__attr_sub"].notna() & bridge["__ad_sub"].notna() & (bridge["__attr_sub"] == bridge["__ad_sub"])
    mismatches = bridge[bridge["__attr_sub"].notna() & bridge["__ad_sub"].notna() & ~bridge["__schema_match"]]
    usable = bridge[bridge["__schema_match"]].copy()
    LOGGER.info(
        "Attribute bridge validation complete: usable=%s mismatches=%s",
        len(usable),
        len(mismatches),
    )

    aggregation_started = perf_counter()
    grouped_records = aggregate_usable_rows(usable)
    aggregated = pd.DataFrame(grouped_records)
    del grouped_records
    LOGGER.info(
        "Aggregated attributes for %s ads in %.2fs",
        len(aggregated),
        perf_counter() - aggregation_started,
    )

    out = ads
    del ads
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
    del bridge, usable, ads_attributes, ad_keys, cat
    gc.collect()
    out["attribute_count"] = pd.to_numeric(out["attribute_count"], errors="coerce").fillna(0).astype("int64")
    out["attribute_value_count"] = (
        pd.to_numeric(out["attribute_value_count"], errors="coerce").fillna(0).astype("int64")
    )
    out["attribute_mapping_source"] = mapped.map(
        {True: "explicit_bridge_table", False: "unresolved"}
    )
    out["attribute_mapping_status"] = mapped.map(
        {True: "bridge_table_mapped", False: "no_ad_attribute_bridge"}
    )
    out["attribute_mapping_confidence"] = mapped.astype(float)
    out["attribute_subcategory_consistency_status"] = mapped.map(
        {True: "consistent", False: "not_applicable"}
    )
    out["attribute_schema_mapping_status"] = mapped.map(
        {True: "schema_matched", False: "schema_available"}
    )
    out["custom_cat_value_raw"] = out.get("custom_cat_value", "")
    out["custom_cat_value_detected_format"] = "not_used_for_attribute_mapping"
    out["custom_cat_value_parse_status"] = "not_applicable"
    out["custom_cat_value_parsed_ids_json"] = "[]"
    out["custom_cat_value_unparsed_tokens_json"] = "[]"
    out["company_id"] = config.company_id
    out["extras_json"] = "{}"
    out = out.drop(columns=["__ad"], errors="ignore")

    csv_path = config.output.intermediate / "ads_stage_03_attributes_enriched.csv"
    parquet_path = config.output.intermediate / "ads_stage_03_attributes_enriched.parquet"
    catalog_path = config.output.diagnostics / "subcategory_attribute_catalog.csv"
    mismatch_path = config.output.diagnostics / "ad_attribute_schema_mismatches.csv"
    out.to_parquet(parquet_path, index=False)
    if not no_csv:
        out.to_csv(csv_path, index=False)
    else:
        csv_path.unlink(missing_ok=True)
    catalog.to_csv(catalog_path, index=False)
    mismatches.to_csv(mismatch_path, index=False)
    report = {
        "input_rows": int(input_rows),
        "output_rows": int(len(out)),
        "confirmed_bridge_table": "ads_attributes.csv",
        "mapped_ads": int((out["attribute_mapping_source"] == "explicit_bridge_table").sum()),
        "schema_mismatch_rows": int(len(mismatches)),
        "duration_seconds": round(perf_counter() - started, 2),
        "output_files": {
            "enriched_csv": str(csv_path) if not no_csv else "",
            "enriched_parquet": str(parquet_path),
        },
    }
    write_json(config.output.reports / "attribute_mapping_report.json", report)
    return report
