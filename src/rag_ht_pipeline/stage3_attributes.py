from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from .config import PipelineConfig
from .stage1_category import NULL_VALUES, key, source_file, write_json


def read_csv(path: Path, nrows: int | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=NULL_VALUES, nrows=nrows, low_memory=False)


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
    for row in rows.to_dict(orient="records"):
        name = clean(row.get("attribute_name")) or "Unknown Attribute"
        value = clean(row.get("attribute_value"))
        if value:
            by_attr[name].append(value)
        if pd.notna(row.get("attribute_id")):
            attr_ids[name].append(int(row["attribute_id"]))
        if pd.notna(row.get("attribute_value_id")):
            value_ids[name].append(int(row["attribute_value_id"]))
        keywords.extend([part.strip() for part in clean(row.get("attribute_value_keywords")).split(",") if part.strip()])
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


def run(
    config: PipelineConfig,
    *,
    sample_size: int | None = None,
    strict_subcategory_consistency: bool = False,
) -> dict[str, Any]:
    ads = read_csv(config.output.intermediate / "ads_stage_02_location_enriched.csv", nrows=sample_size)
    catalog = build_catalog(config)
    ads_attributes = read_csv(source_file(config, "ads_attributes.csv"))
    attrs = read_csv(source_file(config, "attributes.csv"))

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

    grouped = {int(ad): aggregate_group(group) for ad, group in usable.groupby("__ad", dropna=True)}
    out = ads.copy()
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
    for column in defaults:
        out[column] = defaults[column]
    out["attribute_mapping_source"] = "unresolved"
    out["attribute_mapping_status"] = "no_ad_attribute_bridge"
    out["attribute_mapping_confidence"] = 0.0
    out["attribute_subcategory_consistency_status"] = "not_applicable"
    out["attribute_schema_mapping_status"] = "schema_available"
    out["custom_cat_value_raw"] = out.get("custom_cat_value", "")
    out["custom_cat_value_detected_format"] = "not_used_for_attribute_mapping"
    out["custom_cat_value_parse_status"] = "not_applicable"
    out["custom_cat_value_parsed_ids_json"] = "[]"
    out["custom_cat_value_unparsed_tokens_json"] = "[]"
    for idx, row in out.iterrows():
        ad_id = int(key(pd.Series([row["id"]])).iloc[0]) if pd.notna(key(pd.Series([row["id"]])).iloc[0]) else None
        if ad_id in grouped:
            for column, value in grouped[ad_id].items():
                out.at[idx, column] = value
            out.at[idx, "attribute_mapping_source"] = "explicit_bridge_table"
            out.at[idx, "attribute_mapping_status"] = "bridge_table_mapped"
            out.at[idx, "attribute_mapping_confidence"] = 1.0
            out.at[idx, "attribute_subcategory_consistency_status"] = "consistent"
            out.at[idx, "attribute_schema_mapping_status"] = "schema_matched"

    csv_path = config.output.intermediate / "ads_stage_03_attributes_enriched.csv"
    parquet_path = config.output.intermediate / "ads_stage_03_attributes_enriched.parquet"
    catalog_path = config.output.diagnostics / "subcategory_attribute_catalog.csv"
    mismatch_path = config.output.diagnostics / "ad_attribute_schema_mismatches.csv"
    out.to_csv(csv_path, index=False)
    out.to_parquet(parquet_path, index=False)
    catalog.to_csv(catalog_path, index=False)
    mismatches.to_csv(mismatch_path, index=False)
    report = {
        "input_rows": int(len(ads)),
        "output_rows": int(len(out)),
        "confirmed_bridge_table": "ads_attributes.csv",
        "mapped_ads": int((out["attribute_mapping_source"] == "explicit_bridge_table").sum()),
        "schema_mismatch_rows": int(len(mismatches)),
        "output_files": {"enriched_csv": str(csv_path), "enriched_parquet": str(parquet_path)},
    }
    write_json(config.output.reports / "attribute_mapping_report.json", report)
    return report
