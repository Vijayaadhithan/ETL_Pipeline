from __future__ import annotations

import shutil
from pathlib import Path

from .config import PipelineConfig


REPORT_FILES = {
    "category_join_report.json",
    "category_join_report.md",
    "location_join_report.json",
    "location_join_report.md",
    "attribute_mapping_report.json",
    "attribute_mapping_report.md",
    "embedding_ready_report.json",
    "final_output_correctness_report.json",
    "final_output_correctness_report.md",
}

DIAGNOSTIC_FILES = {
    "unresolved_category_ids.csv",
    "unresolved_city_ids.csv",
    "unresolved_locality_ids.csv",
    "city_locality_mismatches.csv",
    "unresolved_custom_cat_values.csv",
    "unmatched_attribute_value_ids.csv",
    "ad_attribute_schema_mismatches.csv",
    "possible_attribute_bridge_tables.csv",
    "final_output_category_mismatches.csv",
    "final_output_location_mismatches.csv",
    "final_output_attribute_mismatches.csv",
    "sample_category_enriched_rows.jsonl",
    "sample_location_enriched_rows.jsonl",
    "sample_attribute_enriched_rows.jsonl",
    "sample_embedding_ready_rows.jsonl",
    "final_output_verified_sample_rows.jsonl",
}


def copy_if_exists(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def organize_stage_artifacts(config: PipelineConfig) -> None:
    for base in [config.output.intermediate, config.output.final]:
        if not base.exists():
            continue
        for path in base.iterdir():
            if path.name in REPORT_FILES:
                copy_if_exists(path, config.output.reports / path.name)
            elif path.name in DIAGNOSTIC_FILES:
                copy_if_exists(path, config.output.diagnostics / path.name)


def publish_final_aliases(config: PipelineConfig) -> None:
    return None
