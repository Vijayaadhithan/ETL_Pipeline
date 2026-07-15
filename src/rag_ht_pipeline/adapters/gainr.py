from __future__ import annotations

from typing import Any

import pandas as pd

from .. import stage1_category, stage2_location, stage3_attributes_streaming
from ..config import PipelineConfig
from .base import CompanyAdapter


class GainrAdapter(CompanyAdapter):
    name = "gainr"
    REQUIRED_FILES = (
        "ads.csv",
        "ads_attributes.csv",
        "categories.csv",
        "sub_categories.csv",
        "attributes.csv",
        "attribute_values.csv",
        "states.csv",
        "location.csv",
        "locations.csv",
    )

    def validate_sources(self, config: PipelineConfig) -> dict[str, Any]:
        if config.artifact_prefix != "ads":
            raise ValueError("The Gainr adapter requires company.artifact_prefix=ads for legacy compatibility.")
        files = {}
        for filename in self.REQUIRED_FILES:
            path = stage1_category.source_file(config, filename)
            files[filename] = str(path)
        required_columns = {
            "ads.csv": {"id", "title", "description", "category_id", "city_id", "locality_id"},
            "sub_categories.csv": {"id", "categoryId", "name"},
            "categories.csv": {"id", "name"},
            "ads_attributes.csv": {"ads_id", "attribute_id", "value"},
        }
        missing: dict[str, list[str]] = {}
        for filename, columns in required_columns.items():
            path = stage1_category.source_file(config, filename)
            available = set(pd.read_csv(path, nrows=0).columns)
            absent = sorted(columns - available)
            if absent:
                missing[filename] = absent
        if missing:
            raise ValueError(f"Gainr source schema is missing required columns: {missing}")
        return {"adapter": self.name, "files": files, "status": "valid"}

    def normalize(
        self,
        config: PipelineConfig,
        *,
        sample_size: int | None = None,
        strict_subcategory_consistency: bool = False,
        no_csv: bool = False,
        record_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        source_validation = self.validate_sources(config)
        reports = {
            "source-validation": source_validation,
            "category": stage1_category.run(
                config,
                sample_size=sample_size,
                no_csv=no_csv,
                record_ids=record_ids,
            ),
            "location": stage2_location.run(config, sample_size=sample_size, no_csv=no_csv),
            "attributes": stage3_attributes_streaming.run(
                config,
                sample_size=sample_size,
                strict_subcategory_consistency=strict_subcategory_consistency,
                no_csv=no_csv,
            ),
        }
        return reports

    def run_legacy_stage(
        self,
        stage: str,
        config: PipelineConfig,
        *,
        sample_size: int | None = None,
        strict_subcategory_consistency: bool = False,
        no_csv: bool = False,
        record_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        if stage == "category":
            return stage1_category.run(
                config,
                sample_size=sample_size,
                no_csv=no_csv,
                record_ids=record_ids,
            )
        if stage == "location":
            return stage2_location.run(config, sample_size=sample_size, no_csv=no_csv)
        if stage == "attributes":
            return stage3_attributes_streaming.run(
                config,
                sample_size=sample_size,
                strict_subcategory_consistency=strict_subcategory_consistency,
                no_csv=no_csv,
            )
        raise ValueError(f"Unknown Gainr legacy stage: {stage}")
