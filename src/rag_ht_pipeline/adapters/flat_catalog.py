from __future__ import annotations

import json
from typing import Any

import pandas as pd

from ..config import PipelineConfig
from ..stage1_category import NULL_VALUES, source_file, write_json
from .base import CompanyAdapter


CORE_COLUMNS = (
    "id",
    "title",
    "description",
    "status",
    "created_at",
    "updated_at",
    "main_category_id",
    "main_category_name",
    "subcategory_id",
    "subcategory_name",
    "state_id",
    "state_name",
    "city_id",
    "city_name",
    "locality_id",
    "locality_name",
    "rental_fee",
    "rental_duration",
    "attributes_json",
    "attributes_text",
    "attribute_values_text",
    "attribute_keywords_text",
)


def _json_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value


class FlatCatalogAdapter(CompanyAdapter):
    """Adapter for a company whose searchable records live in one flat file."""

    name = "flat_catalog"

    def _settings(self, config: PipelineConfig) -> tuple[str, dict[str, str]]:
        filename = str(config.adapter_config.get("filename", "")).strip()
        column_map = dict(config.adapter_config.get("column_map", {}))
        if not filename:
            raise ValueError("flat_catalog requires adapter_config.filename.")
        for required in ("id", "title"):
            if required not in column_map:
                raise ValueError(f"flat_catalog requires adapter_config.column_map.{required}.")
        return filename, column_map

    def validate_sources(self, config: PipelineConfig) -> dict[str, Any]:
        filename, column_map = self._settings(config)
        path = source_file(config, filename)
        available = set(pd.read_csv(path, nrows=0).columns)
        extra_columns = list(config.adapter_config.get("extra_columns", []))
        missing = sorted((set(column_map.values()) | set(extra_columns)) - available)
        if missing:
            raise ValueError(f"Flat catalog source {filename} is missing mapped columns: {missing}")
        return {
            "adapter": self.name,
            "files": {filename: str(path)},
            "mapped_columns": column_map,
            "status": "valid",
        }

    def normalize(
        self,
        config: PipelineConfig,
        *,
        sample_size: int | None = None,
        strict_subcategory_consistency: bool = False,
        no_csv: bool = False,
        record_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        del strict_subcategory_consistency
        validation = self.validate_sources(config)
        filename, column_map = self._settings(config)
        path = source_file(config, filename)
        read_options = {
            "dtype": "string",
            "keep_default_na": True,
            "na_values": NULL_VALUES,
            "low_memory": False,
        }
        if record_ids is None:
            source = pd.read_csv(path, nrows=sample_size, **read_options)
        else:
            source_id = column_map["id"]
            wanted = {str(value).strip() for value in record_ids}
            chunks = [
                chunk[
                    chunk[source_id].astype("string").str.strip().isin(wanted)
                ]
                for chunk in pd.read_csv(path, chunksize=50_000, **read_options)
            ]
            source = (
                pd.concat(chunks, ignore_index=True)
                if chunks
                else pd.read_csv(path, nrows=0, **read_options)
            )
        out = pd.DataFrame(index=source.index)
        for canonical in CORE_COLUMNS:
            source_column = column_map.get(canonical)
            out[canonical] = source[source_column] if source_column else ""
        out["company_id"] = config.company_id
        extra_columns = list(config.adapter_config.get("extra_columns", []))
        if extra_columns:
            out["extras_json"] = source[extra_columns].apply(
                lambda row: json.dumps(
                    {column: _json_value(value) for column, value in row.items()},
                    ensure_ascii=False,
                    default=str,
                ),
                axis=1,
            )
        else:
            out["extras_json"] = "{}"
        for filter_column in config.filter_columns:
            if filter_column in out.columns:
                continue
            source_column = column_map.get(filter_column, filter_column)
            if source_column not in source.columns:
                raise ValueError(
                    f"Configured filter column {filter_column!r} has no source mapping in {filename}."
                )
            out[filter_column] = source[source_column]

        config.output.intermediate.mkdir(parents=True, exist_ok=True)
        stem = f"{config.artifact_prefix}_stage_03_attributes_enriched"
        parquet = config.output.intermediate / f"{stem}.parquet"
        csv = config.output.intermediate / f"{stem}.csv"
        out.to_parquet(parquet, index=False)
        if not no_csv:
            out.to_csv(csv, index=False)
        else:
            csv.unlink(missing_ok=True)
        report = {
            "adapter": self.name,
            "input_rows": int(len(source)),
            "output_rows": int(len(out)),
            "retained_extra_columns": extra_columns,
            "output_files": {"parquet": str(parquet), "csv": str(csv) if not no_csv else ""},
        }
        write_json(config.output.reports / "normalization_report.json", report)
        return {"source-validation": validation, "normalization": report}
