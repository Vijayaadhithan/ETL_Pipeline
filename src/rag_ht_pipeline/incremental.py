from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import OutputLayout, PipelineConfig, ensure_output_dirs
from .stage3_attributes import clean


MERGE_BATCH_SIZE = 25_000


def load_change_set(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing incremental change set: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    for required in ("company_id", "mode", "changed_ids", "removed_ids"):
        if required not in payload:
            raise ValueError(f"Incremental change set is missing {required!r}: {path}")
    return payload


def incremental_work_config(config: PipelineConfig) -> tuple[PipelineConfig, str]:
    run_id = uuid4().hex
    root = config.output.root / "incremental" / "work" / run_id
    output = OutputLayout(
        root=root,
        intermediate=root / "intermediate",
        final=root / "final",
        reports=root / "reports",
        diagnostics=root / "diagnostics",
    )
    work_config = replace(config, output=output)
    ensure_output_dirs(work_config)
    return work_config, run_id


def required_incremental_baselines(config: PipelineConfig) -> list[Path]:
    prefix = config.artifact_prefix
    return [
        config.output.intermediate / f"{prefix}_stage_03_attributes_enriched.parquet",
        config.output.final / f"{prefix}_embedding_ready.parquet",
        config.output.final / f"{prefix}_search_ready.parquet",
    ]


def missing_incremental_baselines(config: PipelineConfig) -> list[Path]:
    return [path for path in required_incremental_baselines(config) if not path.exists()]


def _normalized_ids(series: pd.Series) -> pd.Series:
    return series.map(clean)


def merge_parquet_delta(
    baseline_path: Path,
    delta_path: Path | None,
    output_path: Path,
    *,
    removed_ids: set[str],
) -> dict[str, int]:
    baseline = pq.ParquetFile(baseline_path)
    schema = baseline.schema_arrow
    if "id" not in schema.names:
        raise ValueError(f"Incremental artifact has no id column: {baseline_path}")

    if delta_path is not None and delta_path.exists():
        delta = pd.read_parquet(delta_path)
        if list(delta.columns) != schema.names:
            raise ValueError(
                f"Incremental delta columns do not match baseline for {baseline_path.name}."
            )
    else:
        delta = pd.DataFrame(columns=schema.names)
    delta_keys = _normalized_ids(delta["id"]) if "id" in delta else pd.Series(dtype="string")
    if delta_keys.eq("").any() or delta_keys.duplicated().any():
        raise ValueError(f"Incremental delta contains empty or duplicate IDs: {delta_path}")
    delta = delta.copy()
    delta.index = delta_keys
    delta_ids = set(delta.index)
    removed_only = set(removed_ids) - delta_ids

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temp_path.unlink(missing_ok=True)
    writer = pq.ParquetWriter(temp_path, schema, compression="snappy")
    seen_delta: set[str] = set()
    baseline_rows = 0
    removed_rows = 0
    output_rows = 0
    try:
        for batch in baseline.iter_batches(batch_size=MERGE_BATCH_SIZE):
            frame = batch.to_pandas()
            baseline_rows += len(frame)
            keys = _normalized_ids(frame["id"])
            keep = ~keys.isin(removed_only)
            removed_rows += int((~keep).sum())
            frame = frame.loc[keep].copy()
            keys = keys.loc[keep]
            replace_mask = keys.isin(delta_ids)
            if replace_mask.any():
                replacement_keys = keys.loc[replace_mask].tolist()
                frame.loc[replace_mask, schema.names] = delta.loc[
                    replacement_keys,
                    schema.names,
                ].to_numpy()
                seen_delta.update(replacement_keys)
            table = pa.Table.from_pandas(
                frame,
                schema=schema,
                preserve_index=False,
                safe=False,
            )
            writer.write_table(table)
            output_rows += len(frame)

        new_ids = [record_id for record_id in delta.index if record_id not in seen_delta]
        if new_ids:
            additions = delta.loc[new_ids, schema.names].reset_index(drop=True)
            writer.write_table(
                pa.Table.from_pandas(
                    additions,
                    schema=schema,
                    preserve_index=False,
                    safe=False,
                )
            )
            output_rows += len(additions)
    except Exception:
        writer.close()
        temp_path.unlink(missing_ok=True)
        raise
    else:
        writer.close()
        temp_path.replace(output_path)

    return {
        "baseline_rows": baseline_rows,
        "delta_rows": len(delta),
        "removed_rows": removed_rows,
        "output_rows": output_rows,
    }


def prepare_merged_artifacts(
    config: PipelineConfig,
    work_config: PipelineConfig,
    *,
    removed_ids: set[str],
) -> tuple[list[tuple[Path, Path]], dict[str, Any]]:
    prefix = config.artifact_prefix
    candidates = [
        (
            config.output.intermediate / f"{prefix}_stage_01_category_enriched.parquet",
            work_config.output.intermediate / f"{prefix}_stage_01_category_enriched.parquet",
        ),
        (
            config.output.intermediate / f"{prefix}_stage_02_location_enriched.parquet",
            work_config.output.intermediate / f"{prefix}_stage_02_location_enriched.parquet",
        ),
        (
            config.output.intermediate / f"{prefix}_stage_03_attributes_enriched.parquet",
            work_config.output.intermediate / f"{prefix}_stage_03_attributes_enriched.parquet",
        ),
        (
            config.output.final / f"{prefix}_embedding_ready.parquet",
            work_config.output.final / f"{prefix}_embedding_ready.parquet",
        ),
        (
            config.output.final / f"{prefix}_search_ready.parquet",
            work_config.output.final / f"{prefix}_search_ready.parquet",
        ),
    ]
    prepared_root = work_config.output.root / "merged"
    prepared: list[tuple[Path, Path]] = []
    artifact_reports: dict[str, Any] = {}
    for baseline_path, delta_path in candidates:
        if not baseline_path.exists():
            continue
        prepared_path = prepared_root / baseline_path.relative_to(config.output.root)
        artifact_reports[baseline_path.name] = merge_parquet_delta(
            baseline_path,
            delta_path if delta_path.exists() else None,
            prepared_path,
            removed_ids=removed_ids,
        )
        prepared.append((prepared_path, baseline_path))
    return prepared, artifact_reports


def commit_merged_artifacts(
    prepared: list[tuple[Path, Path]],
    config: PipelineConfig,
) -> None:
    search_name = f"{config.artifact_prefix}_search_ready.parquet"
    ordered = sorted(prepared, key=lambda item: item[1].name == search_name)
    for prepared_path, destination in ordered:
        prepared_path.replace(destination)
        destination.with_suffix(".csv").unlink(missing_ok=True)


def archive_incremental_reports(
    config: PipelineConfig,
    work_config: PipelineConfig,
    run_id: str,
) -> Path:
    archive = config.output.reports / "incremental" / run_id
    archive.mkdir(parents=True, exist_ok=True)
    if work_config.output.reports.exists():
        shutil.copytree(
            work_config.output.reports,
            archive / "stage_reports",
            dirs_exist_ok=True,
        )
    if work_config.output.diagnostics.exists():
        shutil.copytree(
            work_config.output.diagnostics,
            archive / "diagnostics",
            dirs_exist_ok=True,
        )
    return archive


def cleanup_incremental_work(work_config: PipelineConfig) -> None:
    shutil.rmtree(work_config.output.root, ignore_errors=True)
