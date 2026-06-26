from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path("configs/pipeline.yaml")


@dataclass(frozen=True)
class OutputLayout:
    root: Path
    intermediate: Path
    final: Path
    reports: Path
    diagnostics: Path


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path
    input_dir: Path
    data_dir: Path
    output: OutputLayout
    embedding_source_columns: list[str]
    bm25_source_columns: list[str]
    mysql: dict[str, Any]
    postgres: dict[str, Any]


def _path(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> PipelineConfig:
    project_root = Path.cwd()
    config_path = _path(project_root, config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Missing pipeline config: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    paths = raw.get("paths", {})
    output_root = _path(project_root, paths.get("output_root", "output"))
    output = OutputLayout(
        root=output_root,
        intermediate=_path(project_root, paths.get("intermediate_dir", output_root / "intermediate")),
        final=_path(project_root, paths.get("final_dir", output_root / "final")),
        reports=_path(project_root, paths.get("reports_dir", output_root / "reports")),
        diagnostics=_path(project_root, paths.get("diagnostics_dir", output_root / "diagnostics")),
    )

    embedding = raw.get("embedding", {})
    bm25 = raw.get("bm25", {})
    return PipelineConfig(
        project_root=project_root,
        input_dir=_path(project_root, paths.get("input_dir", ".")),
        data_dir=_path(project_root, paths.get("data_dir", "data")),
        output=output,
        embedding_source_columns=list(embedding.get("source_columns", [])),
        bm25_source_columns=list(bm25.get("source_columns", [])),
        mysql=dict(raw.get("mysql", {})),
        postgres=dict(raw.get("postgres", {})),
    )


def ensure_output_dirs(config: PipelineConfig) -> None:
    for path in [
        config.output.root,
        config.output.intermediate,
        config.output.final,
        config.output.reports,
        config.output.diagnostics,
    ]:
        path.mkdir(parents=True, exist_ok=True)
