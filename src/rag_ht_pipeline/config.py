from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path("configs/pipeline.yaml")
DEFAULT_COMPANY = "gainr"
DEFAULT_COMPANIES_DIR = Path("configs/companies")
COMPANY_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


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
    company_id: str
    adapter: str
    artifact_prefix: str
    input_dir: Path
    data_dir: Path
    output: OutputLayout
    embedding_source_columns: list[str]
    bm25_source_columns: list[str]
    search_ready_columns: list[str]
    source: dict[str, Any]
    source_sync: dict[str, Any]
    mysql: dict[str, Any]
    postgres: dict[str, Any]
    destination: dict[str, Any]
    credentials: dict[str, Any]
    adapter_config: dict[str, Any]
    filter_columns: list[str]
    search_ready_types: dict[str, list[str]]
    config_path: Path


def _path(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def validate_company_slug(slug: str) -> str:
    if not COMPANY_SLUG_RE.fullmatch(slug):
        raise ValueError(
            f"Unsafe company slug {slug!r}; use lowercase letters, numbers, underscores, or hyphens."
        )
    return slug


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing pipeline config: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> PipelineConfig:
    project_root = Path.cwd()
    config_path = _path(project_root, config_path)
    raw = _read_yaml(config_path)
    extends = raw.pop("extends", None)
    if extends:
        base_path = Path(extends)
        if not base_path.is_absolute():
            base_path = (config_path.parent / base_path).resolve()
        raw = _deep_merge(_read_yaml(base_path), raw)

    paths = raw.get("paths", {})
    company = raw.get("company", {})
    company_id = validate_company_slug(str(company.get("id", DEFAULT_COMPANY)))
    adapter = str(company.get("adapter", "gainr")).strip()
    if not adapter:
        raise ValueError(f"Company {company_id!r} must configure company.adapter.")
    artifact_prefix = validate_company_slug(str(company.get("artifact_prefix", "ads")))
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
    source = dict(raw.get("source", {}))
    source_backend = str(source.get("backend", "csv")).lower()
    if source_backend not in {"csv", "mysql", "postgres"}:
        raise ValueError(
            f"Company {company_id!r} has unsupported source.backend={source_backend!r}; "
            "expected csv, mysql, or postgres."
        )
    source["backend"] = source_backend
    search_ready = raw.get("search_ready", {})
    search_ready_columns = list(search_ready.get("columns", []))
    canonical_columns = {"company_id", "id", "title", "description", "embedding_content", "bm25_content", "extras_json"}
    missing_canonical = sorted(canonical_columns - set(search_ready_columns))
    if missing_canonical:
        raise ValueError(
            f"Company {company_id!r} search_ready.columns is missing canonical columns: {missing_canonical}"
        )
    filter_columns = list(search_ready.get("filter_columns", []))
    missing_filters = sorted(set(filter_columns) - set(search_ready_columns))
    if missing_filters:
        raise ValueError(
            f"Company {company_id!r} filter columns are not present in search_ready.columns: {missing_filters}"
        )
    return PipelineConfig(
        project_root=project_root,
        company_id=company_id,
        adapter=adapter,
        artifact_prefix=artifact_prefix,
        input_dir=_path(project_root, paths.get("input_dir", ".")),
        data_dir=_path(project_root, paths.get("data_dir", "data")),
        output=output,
        embedding_source_columns=list(embedding.get("source_columns", [])),
        bm25_source_columns=list(bm25.get("source_columns", [])),
        search_ready_columns=search_ready_columns,
        source=source,
        source_sync=dict(raw.get("source_sync", {})),
        mysql=dict(raw.get("mysql", {})),
        postgres=dict(raw.get("postgres", {})),
        destination=dict(raw.get("destination", {})),
        credentials=dict(raw.get("credentials", {})),
        adapter_config=dict(raw.get("adapter_config", {})),
        filter_columns=filter_columns,
        search_ready_types={
            key: list(value)
            for key, value in dict(search_ready.get("types", {})).items()
        },
        config_path=config_path,
    )


def discover_company_profiles(companies_dir: Path = DEFAULT_COMPANIES_DIR) -> dict[str, Path]:
    project_root = Path.cwd()
    directory = _path(project_root, companies_dir)
    if not directory.exists():
        return {}
    profiles: dict[str, Path] = {}
    for path in sorted(directory.glob("*.yaml")):
        slug = validate_company_slug(path.stem)
        if slug in profiles:
            raise ValueError(f"Duplicate company profile: {slug}")
        raw = _read_yaml(path)
        configured = str(raw.get("company", {}).get("id", slug))
        if configured != slug:
            raise ValueError(f"Company profile {path} declares id={configured!r}; expected {slug!r}.")
        profiles[slug] = path
    return profiles


def load_company_config(
    company_id: str = DEFAULT_COMPANY,
    *,
    companies_dir: Path = DEFAULT_COMPANIES_DIR,
) -> PipelineConfig:
    slug = validate_company_slug(company_id)
    profiles = discover_company_profiles(companies_dir)
    if slug not in profiles:
        available = ", ".join(sorted(profiles)) or "none"
        raise FileNotFoundError(f"Unknown company {slug!r}; available profiles: {available}")
    return load_config(profiles[slug])


def ensure_output_dirs(config: PipelineConfig) -> None:
    for path in [
        config.output.root,
        config.output.intermediate,
        config.output.final,
        config.output.reports,
        config.output.diagnostics,
    ]:
        path.mkdir(parents=True, exist_ok=True)
