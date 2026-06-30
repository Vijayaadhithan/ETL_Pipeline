from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import source_sync, stage4_embedding_ready, stage5_search_ready
from .adapters import get_adapter
from .artifacts import organize_stage_artifacts, publish_final_aliases
from .config import (
    DEFAULT_COMPANIES_DIR,
    DEFAULT_COMPANY,
    PipelineConfig,
    discover_company_profiles,
    ensure_output_dirs,
    load_company_config,
    load_config,
)
from .publisher import credential_value, publish_company
from .validation import run_final_verification


LOGGER = logging.getLogger("rag_ht_pipeline")
SHARED_STAGE_ORDER = ["normalize", "embedding-ready", "search-ready", "validate"]
LEGACY_STAGES = ["category", "location", "attributes"]
STAGE_CHOICES = [*SHARED_STAGE_ORDER, *LEGACY_STAGES]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the isolated multi-company catalog enrichment pipeline.")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--company", default=None, help=f"Company profile slug (default: {DEFAULT_COMPANY}).")
    selection.add_argument("--all-companies", action="store_true", help="Run every configured company profile.")
    parser.add_argument("--companies-dir", type=Path, default=DEFAULT_COMPANIES_DIR)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Direct config path for compatibility; cannot be combined with --all-companies.",
    )
    parser.add_argument("--run-all", action="store_true", help="Normalize, build content, finalize, and validate.")
    parser.add_argument(
        "--stage",
        choices=STAGE_CHOICES,
        action="append",
        help="Run one or more specific stages. Legacy stages are supported by the Gainr adapter.",
    )
    parser.add_argument("--sample-size", type=int, default=None, help="Optional row limit for development runs.")
    parser.add_argument("--no-csv", action="store_true", help="Write parquet final artifacts without CSV copies.")
    parser.add_argument(
        "--refresh-source",
        choices=["configured", "csv", "mysql", "postgres"],
        default=None,
        help="Inspect/export source snapshots. Use configured to select each profile's source backend.",
    )
    parser.add_argument(
        "--apply-source-refresh",
        action="store_true",
        help="Replace this company's local snapshots after a database export.",
    )
    parser.add_argument(
        "--strict-subcategory-consistency",
        action="store_true",
        help="Exclude schema-mismatched bridge values in the Gainr adapter.",
    )
    publishing = parser.add_mutually_exclusive_group()
    publishing.add_argument(
        "--publish",
        action="store_true",
        help="Atomically publish validated data to the destination DB.",
    )
    publishing.add_argument(
        "--publish-dry-run",
        action="store_true",
        help="Validate publish configuration and final data without connecting or writing.",
    )
    args = parser.parse_args()
    if args.config and args.all_companies:
        parser.error("--config cannot be combined with --all-companies.")
    if args.config and args.company:
        parser.error("--config cannot be combined with --company.")
    return args


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")


def write_pipeline_report(config: PipelineConfig, reports: dict[str, Any]) -> Path:
    payload = {
        "company_id": config.company_id,
        "adapter": config.adapter,
        "config": str(config.config_path),
        "stages": reports,
    }
    output_path = config.output.reports / "pipeline_run_report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return output_path


def _profile_env_file(config: PipelineConfig) -> Path:
    path = Path(config.credentials.get("env_file", ".env"))
    return path if path.is_absolute() else config.project_root / path


def run_company_pipeline(args: argparse.Namespace, config: PipelineConfig) -> dict[str, Any]:
    ensure_output_dirs(config)
    adapter = get_adapter(config.adapter)
    publish_only = (args.publish or args.publish_dry_run) and not args.run_all and not args.stage
    stages = [] if publish_only else (SHARED_STAGE_ORDER if args.run_all or not args.stage else args.stage)
    reports: dict[str, Any] = {}

    if args.refresh_source:
        resolved_source = source_sync.resolve_source_backend(config, args.refresh_source)
        LOGGER.info(
            "[%s] refreshing source snapshots from %s",
            config.company_id,
            resolved_source,
        )
        reports["source-sync"] = source_sync.run_source_sync(
            config,
            source=resolved_source,
            apply=args.apply_source_refresh,
            env_file=_profile_env_file(config),
            sample_size=args.sample_size,
        )

    for stage in stages:
        LOGGER.info("[%s] running stage: %s", config.company_id, stage)
        if stage == "normalize":
            reports[stage] = adapter.normalize(
                config,
                sample_size=args.sample_size,
                strict_subcategory_consistency=args.strict_subcategory_consistency,
            )
        elif stage in LEGACY_STAGES:
            reports[stage] = adapter.run_legacy_stage(
                stage,
                config,
                sample_size=args.sample_size,
                strict_subcategory_consistency=args.strict_subcategory_consistency,
            )
        elif stage == "embedding-ready":
            reports[stage] = stage4_embedding_ready.run(
                config,
                sample_size=args.sample_size,
                no_csv=args.no_csv,
            )
            publish_final_aliases(config)
        elif stage == "search-ready":
            reports[stage] = stage5_search_ready.run(
                config,
                sample_size=args.sample_size,
                no_csv=args.no_csv,
            )
        elif stage == "validate":
            reports[stage] = run_final_verification(config, sample_size=args.sample_size)
            if reports[stage]["status"] != "PASS":
                raise RuntimeError(f"Final validation failed for company {config.company_id!r}.")
        organize_stage_artifacts(config)

    if args.publish or args.publish_dry_run:
        validation = reports.get("validate") or run_final_verification(config, sample_size=args.sample_size)
        reports["validate"] = validation
        if validation["status"] != "PASS":
            raise RuntimeError(f"Publishing blocked by failed validation for company {config.company_id!r}.")
        reports["publish"] = publish_company(config, dry_run=args.publish_dry_run and not args.publish)

    publish_final_aliases(config)
    organize_stage_artifacts(config)
    write_pipeline_report(config, reports)
    return reports


def selected_configs(args: argparse.Namespace) -> list[PipelineConfig]:
    if args.config:
        return [load_config(args.config)]
    if args.all_companies:
        profiles = discover_company_profiles(args.companies_dir)
        if not profiles:
            raise RuntimeError(f"No company profiles found under {args.companies_dir}.")
        configs = [
            load_company_config(company_id, companies_dir=args.companies_dir)
            for company_id in sorted(profiles)
        ]
        validate_company_isolation(configs)
        return configs
    return [load_company_config(args.company or DEFAULT_COMPANY, companies_dir=args.companies_dir)]


def validate_company_isolation(configs: list[PipelineConfig]) -> None:
    output_owners: dict[Path, str] = {}
    destination_owners: dict[tuple[str, str, str], str] = {}
    for config in configs:
        output_root = config.output.root.resolve()
        if output_root in output_owners:
            raise ValueError(
                f"Companies {output_owners[output_root]!r} and {config.company_id!r} share output root {output_root}."
            )
        output_owners[output_root] = config.company_id

        backend = str(config.destination.get("backend", "mysql")).lower()
        database_env = str(
            config.destination.get(
                "database_env",
                "MYSQL_DATABASE" if backend == "mysql" else "POSTGRES_DATABASE",
            )
        )
        schema = "" if backend == "mysql" else str(config.destination.get("schema", "public"))
        destination_key = (backend, database_env, schema)
        if destination_key in destination_owners:
            raise ValueError(
                f"Companies {destination_owners[destination_key]!r} and {config.company_id!r} "
                f"share destination boundary {destination_key}."
            )
        destination_owners[destination_key] = config.company_id


def validate_resolved_destination_isolation(configs: list[PipelineConfig]) -> None:
    owners: dict[tuple[str, str, str, str], str] = {}
    for config in configs:
        backend = str(config.destination.get("backend", "mysql")).lower()
        if backend == "mysql":
            host = credential_value(config, "host_env", "MYSQL_HOST", default="localhost")
            port = credential_value(config, "port_env", "MYSQL_PORT", default="3306")
            database = credential_value(config, "database_env", "MYSQL_DATABASE")
            schema = ""
        elif backend == "postgres":
            host = credential_value(config, "host_env", "POSTGRES_HOST", default="localhost")
            port = credential_value(config, "port_env", "POSTGRES_PORT", default="5432")
            database = credential_value(config, "database_env", "POSTGRES_DATABASE")
            schema = str(config.destination.get("schema", "public"))
        else:
            raise ValueError(f"Unsupported destination backend {backend!r}.")
        boundary = (backend, f"{host}:{port}", database, schema)
        if boundary in owners:
            raise ValueError(
                f"Companies {owners[boundary]!r} and {config.company_id!r} resolve to the same "
                f"destination database/schema."
            )
        owners[boundary] = config.company_id


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    """Backward-compatible single-company programmatic entry point."""
    configs = selected_configs(args)
    if len(configs) != 1:
        raise ValueError("run_pipeline supports one company; use run_batch for --all-companies.")
    return run_company_pipeline(args, configs[0])


def run_batch(args: argparse.Namespace) -> dict[str, Any]:
    configs = selected_configs(args)
    if len(configs) > 1 and (getattr(args, "publish", False) or getattr(args, "publish_dry_run", False)):
        validate_resolved_destination_isolation(configs)
    started = datetime.now(timezone.utc)
    result: dict[str, Any] = {
        "started_at": started.isoformat(),
        "finished_at": "",
        "status": "PASS",
        "companies": {},
    }
    for config in configs:
        try:
            reports = run_company_pipeline(args, config)
            result["companies"][config.company_id] = {
                "status": "PASS",
                "adapter": config.adapter,
                "reports": reports,
            }
        except Exception as exc:
            LOGGER.exception("[%s] pipeline failed", config.company_id)
            result["status"] = "FAIL"
            result["companies"][config.company_id] = {
                "status": "FAIL",
                "adapter": config.adapter,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    batch_report = Path.cwd() / "output" / "reports" / "company_batch_report.json"
    batch_report.parent.mkdir(parents=True, exist_ok=True)
    batch_report.write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    result["batch_report"] = str(batch_report)
    return result


def print_summary(result: dict[str, Any]) -> None:
    print("\nMulti-company pipeline complete")
    print(f"Status: {result['status']}")
    for company_id, company in result["companies"].items():
        detail = f": {company['error']}" if company["status"] == "FAIL" else ""
        print(f"  - {company_id}: {company['status']}{detail}")
    print(f"Batch report: {result['batch_report']}")


def main() -> None:
    configure_logging()
    args = parse_args()
    result = run_batch(args)
    print_summary(result)
    if result["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
