from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
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
from .incremental import (
    archive_incremental_reports,
    cleanup_incremental_work,
    commit_merged_artifacts,
    incremental_work_config,
    load_change_set,
    missing_incremental_baselines,
    prepare_merged_artifacts,
)
from .publisher import credential_value, publish_company, rollback_company
from .publisher import validate_publish_file
from .operations import finish_run, start_run, update_run, utc_now
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
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Use Parquet-only intermediate/final artifacts and skip CSV copies.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="After an applied source refresh, transform and merge only changed records.",
    )
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
    publishing.add_argument(
        "--rollback",
        action="store_true",
        help="Atomically swap the retained previous destination table back into service.",
    )
    args = parser.parse_args()
    if args.config and args.all_companies:
        parser.error("--config cannot be combined with --all-companies.")
    if args.config and args.company:
        parser.error("--config cannot be combined with --company.")
    if args.incremental:
        if not args.run_all or args.stage:
            parser.error("--incremental requires --run-all and cannot be combined with --stage.")
        if not args.refresh_source or not args.apply_source_refresh:
            parser.error(
                "--incremental requires --refresh-source and --apply-source-refresh."
            )
        if args.sample_size is not None:
            parser.error("--incremental cannot be combined with --sample-size.")
        if not args.no_csv:
            parser.error("--incremental requires --no-csv to avoid stale full CSV copies.")
    return args


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")


def write_pipeline_report(config: PipelineConfig, reports: dict[str, Any]) -> Path:
    payload = {
        "status": "PASS",
        "generated_at": utc_now(),
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


def run_incremental_update(
    config: PipelineConfig,
    adapter: Any,
    change_set: dict[str, Any],
    *,
    strict_subcategory_consistency: bool,
) -> dict[str, Any]:
    changed_ids = {str(value) for value in change_set["changed_ids"]}
    removed_ids = {str(value) for value in change_set["removed_ids"]}
    work_config, run_id = incremental_work_config(config)
    reports: dict[str, Any] = {
        "mode": "incremental",
        "run_id": run_id,
        "changed_id_count": len(changed_ids),
        "removed_id_count": len(removed_ids),
        "changed_ids": sorted(changed_ids),
        "removed_ids": sorted(removed_ids),
    }
    committed = False
    try:
        if changed_ids:
            stage_started = perf_counter()
            reports["normalize"] = adapter.normalize(
                work_config,
                strict_subcategory_consistency=strict_subcategory_consistency,
                no_csv=True,
                record_ids=changed_ids,
            )
            LOGGER.info(
                "[%s] incremental normalize completed in %.2fs",
                config.company_id,
                perf_counter() - stage_started,
            )
            stage_started = perf_counter()
            reports["embedding-ready"] = stage4_embedding_ready.run(
                work_config,
                no_csv=True,
            )
            LOGGER.info(
                "[%s] incremental embedding-ready completed in %.2fs",
                config.company_id,
                perf_counter() - stage_started,
            )
            stage_started = perf_counter()
            reports["search-ready"] = stage5_search_ready.run(
                work_config,
                no_csv=True,
            )
            LOGGER.info(
                "[%s] incremental search-ready completed in %.2fs",
                config.company_id,
                perf_counter() - stage_started,
            )

        prepared, artifact_reports = prepare_merged_artifacts(
            config,
            work_config,
            removed_ids=removed_ids,
        )
        reports["artifacts"] = artifact_reports
        prepared_search = next(
            (
                prepared_path
                for prepared_path, destination in prepared
                if destination.name
                == f"{config.artifact_prefix}_search_ready.parquet"
            ),
            None,
        )
        if prepared_search is None:
            raise RuntimeError("Incremental merge did not prepare a search-ready artifact.")
        reports["pre_commit_validation"] = validate_publish_file(
            config,
            prepared_search,
        )
        reports["archive"] = str(
            archive_incremental_reports(config, work_config, run_id)
        )
        commit_merged_artifacts(prepared, config)
        committed = True
    finally:
        if committed:
            cleanup_incremental_work(work_config)
        else:
            reports["preserved_work_dir"] = str(work_config.output.root)
    return reports


def run_company_pipeline(args: argparse.Namespace, config: PipelineConfig) -> dict[str, Any]:
    ensure_output_dirs(config)
    adapter = get_adapter(config.adapter)
    publish_only = (args.publish or args.publish_dry_run or args.rollback) and not args.run_all and not args.stage
    stages = [] if publish_only else (SHARED_STAGE_ORDER if args.run_all or not args.stage else args.stage)
    reports: dict[str, Any] = {}

    if args.rollback:
        update_run(config, stage="rollback")
        reports["rollback"] = rollback_company(config)
        write_pipeline_report(config, reports)
        return reports

    source_sync_report: dict[str, Any] | None = None
    pending_source = source_sync.load_pending_source_run(config) if args.incremental else None
    if pending_source and pending_source.get("applied"):
        LOGGER.warning(
            "[%s] resuming pending source generation %s",
            config.company_id,
            pending_source.get("run_id"),
        )
        update_run(config, stage="resume-pending-source", pending_source_run=pending_source)
        source_sync_report = {
            "resumed": True,
            "incremental": {
                "change_set_path": pending_source["change_set_path"],
            },
        }
        reports["source-sync-resume"] = pending_source
    elif args.refresh_source:
        update_run(config, stage="source-sync")
        resolved_source = source_sync.resolve_source_backend(config, args.refresh_source)
        LOGGER.info(
            "[%s] refreshing source snapshots from %s",
            config.company_id,
            resolved_source,
        )
        source_sync_report = source_sync.run_source_sync(
            config,
            source=resolved_source,
            apply=args.apply_source_refresh,
            env_file=_profile_env_file(config),
            sample_size=args.sample_size,
        )
        reports["source-sync"] = source_sync_report

    if args.incremental:
        if source_sync_report is None:
            raise RuntimeError("Incremental execution requires a source refresh report.")
        change_set_path = Path(
            source_sync_report.get("incremental", {}).get("change_set_path", "")
        )
        change_set = load_change_set(change_set_path)
        if change_set["company_id"] != config.company_id:
            raise ValueError(
                f"Incremental change set belongs to {change_set['company_id']!r}, "
                f"not {config.company_id!r}."
            )
        missing_baselines = missing_incremental_baselines(config)
        source_mode = change_set["mode"]
        if source_mode == "invalid":
            raise RuntimeError(
                f"Source refresh failed safety checks: {change_set['reason']}"
            )
        if source_mode == "incremental" and not missing_baselines:
            reports["incremental"] = run_incremental_update(
                config,
                adapter,
                change_set,
                strict_subcategory_consistency=args.strict_subcategory_consistency,
            )
            stages = ["validate"]
        elif source_mode == "no_changes" and not missing_baselines:
            reports["incremental"] = {
                "mode": "no_changes",
                "reason": change_set["reason"],
                "changed_id_count": 0,
                "removed_id_count": 0,
            }
            stages = ["validate"]
        else:
            reason = change_set["reason"]
            if missing_baselines:
                reason = (
                    "A full rebuild is required because baseline artifacts are missing: "
                    + ", ".join(str(path) for path in missing_baselines)
                )
            reports["incremental"] = {
                "mode": "full_fallback",
                "source_mode": source_mode,
                "reason": reason,
                "invalidating_tables": change_set.get("invalidating_tables", []),
            }
            LOGGER.info("[%s] incremental fallback: %s", config.company_id, reason)

    for stage in stages:
        update_run(config, stage=stage)
        LOGGER.info("[%s] running stage: %s", config.company_id, stage)
        stage_started = perf_counter()
        if stage == "normalize":
            reports[stage] = adapter.normalize(
                config,
                sample_size=args.sample_size,
                strict_subcategory_consistency=args.strict_subcategory_consistency,
                no_csv=args.no_csv,
            )
        elif stage in LEGACY_STAGES:
            reports[stage] = adapter.run_legacy_stage(
                stage,
                config,
                sample_size=args.sample_size,
                strict_subcategory_consistency=args.strict_subcategory_consistency,
                no_csv=args.no_csv,
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
        LOGGER.info(
            "[%s] completed stage %s in %.2fs",
            config.company_id,
            stage,
            perf_counter() - stage_started,
        )

    if args.publish or args.publish_dry_run:
        update_run(config, stage="publish-dry-run" if args.publish_dry_run else "publish")
        validation = reports.get("validate") or run_final_verification(config, sample_size=args.sample_size)
        reports["validate"] = validation
        if validation["status"] != "PASS":
            raise RuntimeError(f"Publishing blocked by failed validation for company {config.company_id!r}.")
        reports["publish"] = publish_company(config, dry_run=args.publish_dry_run and not args.publish)

    if args.incremental:
        reports["source-fingerprints-committed"] = source_sync.commit_source_fingerprints(config)
        source_sync.clear_pending_source_run(config)
        reports["source-staging-cleanup"] = {
            "removed": source_sync.cleanup_source_staging(config),
        }
        shutil.rmtree(config.output.root / "incremental" / "work", ignore_errors=True)

    publish_final_aliases(config)
    organize_stage_artifacts(config)
    if args.incremental and "incremental" in reports:
        incremental_report = config.output.reports / "incremental_run_report.json"
        incremental_report.write_text(
            json.dumps(
                reports["incremental"],
                indent=2,
                ensure_ascii=False,
                default=str,
            ),
            encoding="utf-8",
        )
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
    if len(configs) > 1 and (
        getattr(args, "publish", False)
        or getattr(args, "publish_dry_run", False)
        or getattr(args, "rollback", False)
    ):
        validate_resolved_destination_isolation(configs)
    started = datetime.now(timezone.utc)
    result: dict[str, Any] = {
        "started_at": started.isoformat(),
        "finished_at": "",
        "status": "PASS",
        "companies": {},
    }
    for config in configs:
        start_run(config, command=sys.argv)
        try:
            reports = run_company_pipeline(args, config)
            result["companies"][config.company_id] = {
                "status": "PASS",
                "adapter": config.adapter,
                "reports": reports,
            }
            finish_run(
                config,
                status="PASS",
                summary={
                    "incremental": reports.get("incremental", {}),
                    "publish": reports.get("publish", {}),
                },
            )
        except Exception as exc:
            LOGGER.exception("[%s] pipeline failed", config.company_id)
            result["status"] = "FAIL"
            result["companies"][config.company_id] = {
                "status": "FAIL",
                "adapter": config.adapter,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            finish_run(config, status="FAIL", error=exc)
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
