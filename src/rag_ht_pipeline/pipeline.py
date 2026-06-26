from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from . import (
    source_sync,
    stage1_category,
    stage2_location,
    stage3_attributes,
    stage4_embedding_ready,
    stage5_search_ready,
)
from .artifacts import organize_stage_artifacts, publish_final_aliases
from .config import DEFAULT_CONFIG_PATH, ensure_output_dirs, load_config
from .validation import run_final_verification


LOGGER = logging.getLogger("rag_ht_pipeline")
STAGE_ORDER = ["category", "location", "attributes", "embedding-ready", "search-ready", "validate"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the rental marketplace enrichment pipeline.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Pipeline YAML config.")
    parser.add_argument("--run-all", action="store_true", help="Run all stages in order.")
    parser.add_argument(
        "--stage",
        choices=STAGE_ORDER,
        action="append",
        help="Run one or more specific stages. Can be repeated.",
    )
    parser.add_argument("--sample-size", type=int, default=None, help="Optional row limit for development runs.")
    parser.add_argument("--no-csv", action="store_true", help="Skip Stage 4 CSV output and write parquet only.")
    parser.add_argument(
        "--refresh-source",
        choices=["csv", "mysql", "postgres"],
        default=None,
        help="Run source snapshot inspection/export before enrichment.",
    )
    parser.add_argument(
        "--apply-source-refresh",
        action="store_true",
        help="Replace local source CSVs from the database export. Only applies with --refresh-source mysql/postgres.",
    )
    parser.add_argument(
        "--strict-subcategory-consistency",
        action="store_true",
        help="Exclude schema-mismatched bridge values from Stage 3 aggregates.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")


def write_pipeline_report(config_path: Path, reports: dict[str, Any], output_path: Path) -> None:
    payload = {
        "config": str(config_path),
        "stages": reports,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    config = load_config(args.config)
    ensure_output_dirs(config)

    stages = STAGE_ORDER if args.run_all or not args.stage else args.stage
    reports: dict[str, Any] = {}

    if args.refresh_source:
        LOGGER.info("Refreshing source snapshots from: %s", args.refresh_source)
        reports["source-sync"] = source_sync.run_source_sync(
            config,
            source=args.refresh_source,
            apply=args.apply_source_refresh,
            sample_size=args.sample_size,
        )

    for stage in stages:
        LOGGER.info("Running stage: %s", stage)
        if stage == "category":
            reports[stage] = stage1_category.run(config, sample_size=args.sample_size)
        elif stage == "location":
            reports[stage] = stage2_location.run(config, sample_size=args.sample_size)
        elif stage == "attributes":
            reports[stage] = stage3_attributes.run(
                config,
                sample_size=args.sample_size,
                strict_subcategory_consistency=args.strict_subcategory_consistency,
            )
        elif stage == "embedding-ready":
            reports[stage] = stage4_embedding_ready.run(config, sample_size=args.sample_size, no_csv=args.no_csv)
            publish_final_aliases(config)
        elif stage == "search-ready":
            reports[stage] = stage5_search_ready.run(config, sample_size=args.sample_size, no_csv=args.no_csv)
        elif stage == "validate":
            reports[stage] = run_final_verification(config, sample_size=args.sample_size)
        organize_stage_artifacts(config)

    publish_final_aliases(config)
    organize_stage_artifacts(config)
    write_pipeline_report(args.config, reports, config.output.reports / "pipeline_run_report.json")
    return reports


def print_summary(reports: dict[str, Any]) -> None:
    print("\nPipeline complete")
    for stage in reports:
        print(f"  - {stage}")


def main() -> None:
    configure_logging()
    args = parse_args()
    reports = run_pipeline(args)
    print_summary(reports)


if __name__ == "__main__":
    main()
