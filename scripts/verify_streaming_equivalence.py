from __future__ import annotations

import argparse
import logging
import platform
import resource
import shutil
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq

from rag_ht_pipeline.adapters import get_adapter
from rag_ht_pipeline.config import OutputLayout, load_company_config
from rag_ht_pipeline.operations import atomic_write_json, utc_now
from rag_ht_pipeline.stage4_embedding_ready import run as build_embedding_ready
from rag_ht_pipeline.stage5_search_ready import run as build_search_ready
from rag_ht_pipeline.validation import run_final_verification


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild with streaming stages and compare with the current baseline.")
    parser.add_argument("--company", default="gainr")
    parser.add_argument("--work-dir", type=Path, default=Path("/tmp/rag-ht-streaming-verification"))
    parser.add_argument("--keep", action="store_true")
    return parser.parse_args()


def compare_parquet(expected_path: Path, actual_path: Path) -> dict[str, object]:
    expected = pq.ParquetFile(expected_path)
    actual = pq.ParquetFile(actual_path)
    if expected.schema_arrow != actual.schema_arrow:
        raise AssertionError("Parquet schemas differ.")
    if expected.metadata.num_rows != actual.metadata.num_rows:
        raise AssertionError(
            f"Row counts differ: expected={expected.metadata.num_rows}, actual={actual.metadata.num_rows}"
        )
    expected_batches = expected.iter_batches(batch_size=25_000)
    actual_batches = actual.iter_batches(batch_size=25_000)
    batches = 0
    while True:
        expected_batch = next(expected_batches, None)
        actual_batch = next(actual_batches, None)
        if expected_batch is None or actual_batch is None:
            if expected_batch is not actual_batch:
                raise AssertionError("Parquet batch counts differ.")
            break
        batches += 1
        if not expected_batch.equals(actual_batch):
            raise AssertionError(f"Parquet values differ in batch {batches}.")
    return {
        "rows": expected.metadata.num_rows,
        "columns": len(expected.schema_arrow.names),
        "batches_compared": batches,
        "values_equal": True,
    }


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    base = load_company_config(args.company)
    expected = base.output.final / f"{base.artifact_prefix}_search_ready.parquet"
    if not expected.exists():
        raise FileNotFoundError(f"Missing comparison baseline: {expected}")
    if args.work_dir.exists():
        shutil.rmtree(args.work_dir)
    output = OutputLayout(
        root=args.work_dir,
        intermediate=args.work_dir / "intermediate",
        final=args.work_dir / "final",
        reports=args.work_dir / "reports",
        diagnostics=args.work_dir / "diagnostics",
    )
    config = replace(base, output=output)
    print(
        f"Starting full streaming verification for {config.company_id}. "
        "Progress is reported every 25,000 rows.",
        flush=True,
    )
    print("[1/5] Normalizing category, location, and attribute data...", flush=True)
    get_adapter(config.adapter).normalize(config, no_csv=True)
    print("[2/5] Building embedding and BM25 content...", flush=True)
    build_embedding_ready(config, no_csv=True)
    print("[3/5] Building search-ready output...", flush=True)
    build_search_ready(config, no_csv=True)
    print("[4/5] Running final quality validation...", flush=True)
    validation = run_final_verification(config)
    if validation["status"] != "PASS":
        raise AssertionError(f"Streaming output validation failed: {validation}")
    actual = output.final / f"{config.artifact_prefix}_search_ready.parquet"
    print("[5/5] Comparing every output value with the current baseline...", flush=True)
    comparison = compare_parquet(expected, actual)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    comparison["peak_rss_mb"] = round(
        peak / (1024 * 1024) if platform.system() == "Darwin" else peak / 1024,
        1,
    )
    comparison["verified_at"] = utc_now()
    atomic_write_json(
        base.output.reports / "streaming_verification_report.json",
        comparison,
    )
    print("Verification PASS", flush=True)
    print(comparison, flush=True)
    if not args.keep:
        shutil.rmtree(args.work_dir)


if __name__ == "__main__":
    main()
