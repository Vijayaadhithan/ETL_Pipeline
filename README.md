# RAG_HT ETL Pipeline

Production-oriented, multi-company ETL for converting company catalog data into
a validated, search-ready dataset.

The pipeline reads company source data, applies company-specific normalization,
builds retrieval content, validates the result, and optionally publishes it to
an isolated destination database. It prepares text for vector and keyword
search; embedding generation itself is handled by a downstream pipeline.

## Architecture

```text
Company source database
        |
        v
Read-only source refresh and local snapshots
        |
        v
Company adapter and normalization
        |
        v
Canonical catalog and retrieval content
        |
        v
Validation
        |
        v
Parquet artifact --> optional atomic database publish
```

Operational guarantees:

- Each company has its own profile, credentials, paths, reports, and destination.
- Source database access is read-only.
- Destination writes occur only with `--publish`.
- Publishing uses a staging table and atomic promotion.
- Failed validation prevents publishing.
- Incremental runs process changed records and safely fall back to a full rebuild
  when shared reference data changes.
- Generated source-snapshot backups retain only the latest successful backup.

## Requirements

- Linux or macOS
- Python 3.11 or newer
- MySQL/MariaDB or PostgreSQL network access, depending on the company profile

## Installation

Run from the cloned repository:

```bash
cd /path/to/ETL_Pipeline
./scripts/setup.sh
```

The setup script:

- creates or reuses `.venv`
- installs the package and database drivers
- creates `.env` from `.env.example` only when `.env` is missing
- preserves existing credentials
- creates output directories
- validates company profiles
- runs the test suite

For a minimal production installation:

```bash
./scripts/setup.sh --skip-tests
```

Verify the installation:

```bash
.venv/bin/rag-ht-pipeline --help
.venv/bin/rag-ht-source-sync --help
```

## Credentials

Copy and edit the environment file if setup has not already created it:

```bash
cp .env.example .env
nano .env
```

Gainr uses:

```text
MYSQL_HOST=
MYSQL_PORT=3306
MYSQL_DATABASE=
MYSQL_USER=
MYSQL_PASSWORD=
```

Never commit `.env`. Additional companies should use separate environment files
and namespaced variables declared by their profile.

## Gainr Production Runbook

All commands below must be run from the repository root.

### 1. Test the source connection

This exports a ten-row sample into the source-sync staging area and does not
replace the active snapshots:

```bash
.venv/bin/rag-ht-source-sync \
  --config configs/companies/gainr.yaml \
  --source configured \
  --sample-size 10
```

Review:

```text
output/reports/source_sync_report.json
output/reports/source_table_changes.csv
```

### 2. First production run

The first successful run creates the full baseline required for later
incremental updates:

```bash
PYTHONUNBUFFERED=1 .venv/bin/rag-ht-pipeline \
  --company gainr \
  --refresh-source configured \
  --apply-source-refresh \
  --run-all \
  --no-csv
```

This command:

- reads the configured source database
- backs up the previous local snapshots
- applies the new snapshots
- builds all ETL stages
- validates the final result
- does not write to the destination table

### 3. Validate publishing

Validate the existing final artifact and destination configuration without
connecting to or writing the destination:

```bash
.venv/bin/rag-ht-pipeline \
  --company gainr \
  --publish-dry-run
```

### 4. Publish the validated result

```bash
.venv/bin/rag-ht-pipeline \
  --company gainr \
  --publish
```

This publishes the existing validated artifact without rerunning the ETL.

## Subsequent Incremental Runs

Use this command after the first successful baseline:

```bash
PYTHONUNBUFFERED=1 .venv/bin/rag-ht-pipeline \
  --company gainr \
  --refresh-source configured \
  --apply-source-refresh \
  --run-all \
  --incremental \
  --no-csv
```

To publish after successful validation:

```bash
PYTHONUNBUFFERED=1 .venv/bin/rag-ht-pipeline \
  --company gainr \
  --refresh-source configured \
  --apply-source-refresh \
  --run-all \
  --incremental \
  --no-csv \
  --publish
```

Incremental behavior:

- new and updated records are rebuilt
- changes in dependent record data rebuild the affected records
- deleted records are removed
- no changes skip the expensive transformation stages
- missing baselines trigger a full rebuild
- shared category, location, or attribute reference changes trigger a full
  rebuild because they may affect many records

The source snapshots are still compared to detect updates and deletions.
Publishing still loads the complete merged final artifact to preserve atomic
table replacement.

## Scheduled Execution

The guarded runner performs source refresh, incremental processing, validation,
and optional publishing:

```bash
./scripts/run_scheduled_etl.sh gainr --publish
```

It prevents overlapping runs for the same company. It is safe to use from the
first scheduled execution because missing baselines automatically cause a full
rebuild.

Example hourly cron entry:

```cron
0 * * * * cd /path/to/ETL_Pipeline && ./scripts/run_scheduled_etl.sh gainr --publish >> output/reports/scheduled_gainr.log 2>&1
```

Install and verify:

```bash
crontab -e
crontab -l
systemctl status cron
```

Watch scheduled output:

```bash
tail -f output/reports/scheduled_gainr.log
```

Omit `--publish` when the schedule should only create and validate local
artifacts.

## Monitoring a Running ETL

Find the process:

```bash
pgrep -af '[r]ag-ht-pipeline|[r]ag_ht_pipeline.pipeline'
```

Inspect CPU, memory, and elapsed time:

```bash
PID=$(pgrep -f '[r]ag-ht-pipeline|[r]ag_ht_pipeline.pipeline' | head -1)
ps -p "$PID" -o pid,etime,%cpu,%mem,rss,state,cmd
```

Check server resources:

```bash
free -h
df -h /
```

Watch artifact progress:

```bash
watch -n 10 'find output -type f -printf "%TY-%Tm-%Td %TH:%TM:%TS %10s %p\n" | sort | tail -20'
```

If a full run completed `embedding-ready` but stopped during `search-ready`,
resume without repeating source refresh, normalization, or content generation:

```bash
PYTHONUNBUFFERED=1 .venv/bin/rag-ht-pipeline \
  --company gainr \
  --stage search-ready \
  --stage validate \
  --no-csv
```

If the log shows `search-ready` completed and only `validate` failed, run:

```bash
PYTHONUNBUFFERED=1 .venv/bin/rag-ht-pipeline \
  --company gainr \
  --stage validate \
  --no-csv
```

## Outputs and Reports

Authoritative Gainr output:

```text
output/final/ads_search_ready.parquet
```

Main operational reports:

```text
output/reports/source_sync_report.json
output/reports/source_table_changes.csv
output/reports/incremental_change_set.json
output/reports/incremental_run_report.json
output/reports/pipeline_run_report.json
output/reports/final_output_correctness_report.json
output/reports/publish_report.json
```

`--no-csv` keeps Parquet intermediates and final artifacts while suppressing
large CSV copies. This is recommended on memory-constrained production servers.

Source snapshot backups are stored under:

```text
output/source_sync/backups/
```

Only the latest successful timestamped backup is retained. This cleanup does not
affect company-managed database backups.

## Multi-Company Operation

Company profiles live under:

```text
configs/companies/
```

Run one company:

```bash
.venv/bin/rag-ht-pipeline --company <company-slug> --run-all --no-csv
```

Run every configured company:

```bash
.venv/bin/rag-ht-pipeline \
  --all-companies \
  --refresh-source configured \
  --apply-source-refresh \
  --run-all \
  --incremental \
  --no-csv
```

Add `--publish` only when every successful company result should be published.
One company failure does not stop the others, but the batch exits nonzero if any
company fails.

To onboard a company:

1. Add `configs/companies/<slug>.yaml`.
2. Configure isolated source and destination credentials.
3. Add or select the company adapter.
4. Validate representative sanitized source samples.
5. Complete a full baseline run.
6. Enable incremental scheduling.

Use `configs/companies/example-flat.yaml.example` as a profile reference.

## Development Verification

Run the test suite:

```bash
.venv/bin/python -m pytest -q
```

Run a small local verification without publishing:

```bash
.venv/bin/rag-ht-pipeline \
  --company gainr \
  --run-all \
  --sample-size 1000 \
  --no-csv
```

`--sample-size` is for development only and cannot be combined with
`--incremental`.

## Operational Safety

- Do not run production database commands with sample credentials.
- Do not commit `.env`, source snapshots, output artifacts, or reports.
- Use `--publish-dry-run` before the first destination publish.
- Keep `--no-csv` enabled on the 4 GB production server.
- Inspect validation and source-change reports after configuration or schema
  changes.
