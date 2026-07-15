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
./scripts/setup.sh gainr
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
./scripts/setup.sh gainr --skip-tests
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

Gainr deliberately uses two database accounts:

```text
SOURCE_MYSQL_HOST=
SOURCE_MYSQL_PORT=3306
SOURCE_MYSQL_DATABASE=
SOURCE_MYSQL_USER=        # SELECT-only account
SOURCE_MYSQL_PASSWORD=

DEST_MYSQL_HOST=
DEST_MYSQL_PORT=3306
DEST_MYSQL_DATABASE=
DEST_MYSQL_USER=          # writer for ads_search_ready only
DEST_MYSQL_PASSWORD=
```

Separate users are recommended in production for least privilege, but they are
not required. A company may use the same database and user for both roles when
that account has source `SELECT` plus destination table publish privileges.
Never commit `.env`, and keep it owner-only:

```bash
chmod 600 .env
```

An administrator-ready account/grant template is available at
`deploy/sql/mysql_accounts.sql.example`. Prefer a separate destination database;
the writer needs table creation, indexing, atomic rename, and cleanup privileges
there because publishing uses staging and retained-previous tables.

Additional companies should use separate environment files and namespaced
variables declared by their profile.

To make separate source/destination users mandatory on a stricter deployment,
set `operations.preflight.require_separate_database_users: true` in that
company's profile. The shared default is `false`, so local and single-database
company installations receive a warning instead of being blocked.

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

Before exporting a table, scheduled database refreshes compare a lightweight
DB-side fingerprint (`COUNT`, maximum primary key, and maximum `updated_at`)
with the last committed successful run. Unchanged tables are not downloaded.
A mandatory full reconciliation runs at least every 24 hours to detect physical
deletions or source systems that failed to advance `updated_at`. Fingerprint
candidates are committed only after ETL succeeds, so a failed run cannot advance
the extraction watermark.

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

### Recommended 4 GB server installation

Production servers should use the provided systemd service and timer instead of
calling the pipeline directly from cron. The unit has persistent scheduling,
overlap protection, a four-hour timeout, process niceness, and 3/3.5 GB
`MemoryHigh`/`MemoryMax` limits.

Install the repository at `/opt/rag-ht`, create a locked-down `raght` service
account, then run:

```bash
sudo ./scripts/install_systemd.sh gainr
systemctl status rag-ht-etl@gainr.timer
systemctl status rag-ht-status.service
journalctl -u rag-ht-etl@gainr.service -f
```

Override the installation path when needed:

```bash
sudo RAG_HT_INSTALL_ROOT=/srv/rag-ht ./scripts/install_systemd.sh gainr
```

The timer runs hourly with a randomized delay. Missed timer executions run after
the server comes back online.

### Preflight, retry, and alerts

Every scheduled execution now checks free disk, available memory, credential-file
permissions, pending generations, and interrupted source-apply journals:

```bash
.venv/bin/rag-ht-ops preflight --company gainr
```

Transient database/network failures retry up to three times with exponential
backoff. Validation and schema failures are not retried blindly. Configure one
of these optional notification destinations in the service environment:

```text
RAG_HT_ALERT_WEBHOOK_URL=https://alerts.example/etl
RAG_HT_ALERT_EMAIL=ops@example.com
```

The webhook receives JSON containing `company_id`, `severity`, `message`, and
`sent_at`.

### Status and health

Inspect the latest run, validation, publish, and pending-generation state:

```bash
.venv/bin/rag-ht-ops status --company gainr
curl http://127.0.0.1:8787/health
curl http://127.0.0.1:8787/status
curl http://127.0.0.1:8787/companies/gainr/health
```

The single status service discovers every installed company profile. `/health`
and `/status` return an aggregate result; `/companies/<slug>/health` and
`/companies/<slug>/status` return one isolated company result. This avoids port
collisions when several companies run on one server. An endpoint returns HTTP
503 for failed, unknown, or stale status. By default a status older than 26
hours is stale. Each profile writes history and current status under its own
configured `paths.reports_dir`.

### Crash-safe resume

Applied source changes are recorded in `pending_source_run.json` before ETL
starts. If normalization, validation, or publishing fails, the next scheduled
execution resumes that exact change set instead of comparing the already-applied
snapshots and incorrectly reporting no changes.

Source-table replacement has its own atomic apply journal. If the process is
killed while files are being replaced, the next source sync restores the prior
complete snapshot before extracting again. Staging exports are deleted only
after the corresponding ETL generation succeeds.

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
output/reports/run_status.json
output/reports/run_history.jsonl
output/reports/quality_baseline.json
output/reports/streaming_verification_report.json
```

`--no-csv` keeps Parquet intermediates and final artifacts while suppressing
large CSV copies. This is recommended on memory-constrained production servers.

Source snapshot backups are stored under:

```text
output/source_sync/backups/
```

Only the latest successful timestamped backup is retained. This cleanup does not
affect company-managed database backups.

## Data-quality Gates

Publishing is blocked when canonical checks fail or configured quality metrics
regress. Gainr currently enforces:

- no duplicate or empty ad IDs
- no empty embedding or BM25 content
- no cross-company rows
- category resolution at least 99%
- city resolution at least 99%
- locality resolution at least 90%
- attribute mapping at least 85%
- no row-count drop greater than 10% from the last accepted baseline
- no join-ratio regression greater than five percentage points
- no incremental source change greater than 50% without operator review

Shared thresholds live under `quality` in `configs/base.yaml`; Gainr-specific
resolution thresholds live in `configs/companies/gainr.yaml`. Successful
validation updates `quality_baseline.json`; a failed gate prevents destination
changes.

## Publish Rollback

Publishing retains one previous destination table and verifies the live row
count after the atomic swap. Restore it without rebuilding ETL artifacts:

```bash
.venv/bin/rag-ht-pipeline --company gainr --rollback
```

Running rollback again swaps the two versions, so the action is reversible.
The report is written to `output/reports/rollback_report.json`.

## Multi-Company Operation

Company profiles live under:

```text
configs/companies/
```

`configs/base.yaml` contains only shared operational defaults. Gainr's source
tables, canonical columns, credentials, and destination settings live only in
`configs/companies/gainr.yaml`. `configs/pipeline.yaml` remains a compatibility
alias for older local Gainr commands; production automation should always use
`--company <company-slug>`.

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

1. Copy `configs/companies/example-flat.yaml.example` to
   `configs/companies/<slug>.yaml` and keep `extends: ../base.yaml`.
2. Set company-owned input, output, report, and source-sync paths.
3. Create an isolated `.env.<slug>` file with mode `600`.
4. Configure the company's read-only source user and separate restricted
   destination writer.
5. Add or select the company adapter for that company's schema.
6. Validate representative sanitized source samples.
7. Complete a full baseline run and publish dry-run.
8. Enable the company's timer with
   `sudo ./scripts/install_systemd.sh <slug>`.

Use `configs/companies/example-flat.yaml.example` as a profile reference.

## Development Verification

Run the test suite:

```bash
.venv/bin/python -m pytest -q
```

Verify a complete streaming rebuild against the current baseline without loading
both full datasets into memory:

```bash
.venv/bin/python scripts/verify_streaming_equivalence.py
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
