# RAG_HT Multi-Company Catalog Pipeline

This repository builds verified, search-ready catalog datasets from company-specific source schemas. Each company has an adapter for its own joins and normalization, while shared stages build semantic/keyword text, validate the canonical contract, and optionally publish to an isolated database.

It does **not** create vector embeddings yet. The current final output is clean retrieval/search text plus structured metadata.

## New System Setup

Prerequisites:

- Linux or macOS
- Python 3.11 or newer
- Network access to the configured Python package index during installation

After cloning or copying the repository code:

```bash
cd /path/to/RAG_HT
./scripts/setup.sh
```

The setup script:

- creates or reuses `.venv`
- installs the ETL package and all MySQL/PostgreSQL Python drivers
- installs test dependencies
- creates `.env` from `.env.example` only when `.env` is missing
- preserves every existing `.env`
- creates empty output directories
- verifies company profiles and adapters
- runs the test suite

It does not copy, download, generate, or modify company CSV/source data.

Skip tests when preparing a minimal runtime:

```bash
./scripts/setup.sh --skip-tests
```

Use a specific Python executable when needed:

```bash
PYTHON_BIN=/usr/bin/python3.11 ./scripts/setup.sh
```

The editable installation also provides shorter commands:

```bash
.venv/bin/rag-ht-pipeline --help
.venv/bin/rag-ht-source-sync --help
```

## Gainr Quick Start

Run these commands from the repository root.

Normal Gainr ETL using the current local CSV files:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --run-all
```

After running `scripts/setup.sh`, the equivalent shorter command is:

```bash
.venv/bin/rag-ht-pipeline \
  --company gainr \
  --run-all
```

Fast 1,000-row verification run:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --run-all \
  --sample-size 1000 \
  --no-csv
```

Refresh Gainr source tables from MySQL, back up and replace the local CSV snapshots, then rebuild:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --refresh-source mysql \
  --apply-source-refresh \
  --run-all
```

Validate the existing final artifact and publishing configuration without writing to MySQL:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --publish-dry-run
```

Rebuild and atomically publish `ads_search_ready` to Gainr's configured destination database:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --run-all \
  --publish
```

The normal and sample commands only write local artifacts under `output/`. Database publishing happens only when `--publish` is explicitly supplied.

## Multi-Company Model

Company profiles live in:

```text
configs/companies/
```

The current company is `gainr`, which remains the default and keeps the existing root input files, `output/` layout, and `ads_search_ready` table name.

Additional companies use isolated paths:

```text
data/companies/<company>/incoming/
output/companies/<company>/
```

Each profile independently selects `source.backend: csv`, `mysql`, or `postgres`. PostgreSQL sources may set a default `source.schema`, and any table may override it with `db_schema`. The destination backend is configured separately, so a company can read from PostgreSQL and publish to MySQL, or the reverse.

One company profile currently represents one source database connection. If a company needs joins across multiple databases, implement that explicitly in its adapter rather than mixing credentials or cross-database joins into the shared pipeline.

The adapters currently available are:

- `gainr`: the existing relational ads/category/location/attribute pipeline.
- `flat_catalog`: a single-file catalog adapter with configurable source-to-canonical field mapping.

Adapters must emit `company_id`, `id`, `title`, `description`, and `extras_json`. Shared stages add `embedding_content` and `bm25_content`. Standard catalog fields may be empty when a company does not provide them; typed company-specific filters are declared in that profile.

## Current Final Outputs

Use the clean search-ready parquet for MySQL/search:

```text
output/final/ads_search_ready.parquet
```

CSV copy:

```text
output/final/ads_search_ready.csv
```

The full audit/debug output is also kept locally:

```text
output/final/ads_embedding_ready.parquet
```

CSV copy:

```text
output/final/ads_embedding_ready.csv
```

Current verified counts:

```text
rows: 250117
embedding source columns: 23
empty embedding_content rows: 0
```

After the multi-company migration, the clean Gainr output has 38 columns, including `company_id` and `extras_json`.

## Pipeline Stages

The package lives under:

```text
src/rag_ht_pipeline/
```

Stage modules:

```text
source_sync.py             # optional CSV/DB source refresh and change report
stage1_category.py          # ads category_id -> subcategory -> main category
stage2_location.py          # city/locality/state enrichment
stage3_attributes.py        # ads_attributes bridge -> selected ad attributes
stage4_embedding_ready.py   # embedding_content and bm25_content
stage5_search_ready.py      # clean retrieval/search table for DB loading
```

The split is intentional. These are the main data boundaries. More splitting is only useful later when we add database extraction, vector generation, or retrieval serving.

## Configuration

Shared Gainr defaults and company profiles:

```text
configs/pipeline.yaml
configs/companies/*.yaml
```

Profiles contain:

- adapter and company identity
- isolated input/output paths
- source backend, schema, table names, CSV filenames, and primary keys
- company-specific credential variable names
- destination database/schema/table metadata
- embedding, BM25, filter, and output type rules

Secrets are not stored in code. Use:

```text
.env
```

Template:

```text
.env.example
```

## Run

From the repo root:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --run-all
```

The command above defaults to `gainr`. The explicit equivalent is:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --run-all
```

Run every configured company independently:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --all-companies \
  --run-all
```

Refresh every company using its own configured source backend, rebuild, and publish each successful result:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --all-companies \
  --refresh-source configured \
  --apply-source-refresh \
  --run-all \
  --publish
```

One company failing does not stop the others. The batch exits nonzero when any company fails and records the result in:

```text
output/reports/company_batch_report.json
```

For a fast sample:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --run-all \
  --sample-size 1000 \
  --no-csv
```

Run one shared stage:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company gainr \
  --stage normalize
```

The legacy `category`, `location`, and `attributes` stage names remain available for the Gainr adapter.

## Add Another Company

1. Put sanitized representative source files under `data/companies/<slug>/incoming/`.
2. Copy `configs/companies/example-flat.yaml.example` to `configs/companies/<slug>.yaml`.
3. Set `company.id`, source backend/schema/tables, paths, canonical column mapping, allowlisted `extra_columns`, filter columns, and destination settings.
4. Create the profile's private env file, such as `.env.<slug>`, using namespaced variables.
5. Run a sample and inspect that company's reports before enabling full or scheduled runs.

For relational schemas with company-specific joins, add a Python adapter under `src/rag_ht_pipeline/adapters/` and register it in the adapter registry. Do not encode unverified multi-table relationships as YAML mappings.

Example PostgreSQL extraction mapping:

```yaml
source:
  backend: postgres
  schema: inventory
  host_env: ACME_SOURCE_POSTGRES_HOST
  database_env: ACME_SOURCE_POSTGRES_DATABASE
  user_env: ACME_SOURCE_POSTGRES_USER
  password_env: ACME_SOURCE_POSTGRES_PASSWORD

source_sync:
  tables:
    - name: products
      db_schema: inventory
      db_table: inventory_items
      filename: products.csv
      primary_key: product_code
```

Example validation run:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company <slug> \
  --run-all \
  --sample-size 1000 \
  --no-csv
```

## Database Publishing

Preprocessing never writes to a destination database unless publishing is explicitly requested.

Validate the final artifact and destination settings without connecting or writing:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company <slug> \
  --publish-dry-run
```

Build, validate, and atomically publish:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --company <slug> \
  --run-all \
  --publish
```

Publishing loads a uniquely named staging table, verifies its row count, and promotes it to the configured final table. A failed preprocessing or validation run is never published. MySQL companies must use separate databases; PostgreSQL companies may use separate databases or schemas.

## Gainr Source Refresh Automation

The Gainr adapter can run from same-name CSV snapshots or from a real database export. The phpMyAdmin URL is only a browser reference; automated refresh should use direct MySQL/MariaDB or Postgres credentials in `.env`.

`--refresh-source configured` reads `source.backend` from the selected company profile. Explicit `--refresh-source mysql` and `--refresh-source postgres` remain available as one-run overrides.

The configured source tables are:

```text
ads.csv
ads_attributes.csv
categories.csv
sub_categories.csv
attributes.csv
attribute_values.csv
states.csv
location.csv
locations.csv
```

Check the current local CSV snapshots without changing files:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.source_sync --source csv
```

Export from MySQL/MariaDB, compare against the current local CSVs, and write a report without replacing files:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.source_sync --source mysql
```

Export from MySQL/MariaDB and replace the local same-name CSV snapshots after backing up the previous files:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.source_sync --source mysql --apply
```

Use Postgres as the source instead:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.source_sync --source postgres --apply
```

Run source refresh, rebuild all stages, and verify the final output in one command:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --refresh-source mysql \
  --apply-source-refresh \
  --run-all
```

For a scheduled run, use cron or any job runner. Example hourly cron entry:

```cron
0 * * * * cd /path/to/RAG_HT && PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --refresh-source mysql --apply-source-refresh --run-all >> output/reports/cron_pipeline.log 2>&1
```

After every refresh, inspect:

```text
output/reports/source_sync_report.json
output/reports/source_table_changes.csv
output/reports/pipeline_run_report.json
output/reports/final_output_correctness_report.json
```

`source_table_changes.csv` shows row counts, added rows, removed rows, updated rows, and duplicate primary-key rows per source table. This is the confirmation step before trusting the rebuilt final file.

## Local MySQL End-To-End Test

To test the same workflow against a local/test MySQL database, first put credentials in `.env`:

```text
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=rag_ht_test
MYSQL_USER=root
MYSQL_PASSWORD=
```

Check which CSVs would be uploaded:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_source_loader --dry-run
```

Create the database if needed and upload the raw source CSVs into MySQL tables:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_source_loader \
  --create-database \
  --if-exists replace
```

The loader creates/replaces these source tables:

```text
ads
ads_attributes
categories
sub_categories
attributes
attribute_values
states
location
locations
```

Then test that the pipeline can export from MySQL and rebuild:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --refresh-source mysql \
  --apply-source-refresh \
  --run-all
```

Finally, load the completed final file back into MySQL:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_search_ready.parquet \
  --table ads_search_ready \
  --if-exists replace
```

Quick SQL checks:

```sql
SELECT COUNT(*) FROM ads;
SELECT COUNT(*) FROM ads_search_ready;
SELECT id, title, main_category_name, subcategory_name, city_name, locality_name
FROM ads_search_ready
LIMIT 10;
```

## When New Data Is Added

If source data changes, either export or refresh these CSV files in the project root:

```text
ads.csv
ads_attributes.csv
categories.csv
sub_categories.csv
attributes.csv
attribute_values.csv
states.csv
location.csv
locations.csv
```

Manual file workflow:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --run-all
```

Automated database workflow:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --refresh-source mysql \
  --apply-source-refresh \
  --run-all
```

The pipeline will rebuild the final files in:

```text
output/final/
```

The refresh is snapshot-based: the database table is exported as the current source of truth, the old local CSV is backed up, and the enrichment pipeline is rebuilt from the refreshed snapshot. It does not append blindly, because blind append can keep stale or deleted records.

## Output Layout

```text
output/
├── final/
│   ├── ads_embedding_ready.parquet
│   ├── ads_embedding_ready.csv
│   ├── ads_search_ready.parquet
│   └── ads_search_ready.csv
├── reports/
└── diagnostics/
```

Important reports:

```text
output/reports/final_output_correctness_report.json
output/reports/embedding_ready_report.json
output/reports/search_ready_report.json
output/reports/attribute_mapping_report.json
```

Important diagnostics:

```text
output/diagnostics/ad_attribute_schema_mismatches.csv
output/diagnostics/final_output_attribute_mismatches.csv
output/diagnostics/final_output_category_mismatches.csv
output/diagnostics/final_output_location_mismatches.csv
```

The final verifier passed with:

```text
category mismatches: 0
location mismatches: 0
attribute mismatches: 0
```

## Embedding Columns

`embedding_content` is built from these 23 columns:

```text
title
description
meta_title
meta_description
keywords
custom_cat_value
main_category_name
main_category_meta_title
main_category_meta_description
subcategory_name
subcategory_meta_title
subcategory_meta_description
rental_duration
state_name
city_name
locality_name
locality_district
attributes_text
attribute_values_text
attribute_keywords_text
meta_keywords
main_category_meta_keywords
subcategory_meta_keywords
```

`bm25_content` keeps keyword/filter-oriented text separate.

## Search-Ready Columns

`ads_search_ready` is the clean table intended for MySQL/search. It keeps only useful retrieval, display, and filter columns plus:

```text
company_id
embedding_content
bm25_content
extras_json
```

It intentionally excludes wide/debug/source columns such as:

```text
embedding_source_columns_json
embedding_content_char_count
embedding_content_token_estimate
raw_category_id
raw_city_id
raw_locality_id
*_created_at / *_updated_at / *_deleted_at from master tables
photos
mobile
is_mobile_visible
top_start_date / top_end_date
premium_start_date / premium_end_date
```

The full `ads_embedding_ready` file remains available locally for audit and troubleshooting.

## Attribute Mapping

The confirmed ad-level bridge table is:

```text
ads_attributes.csv
```

Relationships:

```text
ads_attributes.ads_id -> ads.id
ads_attributes.attribute_id -> attributes.id
ads_attributes.value -> attribute_values.id
```

This means selected ad attributes are now populated in:

```text
attributes_text
attribute_values_text
attribute_keywords_text
```

Schema mismatches from source data are not silently dropped. They are exported to:

```text
output/diagnostics/ad_attribute_schema_mismatches.csv
```

## Legacy Direct MySQL Loading

The direct loader remains for Gainr compatibility and local troubleshooting. It does not provide the multi-company atomic staging/swap protection; use pipeline `--publish` for production.

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_search_ready.parquet \
  --table ads_search_ready \
  --if-exists replace
```

Dry run:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_search_ready.parquet \
  --table ads_search_ready \
  --dry-run
```

Set credentials in `.env`:

```text
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=gainr_catalog
MYSQL_USER=
MYSQL_PASSWORD=
```

Default behavior is `--if-exists replace`. That means the loader creates `ads_search_ready` if missing, or rebuilds it if it already exists. This is better than append for this pipeline because `ads_search_ready.parquet` is a complete fresh snapshot.

Full rebuild plus MySQL load:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --run-all

PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_search_ready.parquet \
  --table ads_search_ready \
  --if-exists replace
```

## Legacy Direct Postgres Loading

The optional direct Postgres loader is also retained for compatibility:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.postgres_loader \
  --input-file output/final/ads_search_ready.parquet \
  --table ads_search_ready
```

Dry run:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.postgres_loader \
  --input-file output/final/ads_search_ready.parquet \
  --dry-run
```

Set credentials in `.env`:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=rag_ht
POSTGRES_USER=
POSTGRES_PASSWORD=
```

MySQL source credentials also go in `.env`:

```text
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=gainr_source
MYSQL_USER=
MYSQL_PASSWORD=
```

Do not put usernames or passwords into Python files, YAML config, README, or Git. `.env` is ignored by `.gitignore`.

## Tests

```bash
.venv/bin/python -m pytest -q
```

The test count can change as checks are added; the command should finish with all tests passing.
