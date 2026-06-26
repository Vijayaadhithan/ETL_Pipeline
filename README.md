# RAG_HT Rental Marketplace Pipeline

This repository builds a verified, embedding-ready rental marketplace dataset from relational CSV exports. The pipeline preserves the source ad rows, enriches IDs into readable category/location/attribute fields, and produces text fields for later semantic search and keyword search.

It does **not** create vector embeddings yet. The current final output is embedding-ready text plus structured metadata.

## Current Final Output

Use the parquet file first:

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
```

The split is intentional. These are the main data boundaries. More splitting is only useful later when we add database extraction, vector generation, or retrieval serving.

## Configuration

Main config:

```text
configs/pipeline.yaml
```

It contains:

- input/output paths
- MySQL/phpMyAdmin reference metadata
- source table names, CSV filenames, and primary keys
- Postgres environment variable names
- all 23 embedding source columns
- BM25/filter candidate columns

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

For a fast sample:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline \
  --run-all \
  --sample-size 1000 \
  --no-csv
```

Run one stage:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --stage attributes
```

## Source Refresh Automation

The pipeline can run from same-name CSV snapshots or from a real database export. The phpMyAdmin URL is only a browser reference; automated refresh should use direct MySQL/MariaDB or Postgres credentials in `.env`.

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
  --input-file output/final/ads_embedding_ready.parquet \
  --table ads_embedding_ready \
  --if-exists replace
```

Quick SQL checks:

```sql
SELECT COUNT(*) FROM ads;
SELECT COUNT(*) FROM ads_embedding_ready;
SELECT id, title, main_category_name, subcategory_name, city_name, locality_name
FROM ads_embedding_ready
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
│   ├── ads_stage_04_embedding_ready.parquet
│   └── ads_stage_04_embedding_ready.csv
├── reports/
└── diagnostics/
```

Important reports:

```text
output/reports/final_output_correctness_report.json
output/reports/embedding_ready_report.json
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

## MySQL Loading

If the production database is MySQL/MariaDB, load the final embedding-ready snapshot into MySQL:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_embedding_ready.parquet \
  --table ads_embedding_ready \
  --if-exists replace
```

Dry run:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_embedding_ready.parquet \
  --table ads_embedding_ready \
  --dry-run
```

Set credentials in `.env`:

```text
MYSQL_HOST=testphpmyadmin.gainr.in
MYSQL_PORT=3306
MYSQL_DATABASE=hvkbynbu_wwwdevsl_slowr_test
MYSQL_USER=
MYSQL_PASSWORD=
```

Default behavior is `--if-exists replace`. That means the loader creates `ads_embedding_ready` if missing, or rebuilds it if it already exists. This is better than append for this pipeline because `ads_embedding_ready.parquet` is a complete fresh snapshot.

Full rebuild plus MySQL load:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --run-all

PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.mysql_loader \
  --input-file output/final/ads_embedding_ready.parquet \
  --table ads_embedding_ready \
  --if-exists replace
```

## Postgres Loading

Optional Postgres loader:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.postgres_loader \
  --input-file output/final/ads_embedding_ready.parquet \
  --table ads_embedding_ready
```

Dry run:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.postgres_loader \
  --input-file output/final/ads_embedding_ready.parquet \
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
MYSQL_HOST=testphpmyadmin.gainr.in
MYSQL_PORT=3306
MYSQL_DATABASE=hvkbynbu_wwwdevsl_slowr_test
MYSQL_USER=
MYSQL_PASSWORD=
```

Do not put usernames or passwords into Python files, YAML config, README, or Git. `.env` is ignored by `.gitignore`.

## Tests

```bash
.venv/bin/python -m pytest -q
```

The test count can change as checks are added; the command should finish with all tests passing.
