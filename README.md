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

## When New Data Is Added

Current pipeline input is file-based. When the source database changes, export or refresh these CSV files in the project root:

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

Then rerun:

```bash
PYTHONPATH=src .venv/bin/python -m rag_ht_pipeline.pipeline --run-all
```

The pipeline will rebuild the final files in:

```text
output/final/
```

The phpMyAdmin URL in `configs/pipeline.yaml` is only a source database reference. For future automated extraction, use direct MySQL/MariaDB credentials instead of scraping phpMyAdmin.

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

Current test status:

```text
14 passed
```
