# Gainr Production Manual Update Runbook

This runbook is for the existing no-sudo deployment at:

```text
/home/gainr/ETL_Pipeline
```

Run the ETL manually under the existing `gainr` Linux account. Do not install
the system-wide systemd units or create a `raght` user when sudo is unavailable.

## Important boundaries

- Source refresh performs `SELECT` queries against the company database.
- `--publish-dry-run` validates without changing the destination table.
- `--publish` writes and atomically replaces `ads_search_ready`.
- The source and destination may be the same MySQL database and user.
- Never start a second ETL while one is running. The scheduled wrapper also
  has a per-company overlap lock.
- Do not delete `pending_source_run.json` or source-apply journals to force a
  run. The next wrapper execution uses them for crash-safe resume.

## 1. Enter the deployment

```bash
cd /home/gainr/ETL_Pipeline
pwd
chmod 600 .env
```

## 2. Check whether an ETL is already running

```bash
pgrep -af 'rag-ht-pipeline|run_scheduled_etl' || true
```

If an ETL process is listed, monitor that run instead of starting another one:

```bash
.venv/bin/rag-ht-ops status --company gainr
```

## 3. Frequent data update when code has not changed

Preflight:

```bash
.venv/bin/rag-ht-ops preflight --company gainr
```

Run source refresh, incremental processing, validation, and production publish:

```bash
./scripts/run_scheduled_etl.sh gainr --publish
```

This is the normal manual production-update command. Missing baselines or
shared lookup changes automatically cause a safe full rebuild.

Verify completion:

```bash
.venv/bin/rag-ht-ops status --company gainr
.venv/bin/python -m json.tool output/reports/publish_report.json
```

Success requires:

```text
status: PASS
published: true
post_publish_verification.status: PASS
wrong_company_rows: 0
```

## 4. Update after new code is pushed

Review the current revision and local modifications before pulling:

```bash
git status --short
git log -1 --oneline
```

Do not overwrite unexplained production modifications. If the worktree is
clean, update with:

```bash
git pull --ff-only
```

Refresh the installed package without replacing `.env`:

```bash
./scripts/setup.sh gainr --skip-tests
```

Run regression checks:

```bash
.venv/bin/python -m pytest -q
bash -n scripts/setup.sh scripts/run_scheduled_etl.sh scripts/install_systemd.sh
.venv/bin/python -m compileall -q src
git diff --check
```

Run preflight:

```bash
.venv/bin/rag-ht-ops preflight --company gainr
```

Build and validate current source data without publishing:

```bash
./scripts/run_scheduled_etl.sh gainr
```

Review status and final validation:

```bash
.venv/bin/rag-ht-ops status --company gainr
.venv/bin/python -m json.tool output/reports/final_output_correctness_report.json
```

Validate the publish operation without connecting or writing:

```bash
.venv/bin/rag-ht-pipeline --company gainr --publish-dry-run
```

Publish the validated artifact:

```bash
.venv/bin/rag-ht-pipeline --company gainr --publish
```

Final verification:

```bash
.venv/bin/rag-ht-ops status --company gainr
.venv/bin/python -m json.tool output/reports/publish_report.json
```

## 5. Monitor a running manual update

Open a second SSH session:

```bash
cd /home/gainr/ETL_Pipeline
.venv/bin/rag-ht-ops status --company gainr
```

Read the pipeline PID from the status file:

```bash
PID="$(.venv/bin/python -c 'import json; print(json.load(open("output/reports/run_status.json"))["pid"])')"
echo "$PID"
ps -p "$PID" -o pid,ppid,stat,etime,%cpu,%mem,rss,command
```

Continuously monitor CPU and memory:

```bash
watch -n 5 "ps -p $PID -o pid,stat,etime,%cpu,%mem,rss,command"
```

Monitor process disk I/O:

```bash
watch -n 5 "grep -E 'read_bytes|write_bytes' /proc/$PID/io"
```

An unchanged status heartbeat during source snapshot comparison does not by
itself mean the process is stuck. Large CSV comparisons use bounded-memory
temporary SQLite indexes and may be quiet for several minutes.

## 6. Failure and resume

Inspect the recorded error:

```bash
.venv/bin/rag-ht-ops status --company gainr
.venv/bin/python -m json.tool output/reports/run_status.json
```

Check pending recovery state:

```bash
if [ -f output/reports/pending_source_run.json ]; then
  .venv/bin/python -m json.tool output/reports/pending_source_run.json
else
  echo "No pending source generation"
fi
```

After correcting the reported cause, resume with the same guarded wrapper:

```bash
./scripts/run_scheduled_etl.sh gainr --publish
```

Do not delete pending state merely to make the warning disappear.

## 7. Disk usage audit

The observed server filesystem is 77 GB with approximately 39 GB free and 51%
used. That is healthy. Measure the actual owner of disk space before deleting
anything.

Overall filesystem:

```bash
df -h
```

Largest directories accessible to the `gainr` user:

```bash
du -xhd1 /home/gainr 2>/dev/null | sort -h
du -xhd1 /home/gainr/ETL_Pipeline 2>/dev/null | sort -h
du -xhd2 /home/gainr/ETL_Pipeline/output 2>/dev/null | sort -h
```

Largest files under the account:

```bash
find /home/gainr -xdev -type f -size +100M -printf '%s %p\n' 2>/dev/null \
  | sort -n \
  | numfmt --field=1 --to=iec
```

Check for deleted files still held open by this user:

```bash
lsof +L1 2>/dev/null | grep '/home/gainr' || true
```

If most usage is outside `/home/gainr`, an administrator must inspect and clean
system-owned locations such as `/var/log`, `/var/lib/docker`, package caches,
or system journals. Do not attempt to work around missing permissions.

## 8. Required ETL files: do not delete

These are required for the next incremental run:

```text
ads.csv and the other active source CSV snapshots
output/intermediate/ads_stage_03_attributes_enriched.parquet
output/final/ads_embedding_ready.parquet
output/final/ads_search_ready.parquet
output/reports/pending_source_run.json, when present
output/source_sync/apply_journal.json, when present
.env
.venv
```

Deleting the Parquet baselines forces a full rebuild. Deleting pending state or
an apply journal can break crash-safe recovery.

## 9. Safe cleanup after a successful run

First confirm no ETL is running and no recovery is pending:

```bash
pgrep -af 'rag-ht-pipeline|run_scheduled_etl' || true
.venv/bin/rag-ht-ops status --company gainr
test ! -f output/reports/pending_source_run.json && echo "No pending source run"
test ! -f output/source_sync/apply_journal.json && echo "No source apply journal"
```

Continue only when no process is listed, status is `PASS`, and both `test`
commands print their confirmation.

Remove Python/test caches:

```bash
find . -type d -name '__pycache__' -prune -exec rm -rf {} +
find . -type f -name '*.pyc' -delete
rm -rf .pytest_cache
```

Remove regenerable diagnostics:

```bash
rm -rf output/diagnostics/*
```

The Stage 1 and Stage 2 full intermediate files are useful for inspection but
are not among the three required incremental baselines. After a successful
run, they may be removed and will be recreated by the next full rebuild:

```bash
rm -f output/intermediate/ads_stage_01_category_enriched.parquet
rm -f output/intermediate/ads_stage_02_location_enriched.parquet
```

Remove incremental work left by failed historical attempts:

```bash
rm -rf output/incremental/work
```

Remove incremental report archives older than 30 days:

```bash
find output/reports/incremental \
  -mindepth 1 -maxdepth 1 -type d -mtime +30 \
  -exec rm -rf {} + 2>/dev/null
```

Remove scheduled log files older than 30 days:

```bash
find output/reports -type f -name '*.log.*' -mtime +30 -delete
```

If `output/source_sync/latest` contains abandoned exports even though there is
no pending run or apply journal, clear only that staging directory:

```bash
rm -rf output/source_sync/latest/*
```

Review recovered space:

```bash
du -sh .venv .git output 2>/dev/null
du -xhd2 output 2>/dev/null | sort -h
df -h
```

## 10. Optional cleanup with recovery tradeoffs

The pipeline automatically retains only one source snapshot backup. It is used
to restore active CSV snapshots if source apply is interrupted. Keep it unless
disk pressure is real.

Measure it:

```bash
du -sh output/source_sync/backups 2>/dev/null
```

Only after a successful ETL, with no running process, pending run, or apply
journal, the retained source backup may be removed to reclaim space:

```bash
find output/source_sync/backups \
  -mindepth 1 -maxdepth 1 -type d \
  -exec rm -rf {} +
```

This gives up local source-snapshot rollback until the next successful refresh
creates a new backup.

The root-level `users.csv` is not an input to the Gainr search ETL. If it exists
on production and its separate database import has already been verified, it
may be compressed or removed independently:

```bash
ls -lh users.csv 2>/dev/null
gzip -9 users.csv
```

Do not remove the nine configured Gainr source CSV snapshots.

## 11. Manual rollback

Publishing retains `ads_search_ready__previous`. Use rollback only when the
latest live publish has been verified as defective or a documented recovery
procedure explicitly requires restoring the prior table:

```bash
.venv/bin/rag-ht-pipeline --company gainr --rollback
```

Verify immediately:

```bash
.venv/bin/rag-ht-ops status --company gainr
.venv/bin/python -m json.tool output/reports/rollback_report.json
```

## 12. Git storage maintenance

Measure repository metadata:

```bash
du -sh .git
```

If the worktree is clean, compact unreachable Git objects:

```bash
git status --short
git gc
```

Do not use `git reset --hard` or delete `.git` as a disk-cleanup method.

## 13. Quick command summary

Routine manual update:

```bash
cd /home/gainr/ETL_Pipeline
.venv/bin/rag-ht-ops preflight --company gainr
./scripts/run_scheduled_etl.sh gainr --publish
.venv/bin/rag-ht-ops status --company gainr
```

After code changes:

```bash
cd /home/gainr/ETL_Pipeline
git status --short
git pull --ff-only
./scripts/setup.sh gainr --skip-tests
.venv/bin/python -m pytest -q
.venv/bin/rag-ht-ops preflight --company gainr
./scripts/run_scheduled_etl.sh gainr
.venv/bin/rag-ht-pipeline --company gainr --publish-dry-run
.venv/bin/rag-ht-pipeline --company gainr --publish
.venv/bin/rag-ht-ops status --company gainr
```
