Job Failure Scraper

Overview

- Collects failed Databricks job runs in a given time window via Jobs API 2.2 and materializes results as a PySpark DataFrame.
- Can append results to a Delta table for auditing/monitoring.
- Supports direct execution via CLI or as a scheduled Databricks job (DAB).

Repo Contents

- job_failure_scraper.py: Main scraper (Jobs API 2.2, pagination, output enrichment, optional table logging)
- args.py: Centralized CLI argument parser for the scraper
- job_failure_scraper/scraper.py: Also supports scheduled jobs with optional --state-table watermarking
- bundle.yml: Databricks Asset Bundle defining a scheduled job

Requirements

- Python 3.8+
- requests
- PySpark (Databricks notebook clusters provide Spark and PySpark by default)

Authentication

- Uses a Databricks Personal Access Token (PAT) passed as an argument.
- In notebooks, you can supply the token directly via the widget or store it in a secret scope and let the runbook fetch it.
- Credential resolution precedence inside the scraper:
  1) Explicit args (--host/--token) — widget/secret values provided to these flags count as explicit
  2) Environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN (or DATABRICKS_PERSONAL_ACCESS_TOKEN)
  3) ~/.databrickscfg (profile via DATABRICKS_CONFIG_PROFILE or --profile)

API Details

- Lists runs via /api/2.2/jobs/runs/list with pagination using next_page_token
- Server-enforced page size limit is 26; the scraper defaults to 26 and clamps higher values
- Gets run details via /api/2.2/jobs/runs/get-output
- For multi-task runs, fetches task metadata and attempts per-task outputs; adds one summary row and per-task rows when available

Run in a Databricks Workspace

Use the Databricks Asset Bundle job defined in `bundle.yml`, or invoke `job_failure_scraper/scraper.py` with parameters from a Databricks job (spark_python_task). Host is auto-detected; pass `--token` if needed.

Local CLI Usage

- Ensure you have Spark/PySpark available locally or set the code to run on a remote cluster.
- Provide host and token explicitly (or via env/.databrickscfg).

Example:

```bash
export DATABRICKS_HOST="https://<your-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="<your_pat>"

python job_failure_scraper.py \
  --start "2025-09-23 08:00:00" \
  --end "2025-09-23 10:59:59" \
  --host "$DATABRICKS_HOST" \
  --token "$DATABRICKS_TOKEN" \
  --limit 26 \
  --log-table catalog.schema.job_failure_log
```

CLI Arguments

- --start: Start time (ISO 8601 or "YYYY-MM-DD HH:MM:SS")
- --end: End time (ISO 8601 or "YYYY-MM-DD HH:MM:SS")
- --host: Databricks workspace URL (e.g., https://<your-workspace>.azuredatabricks.net)
- --token: Databricks Personal Access Token
- --profile: Optional; read host/token from ~/.databrickscfg profile if not explicitly provided
- --limit: Page size for the API (max 26; default 26)
- --no-show: Suppress DataFrame display
- --log-table: Optional; fully qualified table name to append results (creates table on first write if missing)

Databricks Asset Bundle (Scheduled Job)

Prerequisites

- Databricks CLI configured with a profile targeting your workspace.
- Unity Catalog catalog/schema for `log_table` and `state_table` exist and are writable.

Bundle Variables (bundle.yml)

- workspace_host: Workspace URL (e.g. https://xyz.cloud.databricks.com)
- log_table: Fully qualified logging table (catalog.schema.table)
- state_table: Fully qualified watermark table (catalog.schema.table)
- spark_version: Runtime (default 14.3.x-scala2.12)
- node_type_id: Node type (default i3.xlarge)
- run_as_user/run_as_spn: Identity for dev/prod targets

Deploy and Run

```bash
# Validate the bundle locally
databricks bundles validate

# Deploy to target (e.g., dev)
databricks bundles deploy -t dev

# Run the job on demand (optional)
databricks bundles run -t dev -r scraper-job
```

What It Does

- The job runs daily at 19:00 UTC.
- Entry point `job_failure_scraper/scraper.py` computes the time window from the last watermark:
  - Uses `--state-table` last_run_iso if available; otherwise falls back to `max(row_added_at)` in `--log-table`; otherwise defaults to 24h window.
  - Calls the scraper with `--start`, `--end`, `--limit=26`, and passes through `--log-table` and optional UC metadata flags.
  - Persists the new watermark (`end_iso`) back to `--state-table` after success.

Parameters to Override (optional)

- You can pass `--start` and/or `--end` to `entrypoint.py` via `parameters` in `bundle.yml` to override the computed window.
- You can pass `--host`/`--token` if you prefer not to rely on cluster/workspace defaults and cluster identity.

Table Schema (auto-created if missing)

- job_id (BIGINT) – Job ID from the failed run
- run_id (BIGINT)
- job_name (STRING)
- run_name (STRING)
- run_page_url (STRING)
- child_task_run_id (BIGINT)
- child_task_key (STRING)
- task_type (STRING)
- job_start_datetime_iso (STRING, ISO 8601 UTC)
- job_end_datetime_iso (STRING, ISO 8601 UTC)
- duration_ms (BIGINT)
- life_cycle_state, result_state, state_message (STRING)
- termination_code, termination_type, termination_reason (STRING)
- output_error (STRING)
- is_retry (BOOLEAN), attempt_number (BIGINT), original_attempt_run_id (BIGINT)
- error_category (STRING), error_provider (STRING), error_message_short (STRING)
- row_added_at (STRING, ISO 8601 UTC)

Troubleshooting

- Invalid limit error: The API caps page size at 26. The script enforces this; pass --limit 26 or smaller
- Token errors in the runbook: The runbook validates tokens via a lightweight API call and reports clear errors
- Import caching in notebooks: If code was updated but behavior looks stale, restart the kernel/cluster or reload the module with `importlib.reload`

Notes

- The scraper only collects runs with result_state == FAILED
- Time inputs are assumed to be UTC if timezone is not provided
- In ACL mode, automatic schema evolution is blocked; this project auto-creates the table with explicit schema and casts ID columns before writing. If schema changes, DROP TABLE and re-run to re-create.

Performance & Ops

- Consider partitioning by DATE(job_end_datetime_iso) and ZORDER (job_id, run_id, child_task_run_id)
- Optional dedup: define event_run_id = COALESCE(child_task_run_id, run_id) and ingest via MERGE to collapse retries
# admin-scripts