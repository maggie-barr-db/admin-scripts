Job Failure Scraper

Overview

- Collects failed Databricks job runs in a given time window via Jobs API 2.2 and materializes results as a PySpark DataFrame.
- Can append results to a Delta table for auditing/monitoring.
- Includes a Databricks runbook notebook with UI widgets to simplify execution in a workspace.

Repo Contents

- job_failure_scraper.py: Main scraper (Jobs API 2.2, pagination, output enrichment, optional table logging)
- args.py: Centralized CLI argument parser for the scraper
- job_failure_scraper_runbook.ipynb: Notebook runbook with widgets and token validation

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

Run in a Databricks Workspace (Recommended)

1) Add this repo to your Databricks workspace (Repos) so the notebook and modules are importable
2) Open job_failure_scraper_runbook.ipynb
3) Run the first cell to create widgets; fill in:
   - start (UTC), end (UTC)
   - limit (use 26 unless you need smaller pages)
   - log_table (optional, fully qualified: catalog.schema.table)
   - token (PAT) OR secret_scope + secret_key
   - Host is auto-detected from the current workspace
4) Run the second cell to validate the token, execute the scraper, and optionally append to the log table

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

DataFrame Columns (core)

- job_id, run_id, job_name, run_name, run_page_url, task_type
- start_time_ms, end_time_ms, duration_ms
- life_cycle_state, result_state, state_message
- termination_code, termination_type, termination_reason
- output_error

Additional Metadata Columns (logging)

- scrape_start_ms, scrape_end_ms, scrape_start_iso, scrape_end_iso
- ingest_ms, ingest_iso
- workspace_host, job_id (job context), job_run_id, task_run_id, task_key, cluster_id

Troubleshooting

- Invalid limit error: The API caps page size at 26. The script enforces this; pass --limit 26 or smaller
- Token errors in the runbook: The runbook validates tokens via a lightweight API call and reports clear errors
- Import caching in notebooks: If code was updated but behavior looks stale, restart the kernel/cluster or reload the module

Notes

- The scraper only collects runs with result_state == FAILED
- Time inputs are assumed to be UTC if timezone is not provided
# admin-scripts