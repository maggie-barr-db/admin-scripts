# Serverless Migration Prep

Tools for assessing a Databricks workspace prior to a serverless compute migration.

## job_api_scraper

A Databricks notebook that inventories all jobs in a workspace by scraping the Jobs API via the Databricks SDK.

### What it does

1. Lists all non-deleted jobs using `WorkspaceClient.jobs.list()`
2. Extracts task-level details including compute type, notebook paths, and languages
3. Fetches the most recent run info (duration, status, end time) for each job
4. Writes results to Delta tables for analysis

### Output columns

| Column | Description |
|--------|-------------|
| `job_id` | Databricks job ID |
| `job_name` | Job name |
| `job_tags` | Job tags (as string) |
| `task_key` | Task key within the job |
| `task_type` | Task type: `notebook`, `spark_python`, `python_wheel`, `spark_jar`, `spark_submit`, `pipeline`, `sql`, `dbt`, `run_job`, or `other` |
| `cluster_type` | Compute type: `job_cluster`, `interactive`, `serverless`, or `unknown` |
| `notebook_path` | Full workspace path for notebook tasks |
| `notebook_name` | Notebook filename |
| `notebook_language` | Language (PYTHON, SQL, SCALA, R) |
| `last_run_duration_seconds` | Duration of the most recent run |
| `last_run_status` | Result or lifecycle state of the most recent run |
| `last_run_end_time` | UTC timestamp of the most recent run's end |

### Configuration

Update these variables in the notebook before running:

```python
CATALOG = "prod_catalog"
SCHEMA = "databricks_serverless_db"
SOURCE_TABLE = "workspace_jobs"
SUMMARY_TABLE = "workspace_jobs_summary"
```

### Usage

1. Import the notebook into the target Databricks workspace
2. Update the catalog/schema/table configuration
3. Attach to a cluster and run all cells
4. Query the resulting Delta table to understand the workspace's job landscape and compute usage
