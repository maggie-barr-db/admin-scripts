import os
import sys
import json
import time
import argparse
import configparser
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

MAX_API_LIMIT = 26


def load_databricks_config(
  explicit_host: Optional[str],
  explicit_token: Optional[str],
  profile: Optional[str]
) -> Dict[str, str]:
  """Resolve Databricks host and token from args, env, or ~/.databrickscfg.

  Precedence:
  1) Explicit args
  2) Env vars: DATABRICKS_HOST, DATABRICKS_TOKEN (or DATABRICKS_PERSONAL_ACCESS_TOKEN)
  3) ~/.databrickscfg (profile via DATABRICKS_CONFIG_PROFILE or --profile)
  """
  host = explicit_host or os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_URL")
  token = explicit_token or os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_PERSONAL_ACCESS_TOKEN")

  if host and token:
    return {"host": host.rstrip("/"), "token": token}

  cfg_path = os.path.expanduser("~/.databrickscfg")
  chosen_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"
  if os.path.exists(cfg_path):
    parser = configparser.ConfigParser()
    parser.read(cfg_path)
    if chosen_profile in parser:
      if not host:
        host = parser[chosen_profile].get("host")
      if not token:
        token = parser[chosen_profile].get("token")

  if not host or not token:
    raise RuntimeError(
      "Missing Databricks credentials. Provide --host/--token, set env DATABRICKS_HOST/DATABRICKS_TOKEN, or configure ~/.databrickscfg."
    )

  return {"host": host.rstrip("/"), "token": token}


def build_session(total_retries: int = 5, backoff_factor: float = 0.5) -> requests.Session:
  session = requests.Session()
  retries = Retry(
    total=total_retries,
    backoff_factor=backoff_factor,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
  )
  adapter = HTTPAdapter(max_retries=retries)
  session.mount("http://", adapter)
  session.mount("https://", adapter)
  return session


def to_epoch_ms(ts_str: str, assume_utc: bool = True) -> int:
  """Convert a timestamp string to epoch ms.

  Accepted formats:
  - ISO 8601 (e.g., 2025-09-24T12:34:56Z or 2025-09-24T12:34:56+00:00)
  - "YYYY-MM-DD HH:MM:SS" (assumed UTC if assume_utc=True)
  - "YYYY-MM-DD" (interpreted as start of day 00:00:00)
  """
  s = ts_str.strip()
  try:
    if "T" in s or s.endswith("Z") or "+" in s:
      # Normalize trailing Z for fromisoformat
      if s.endswith("Z"):
        s = s[:-1] + "+00:00"
      dt = datetime.fromisoformat(s)
    else:
      # Fallbacks for date or datetime without timezone
      if len(s) == 10:  # YYYY-MM-DD
        s = s + " 00:00:00"
      dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
      if assume_utc:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)
  except Exception as e:
    raise ValueError(f"Could not parse time '{ts_str}': {e}")


def list_failed_runs(
  host: str,
  token: str,
  start_time_from_ms: int,
  start_time_to_ms: int,
  limit: int = MAX_API_LIMIT
) -> List[Dict[str, Any]]:
  """List failed job runs between start and end (inclusive) using Jobs API 2.2.

  Paginates using has_more + next_page_token.
  Returns the full run objects for runs with result_state == FAILED.
  """
  session = build_session()
  headers = {"Authorization": f"Bearer {token}"}
  url = f"{host}/api/2.2/jobs/runs/list"

  failed_runs: List[Dict[str, Any]] = []
  page_token: Optional[str] = None

  # Enforce API maximum page size
  if not isinstance(limit, int) or limit <= 0:
    limit = MAX_API_LIMIT
  if limit > MAX_API_LIMIT:
    limit = MAX_API_LIMIT

  while True:
    params: Dict[str, Any] = {
      "start_time_from": start_time_from_ms,
      "start_time_to": start_time_to_ms,
      "limit": limit,
    }
    if page_token:
      params["page_token"] = page_token

    resp = session.get(url, headers=headers, params=params, timeout=60)
    if resp.status_code >= 400:
      raise RuntimeError(f"runs/list failed: HTTP {resp.status_code} - {resp.text}")
    payload = resp.json()

    for run in payload.get("runs", []):
      state = run.get("state", {})
      if state.get("result_state") == "FAILED":
        failed_runs.append(run)

    has_more = payload.get("has_more", False)
    if has_more:
      page_token = payload.get("next_page_token")
      if not page_token:
        # Defensive: avoid infinite loops if has_more without token
        break
      # Gentle throttle to be polite
      time.sleep(0.1)
      continue
    break

  return failed_runs


def get_run_output(host: str, token: str, run_id: int) -> Dict[str, Any]:
  session = build_session()
  headers = {"Authorization": f"Bearer {token}"}
  url = f"{host}/api/2.2/jobs/runs/get-output"
  params = {"run_id": run_id}
  resp = session.get(url, headers=headers, params=params, timeout=60)
  if resp.status_code >= 400:
    raise RuntimeError(f"runs/get-output failed for run_id={run_id}: HTTP {resp.status_code} - {resp.text}")
  return resp.json()


def infer_task_type(metadata: Dict[str, Any]) -> Optional[StringType]:
  # Try common task descriptors
  task_obj = metadata.get("task") or {}
  if isinstance(task_obj, dict) and task_obj:
    # Return key name of the concrete task type if present
    for k in [
      "notebook_task",
      "spark_jar_task",
      "spark_python_task",
      "pipeline_task",
      "python_wheel_task",
      "spark_submit_task",
      "dbt_task",
      "sql_task",
      "run_job_task",
    ]:
      if k in task_obj:
        return k
  return None


def build_schema() -> StructType:
  return StructType([
    StructField("job_id", LongType(), True),
    StructField("run_id", LongType(), True),
    StructField("job_name", StringType(), True),
    StructField("run_name", StringType(), True),
    StructField("run_page_url", StringType(), True),
    StructField("task_type", StringType(), True),
    StructField("start_time_ms", LongType(), True),
    StructField("end_time_ms", LongType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("life_cycle_state", StringType(), True),
    StructField("result_state", StringType(), True),
    StructField("state_message", StringType(), True),
    StructField("termination_code", StringType(), True),
    StructField("termination_type", StringType(), True),
    StructField("termination_reason", StringType(), True),
    StructField("output_error", StringType(), True),
  ])


def main(argv: Optional[List[str]] = None) -> int:
  parser = argparse.ArgumentParser(description="Scrape failed Databricks job runs into a Spark DataFrame (Jobs API 2.2)")
  parser.add_argument("--start", required=True, help="Start time (ISO 8601 or 'YYYY-MM-DD HH:MM:SS'). UTC assumed if naive")
  parser.add_argument("--end", required=True, help="End time (ISO 8601 or 'YYYY-MM-DD HH:MM:SS'). UTC assumed if naive")
  parser.add_argument("--host", default=None, help="Databricks workspace URL, e.g. https://xyz.cloud.databricks.com")
  parser.add_argument("--token", default=None, help="Databricks personal access token")
  parser.add_argument("--profile", default=None, help="Profile name from ~/.databrickscfg (fallback if no host/token provided)")
  parser.add_argument("--limit", type=int, default=MAX_API_LIMIT, help="Page size for runs/list (max 26; default 26)")
  parser.add_argument("--no-show", action="store_true", help="Do not show the resulting DataFrame")

  args = parser.parse_args(argv)

  cfg = load_databricks_config(args.host, args.token, args.profile)
  host = cfg["host"]
  token = cfg["token"]

  start_ms = to_epoch_ms(args.start)
  end_ms = to_epoch_ms(args.end)
  if end_ms < start_ms:
    raise ValueError("--end must be after --start")

  spark = SparkSession.builder.appName("JobFailureScraperV2").getOrCreate()

  failed_runs = list_failed_runs(host, token, start_ms, end_ms, limit=args.limit)

  rows: List[tuple] = []
  schema = build_schema()

  for run in failed_runs:
    run_id = run.get("run_id")
    try:
      out = get_run_output(host, token, run_id)
    except Exception as e:
      # Capture minimal info if output call fails
      state = (run.get("state") or {})
      life_cycle_state = state.get("life_cycle_state")
      result_state = state.get("result_state")
      state_message = state.get("state_message")
      start_time_ms = run.get("start_time")
      end_time_ms = run.get("end_time")
      duration_ms = (end_time_ms - start_time_ms) if (end_time_ms and start_time_ms) else None
      rows.append((
        run.get("job_id"),
        run_id,
        None,
        run.get("run_name"),
        run.get("run_page_url"),
        None,
        start_time_ms,
        end_time_ms,
        duration_ms,
        life_cycle_state,
        result_state,
        state_message,
        None,
        None,
        None,
        f"get-output error: {e}",
      ))
      continue

    metadata = out.get("metadata", {})
    state = metadata.get("state") or metadata.get("status") or {}
    term = state.get("termination_details") or {}

    job_id = metadata.get("job_id") or run.get("job_id")
    job_name = metadata.get("job_name")
    run_name = metadata.get("run_name") or run.get("run_name")
    run_page_url = metadata.get("run_page_url") or run.get("run_page_url")
    start_time_ms = metadata.get("start_time") or run.get("start_time")
    end_time_ms = metadata.get("end_time") or run.get("end_time")
    duration_ms = (end_time_ms - start_time_ms) if (end_time_ms and start_time_ms) else None
    life_cycle_state = state.get("life_cycle_state") or state.get("state")
    result_state = state.get("result_state")
    state_message = state.get("state_message")
    termination_code = term.get("code")
    termination_type = term.get("type")
    termination_reason = term.get("reason")
    output_error = out.get("error")
    task_type = infer_task_type(metadata)

    # Fallback job_name to run_name if absent
    if not job_name:
      job_name = run_name

    rows.append((
      job_id,
      run_id,
      job_name,
      run_name,
      run_page_url,
      task_type,
      start_time_ms,
      end_time_ms,
      duration_ms,
      life_cycle_state,
      result_state,
      state_message,
      termination_code,
      termination_type,
      termination_reason,
      output_error,
    ))

  df = spark.createDataFrame(rows, schema=schema)
  if not args.no_show:
    df.show(truncate=False)

  return 0


if __name__ == "__main__":
  try:
    sys.exit(main())
  except Exception as exc:
    print(f"Error: {exc}", file=sys.stderr)
    sys.exit(1)


