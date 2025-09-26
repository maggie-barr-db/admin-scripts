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
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from pyspark.sql import functions as F
from args import parse_args

MAX_API_LIMIT = 26


def load_databricks_config(
  explicit_host: Optional[str],
  explicit_token: Optional[str],
  profile: Optional[str]
) -> Dict[str, str]:
  """Resolve Databricks host and token from args, env, or ~/.databrickscfg.

  Precedence (highest to lowest):
  1) Explicit args (--host/--token). In notebooks, values read from widgets or
     secrets and supplied to --host/--token are treated as explicit.
  2) Env vars: DATABRICKS_HOST, DATABRICKS_TOKEN (or DATABRICKS_PERSONAL_ACCESS_TOKEN)
  3) ~/.databrickscfg (profile via DATABRICKS_CONFIG_PROFILE or --profile)
  4) If still missing, raise a clear error.
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


def get_run_output(host: str, token: str, run_id: int, task_key: Optional[str] = None) -> Dict[str, Any]:
  session = build_session()
  headers = {"Authorization": f"Bearer {token}"}
  url = f"{host}/api/2.2/jobs/runs/get-output"
  params: Dict[str, Any] = {"run_id": run_id}
  if task_key:
    params["task_key"] = task_key
  resp = session.get(url, headers=headers, params=params, timeout=60)
  if resp.status_code >= 400:
    raise RuntimeError(f"runs/get-output failed for run_id={run_id}: HTTP {resp.status_code} - {resp.text}")
  return resp.json()


def get_run_details(host: str, token: str, run_id: int) -> Dict[str, Any]:
  session = build_session()
  headers = {"Authorization": f"Bearer {token}"}
  url = f"{host}/api/2.2/jobs/runs/get"
  params = {"run_id": run_id}
  resp = session.get(url, headers=headers, params=params, timeout=60)
  if resp.status_code >= 400:
    raise RuntimeError(f"runs/get failed for run_id={run_id}: HTTP {resp.status_code} - {resp.text}")
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
    StructField("child_task_run_id", LongType(), True),
    StructField("child_task_key", StringType(), True),
    StructField("task_type", StringType(), True),
    StructField("job_start_datetime_iso", StringType(), True),
    StructField("job_end_datetime_iso", StringType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("life_cycle_state", StringType(), True),
    StructField("result_state", StringType(), True),
    StructField("state_message", StringType(), True),
    StructField("termination_code", StringType(), True),
    StructField("termination_type", StringType(), True),
    StructField("termination_reason", StringType(), True),
    StructField("output_error", StringType(), True),
    StructField("is_retry", BooleanType(), True),
    StructField("attempt_number", LongType(), True),
    StructField("original_attempt_run_id", LongType(), True),
    StructField("error_category", StringType(), True),
    StructField("error_provider", StringType(), True),
    StructField("error_message_short", StringType(), True),
  ])


def _sql_literal(value: Any) -> str:
  """Return a SQL-safe single-quoted literal content (without surrounding quotes)."""
  s = str(value)
  # Escape backslashes first, then single quotes for SQL literal
  s = s.replace("\\", "\\\\").replace("'", "''")
  return s


def _as_int(value: Any) -> Optional[int]:
  try:
    return int(value) if value is not None else None
  except Exception:
    return None


def determine_is_retry(run: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None, details: Optional[Dict[str, Any]] = None) -> bool:
  """Best-effort detection of retries from Jobs API payloads.
  Checks attempt_number/run_attempt and original_attempt_run_id if present.
  """
  attempt_candidates = [
    (metadata or {}).get("attempt_number"),
    (details or {}).get("attempt_number"),
    run.get("attempt_number"),
    run.get("run_attempt"),
  ]
  for cand in attempt_candidates:
    n = _as_int(cand)
    if n is not None and n > 1:
      return True
    if n is not None and n > 0:
      # Some payloads are 0-based
      return True
  original_attempt = (details or {}).get("original_attempt_run_id") or run.get("original_attempt_run_id")
  run_id = run.get("run_id")
  if original_attempt and run_id and original_attempt != run_id:
    return True
  return False


def determine_attempt_number(run: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None, details: Optional[Dict[str, Any]] = None) -> Optional[int]:
  for cand in [
    (metadata or {}).get("attempt_number"),
    (details or {}).get("attempt_number"),
    run.get("attempt_number"),
    run.get("run_attempt"),
  ]:
    n = _as_int(cand)
    if n is not None:
      return n
  return None


def determine_original_attempt_run_id(run: Dict[str, Any], details: Optional[Dict[str, Any]] = None) -> Optional[int]:
  for cand in [
    (details or {}).get("original_attempt_run_id"),
    run.get("original_attempt_run_id"),
  ]:
    n = _as_int(cand)
    if n is not None:
      return n
  return None


def parse_error_fields(message: Optional[str]) -> Dict[str, Optional[str]]:
  if not message:
    return {"category": None, "provider": None, "short": None}
  m = message.replace("\n", " ").strip()
  upper = m.upper()
  category = None
  for key in [
    "RESOURCE_DOES_NOT_EXIST",
    "UNAUTHENTICATED",
    "PERMISSION_DENIED",
    "INVALID_PARAMETER_VALUE",
    "INTERNAL_ERROR",
    "TIMEOUT",
    "CANCELLED",
    "NOT_FOUND",
  ]:
    if key in upper:
      category = key
      break
  provider = None
  if "GIT" in upper or "GITHUB" in upper:
    provider = "GIT"
  elif "PIPELINE" in upper or "DELTA LIVE TABLES" in upper or "DLT" in upper:
    provider = "DLT"
  elif "SCHEMA" in upper or "CATALOG" in upper or "TABLE" in upper:
    provider = "UNITY_CATALOG"
  short = m[:240]
  return {"category": category, "provider": provider, "short": short}


def main(argv: Optional[List[str]] = None) -> int:
  from args import parse_args
  args = parse_args(argv)

  cfg = load_databricks_config(args.host, args.token, args.profile)
  host = cfg["host"]
  token = cfg["token"]

  start_ms = to_epoch_ms(args.start)
  end_ms = to_epoch_ms(args.end)
  if end_ms < start_ms:
    raise ValueError("--end must be after --start")

  spark = SparkSession.builder.appName("JobFailureScraper").getOrCreate()

  failed_runs = list_failed_runs(host, token, start_ms, end_ms, limit=args.limit)

  rows: List[tuple] = []
  schema = build_schema()

  for run in failed_runs:
    run_id = run.get("run_id")
    try:
      out = get_run_output(host, token, run_id)
    except Exception as e:
      err_msg = str(e)
      # If multi-task run output isn't supported at run-level, fetch per-task outputs
      if "multiple tasks is not supported" in err_msg:
        try:
          details = get_run_details(host, token, run_id)
          tasks = details.get("tasks", []) or []
          details_job_id = details.get("job_id") or run.get("job_id") or -1
          if args.debug_multitask:
            print(f"[debug] run_id={run_id} tasks field: {json.dumps(tasks)[:2000]}")
          aggregated_errors: List[str] = []
          details_job_name = details.get("job_name") or run.get("run_name")
          for t in tasks:
            task_key = t.get("task_key")
            task_run_id = t.get("run_id") or t.get("task_run_id")
            try:
              # Prefer calling get-output on the task's own run_id when available
              if task_run_id:
                if args.debug_multitask:
                  print(f"[debug] trying child run_id={task_run_id} for task_key={task_key}")
                task_out = get_run_output(host, token, task_run_id)
              elif task_key:
                # Fallback: some environments may accept parent run + task_key
                if args.debug_multitask:
                  print(f"[debug] trying parent run_id={run_id} with task_key={task_key}")
                task_out = get_run_output(host, token, run_id, task_key=task_key)
              else:
                task_out = None
              task_err = None
              if task_out is not None:
                task_err = task_out.get("error")
              aggregated_errors.append(f"{task_key or task_run_id or 'unknown_task'}: {task_err or 'no error field'}")
            except Exception as te:
              aggregated_errors.append(f"{task_key or task_run_id or 'unknown_task'}: get-output error: {te}")
          if args.debug_multitask and aggregated_errors:
            print(f"[debug] aggregated_errors for run_id={run_id}: {'; '.join(aggregated_errors)[:2000]}")
          # Fall through to add a row with aggregated errors
          state = (run.get("state") or {})
          life_cycle_state = state.get("life_cycle_state")
          result_state = state.get("result_state")
          state_message = state.get("state_message")
          start_time_ms = run.get("start_time")
          end_time_ms = run.get("end_time")
          duration_ms = (end_time_ms - start_time_ms) if (end_time_ms and start_time_ms) else None
          start_time_iso = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).isoformat() if start_time_ms else None
          end_time_iso = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).isoformat() if end_time_ms else None
          # Add one row summarizing the run-level failure (no child task ids)
          is_retry = determine_is_retry(run, None, details)
          attempt_num = determine_attempt_number(run, None, details)
          original_attempt = determine_original_attempt_run_id(run, details)
          err_fields = parse_error_fields("; ".join(aggregated_errors) if aggregated_errors else err_msg)
          rows.append((
            details_job_id,                 # job_id
            run_id,                         # run_id
            details_job_name,               # job_name
            run.get("run_name"),           # run_name
            run.get("run_page_url"),       # run_page_url
            None,                           # child_task_run_id
            None,                           # child_task_key
            "multi_task_run",              # task_type
            start_time_iso,                 # job_start_datetime_iso
            end_time_iso,                   # job_end_datetime_iso
            duration_ms,                    # duration_ms
            life_cycle_state,               # life_cycle_state
            result_state,                   # result_state
            state_message,                  # state_message
            None,                           # termination_code
            None,                           # termination_type
            None,                           # termination_reason
            "; ".join(aggregated_errors) if aggregated_errors else err_msg,
            is_retry,                       # is_retry
            attempt_num,                    # attempt_number
            original_attempt,               # original_attempt_run_id
            err_fields["category"],        # error_category
            err_fields["provider"],        # error_provider
            err_fields["short"],           # error_message_short
          ))
          # Also add one row per task to enable aggregation by task-level failures
          for t in tasks:
            task_key = t.get("task_key")
            task_run_id = t.get("run_id") or t.get("task_run_id")
            # Per-task row (child identifiers filled when present; no aggregated error)
            rows.append((
              details_job_id,               # job_id
              run_id,                       # run_id
              details_job_name,             # job_name
              run.get("run_name"),         # run_name
              run.get("run_page_url"),     # run_page_url
              task_run_id,                  # child_task_run_id
              task_key,                     # child_task_key
              (infer_task_type(t) or None), # task_type
              start_time_iso,               # job_start_datetime_iso
              end_time_iso,                 # job_end_datetime_iso
              duration_ms,                  # duration_ms
              life_cycle_state,             # life_cycle_state
              result_state,                 # result_state
              state_message,                # state_message
              None,                         # termination_code
              None,                         # termination_type
              None,                         # termination_reason
              None,                         # output_error
              is_retry,                     # is_retry
              attempt_num,                  # attempt_number
              original_attempt,             # original_attempt_run_id
              None,                         # error_category
              None,                         # error_provider
              None,                         # error_message_short
            ))
          continue
        except Exception as e_tasks:
          # Could not enumerate tasks; record original error
          state = (run.get("state") or {})
          life_cycle_state = state.get("life_cycle_state")
          result_state = state.get("result_state")
          state_message = state.get("state_message")
          start_time_ms = run.get("start_time")
          end_time_ms = run.get("end_time")
          duration_ms = (end_time_ms - start_time_ms) if (end_time_ms and start_time_ms) else None
          start_time_iso = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).isoformat() if start_time_ms else None
          end_time_iso = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).isoformat() if end_time_ms else None
          is_retry_fallback = determine_is_retry(run, None, details)
          attempt_fallback = determine_attempt_number(run, None, details)
          original_fallback = determine_original_attempt_run_id(run, details)
          err_fields_fb = parse_error_fields(f"get-output multi-task fallback failed: {e_tasks}")
          rows.append((
            (run.get("job_id") or -1),     # job_id
            run_id,                         # run_id
            (run.get("run_name")),         # job_name fallback to run_name
            run.get("run_name"),           # run_name
            run.get("run_page_url"),       # run_page_url
            None,                           # child_task_run_id
            None,                           # child_task_key
            None,                           # task_type
            start_time_iso,                 # job_start_datetime_iso
            end_time_iso,                   # job_end_datetime_iso
            duration_ms,                    # duration_ms
            life_cycle_state,               # life_cycle_state
            result_state,                   # result_state
            state_message,                  # state_message
            None,                           # termination_code
            None,                           # termination_type
            None,                           # termination_reason
            f"get-output multi-task fallback failed: {e_tasks}",
            is_retry_fallback,              # is_retry
            attempt_fallback,               # attempt_number
            original_fallback,              # original_attempt_run_id
            err_fields_fb["category"],      # error_category
            err_fields_fb["provider"],      # error_provider
            err_fields_fb["short"],         # error_message_short
          ))
          continue
      # Generic error fallback
      state = (run.get("state") or {})
      life_cycle_state = state.get("life_cycle_state")
      result_state = state.get("result_state")
      state_message = state.get("state_message")
      start_time_ms = run.get("start_time")
      end_time_ms = run.get("end_time")
      duration_ms = (end_time_ms - start_time_ms) if (end_time_ms and start_time_ms) else None
      start_time_iso = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).isoformat() if start_time_ms else None
      end_time_iso = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).isoformat() if end_time_ms else None
      # Try to fetch details for better job_name/task_type on generic error
      details_ge = None
      try:
        details_ge = get_run_details(host, token, run_id)
      except Exception:
        details_ge = None
      job_name_ge = (details_ge.get("job_name") if details_ge else None) or run.get("run_name")
      task_type_ge = infer_task_type(details_ge or {})
      is_retry_generic = determine_is_retry(run)
      attempt_generic = determine_attempt_number(run)
      original_generic = determine_original_attempt_run_id(run)
      err_fields_ge = parse_error_fields(f"get-output error: {e}")
      rows.append((
        (run.get("job_id") or -1),       # job_id
        run_id,                           # run_id
        job_name_ge,                      # job_name
        run.get("run_name"),             # run_name
        run.get("run_page_url"),         # run_page_url
        None,                             # child_task_run_id
        None,                             # child_task_key
        task_type_ge,                     # task_type
        start_time_iso,                   # job_start_datetime_iso
        end_time_iso,                     # job_end_datetime_iso
        duration_ms,                      # duration_ms
        life_cycle_state,                 # life_cycle_state
        result_state,                     # result_state
        state_message,                    # state_message
        None,                             # termination_code
        None,                             # termination_type
        None,                             # termination_reason
        f"get-output error: {e}",
        is_retry_generic,                 # is_retry
        attempt_generic,                  # attempt_number
        original_generic,                 # original_attempt_run_id
        err_fields_ge["category"],       # error_category
        err_fields_ge["provider"],       # error_provider
        err_fields_ge["short"],          # error_message_short
      ))
      continue

    metadata = out.get("metadata", {})
    state = metadata.get("state") or metadata.get("status") or {}
    term = state.get("termination_details") or {}

    job_id = metadata.get("job_id") or run.get("job_id") or -1
    job_name = metadata.get("job_name")
    run_name = metadata.get("run_name") or run.get("run_name")
    run_page_url = metadata.get("run_page_url") or run.get("run_page_url")
    start_time_ms = metadata.get("start_time") or run.get("start_time")
    end_time_ms = metadata.get("end_time") or run.get("end_time")
    duration_ms = (end_time_ms - start_time_ms) if (end_time_ms and start_time_ms) else None
    start_time_iso = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).isoformat() if start_time_ms else None
    end_time_iso = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).isoformat() if end_time_ms else None
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

    is_retry_ok = determine_is_retry(run, metadata, None)
    attempt_ok = determine_attempt_number(run, metadata, None)
    original_ok = determine_original_attempt_run_id(run, None)
    err_fields_ok = parse_error_fields(output_error or state_message)
    rows.append((
      job_id,               # job_id
      run_id,               # run_id
      job_name,             # job_name
      run_name,             # run_name
      run_page_url,         # run_page_url
      None,                 # child_task_run_id
      None,                 # child_task_key
      task_type,            # task_type
      start_time_iso,       # job_start_datetime_iso
      end_time_iso,         # job_end_datetime_iso
      duration_ms,          # duration_ms
      life_cycle_state,     # life_cycle_state
      result_state,         # result_state
      state_message,        # state_message
      termination_code,     # termination_code
      termination_type,     # termination_type
      termination_reason,   # termination_reason
      output_error,         # output_error
      is_retry_ok,          # is_retry
      attempt_ok,           # attempt_number
      original_ok,          # original_attempt_run_id
      err_fields_ok["category"],
      err_fields_ok["provider"],
      err_fields_ok["short"],
    ))

  df = spark.createDataFrame(rows, schema=schema)
  # Enrich with metadata columns for logging
  try:
    cluster_id_conf = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
  except Exception:
    cluster_id_conf = None

  ingest_ms = int(time.time() * 1000)
  ingest_iso = datetime.fromtimestamp(ingest_ms / 1000, tz=timezone.utc).isoformat()
  start_iso = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
  end_iso = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat()

  df = (
    df
      .withColumn("row_added_at", F.lit(ingest_iso))
  )

  # Ensure strong types in the DataFrame before writing
  df = df.withColumn("job_id", F.col("job_id").cast(LongType()))
  df = df.withColumn("run_id", F.col("run_id").cast(LongType()))
  df = df.withColumn("child_task_run_id", F.col("child_task_run_id").cast(LongType()))
  if not args.no_show:
    df.show(truncate=False)

  # Optional debug table logging for multi-task diagnostics
  if args.debug_multitask and args.debug_table:
    try:
      debug_rows = []
      for run in failed_runs:
        debug_rows.append((
          run.get("job_id"),
          run.get("run_id"),
          json.dumps(run.get("state", {}))[:4000],
          run.get("run_name"),
          run.get("run_page_url"),
          host,
          ingest_iso,
        ))
      debug_schema = StructType([
        StructField("job_id", LongType(), True),
        StructField("run_id", LongType(), True),
        StructField("state_json", StringType(), True),
        StructField("run_name", StringType(), True),
        StructField("run_page_url", StringType(), True),
        StructField("workspace_host", StringType(), True),
        StructField("ingest_iso", StringType(), True),
      ])
      spark.createDataFrame(debug_rows, schema=debug_schema).write.mode("append").saveAsTable(args.debug_table)
    except Exception as e:
      print(f"Warning: failed to write debug table: {e}")

  # Append to logging table if provided
  if args.log_table:
    # Create table with explicit schema if it does not exist to enforce column types
    table_ident = args.log_table
    try:
      required_columns_sql = [
        ("job_id", "BIGINT"),
        ("run_id", "BIGINT"),
        ("job_name", "STRING"),
        ("run_name", "STRING"),
        ("run_page_url", "STRING"),
        ("child_task_run_id", "BIGINT"),
        ("child_task_key", "STRING"),
        ("task_type", "STRING"),
        ("job_start_datetime_iso", "STRING"),
        ("job_end_datetime_iso", "STRING"),
        ("duration_ms", "BIGINT"),
        ("life_cycle_state", "STRING"),
        ("result_state", "STRING"),
        ("state_message", "STRING"),
        ("termination_code", "STRING"),
        ("termination_type", "STRING"),
        ("termination_reason", "STRING"),
        ("output_error", "STRING"),
        ("is_retry", "BOOLEAN"),
        ("attempt_number", "BIGINT"),
        ("original_attempt_run_id", "BIGINT"),
        ("error_category", "STRING"),
        ("error_provider", "STRING"),
        ("error_message_short", "STRING"),
        ("row_added_at", "STRING"),
      ]

      if not spark.catalog.tableExists(table_ident):
        cols_ddl = ",\n            ".join([f"{c} {t}" for c, t in required_columns_sql])
        spark.sql(f"""
          CREATE TABLE {table_ident} (
            {cols_ddl}
          ) USING DELTA
        """)
      else:
        # Add any missing columns explicitly (ACL mode blocks auto-merge)
        try:
          existing_cols = set([f.name for f in spark.table(table_ident).schema.fields])
          missing = [(c, t) for c, t in required_columns_sql if c not in existing_cols]
          if missing:
            add_ddl = ", ".join([f"{c} {t}" for c, t in missing])
            spark.sql(f"ALTER TABLE {table_ident} ADD COLUMNS ({add_ddl})")
        except Exception as e_alter:
          print(f"Warning: could not alter {table_ident} to add missing columns: {e_alter}")
    except Exception as e:
      print(f"Warning: could not ensure table schema for {table_ident}: {e}")

    df.write.mode("append").saveAsTable(table_ident)
    # Apply Unity Catalog metadata if specified
    spark = SparkSession.builder.getOrCreate()
    # Table comment
    if args.table_comment:
      spark.sql(f"COMMENT ON TABLE {table_ident} IS '{_sql_literal(args.table_comment)}'")
    # Table tags (table properties)
    if args.table_tags:
      try:
        props = json.loads(args.table_tags)
        if isinstance(props, dict) and props:
          assignments = ", ".join([f"{k}='{_sql_literal(v)}'" for k, v in props.items()])
          spark.sql(f"ALTER TABLE {table_ident} SET TBLPROPERTIES ({assignments})")
      except Exception as e:
        print(f"Warning: failed to apply table tags: {e}")
    # Column comments
    if args.column_comments:
      try:
        col_comments = json.loads(args.column_comments)
        if isinstance(col_comments, dict):
          for col, comment in col_comments.items():
            spark.sql(f"ALTER TABLE {table_ident} ALTER COLUMN `{col}` COMMENT '{_sql_literal(comment)}'")
      except Exception as e:
        print(f"Warning: failed to apply column comments: {e}")
    # Column tags (properties on columns)
    if args.column_tags:
      try:
        col_tags = json.loads(args.column_tags)
        if isinstance(col_tags, dict):
          for col, tags in col_tags.items():
            if isinstance(tags, dict) and tags:
              assignments = ", ".join([f"{k}='{_sql_literal(v)}'" for k, v in tags.items()])
              spark.sql(f"ALTER TABLE {table_ident} ALTER COLUMN `{col}` SET TAGS ({assignments})")
      except Exception as e:
        print(f"Warning: failed to apply column tags: {e}")
    # Column masking policies
    if args.column_masks:
      try:
        col_masks = json.loads(args.column_masks)
        if isinstance(col_masks, dict):
          for col, policy in col_masks.items():
            if policy:
              spark.sql(f"ALTER TABLE {table_ident} ALTER COLUMN `{col}` SET MASKING POLICY {policy}")
      except Exception as e:
        print(f"Warning: failed to apply column masking policies: {e}")

  return 0


if __name__ == "__main__":
  try:
    sys.exit(main())
  except Exception as exc:
    print(f"Error: {exc}", file=sys.stderr)
    sys.exit(1)


