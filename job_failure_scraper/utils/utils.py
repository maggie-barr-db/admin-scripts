from typing import Any, Dict, Optional
from datetime import datetime, timezone

from pyspark.sql.types import (
  StructType,
  StructField,
  StringType,
  LongType,
  BooleanType,
)

def infer_task_type(obj: Dict[str, Any]) -> Optional[str]:
  task_obj = obj.get("task") if isinstance(obj, dict) else None
  candidates = task_obj if isinstance(task_obj, dict) and task_obj else obj
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
    if isinstance(candidates, dict) and k in candidates:
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
  s = str(value)
  return s.replace("\\", "\\\\").replace("'", "''")


def _as_int(value: Any) -> Optional[int]:
  try:
    return int(value) if value is not None else None
  except Exception:
    return None


def determine_is_retry(run: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None, details: Optional[Dict[str, Any]] = None) -> bool:
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