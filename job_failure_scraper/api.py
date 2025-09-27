import time
from typing import Any, Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


def list_failed_runs(host: str, token: str, start_time_from_ms: int, start_time_to_ms: int, limit: int = 26) -> List[Dict[str, Any]]:
  session = build_session()
  headers = {"Authorization": f"Bearer {token}"}
  url = f"{host}/api/2.2/jobs/runs/list"

  if not isinstance(limit, int) or limit <= 0:
    limit = 26
  if limit > 26:
    limit = 26

  failed_runs: List[Dict[str, Any]] = []
  page_token: Optional[str] = None

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

    if payload.get("has_more"):
      page_token = payload.get("next_page_token")
      if not page_token:
        break
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


