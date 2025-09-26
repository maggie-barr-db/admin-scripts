import argparse
from typing import Optional


DEFAULT_LIMIT = 26


def build_parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(description="Scrape failed Databricks job runs into a Spark DataFrame (Jobs API 2.2)")
  parser.add_argument("--start", required=True, help="Start time (ISO 8601 or 'YYYY-MM-DD HH:MM:SS'). UTC assumed if naive")
  parser.add_argument("--end", required=True, help="End time (ISO 8601 or 'YYYY-MM-DD HH:MM:SS'). UTC assumed if naive")
  parser.add_argument("--host", default=None, help="Databricks workspace URL, e.g. https://xyz.cloud.databricks.com")
  parser.add_argument("--token", default=None, help="Databricks personal access token")
  parser.add_argument("--profile", default=None, help="Profile name from ~/.databrickscfg (fallback if no host/token provided)")
  parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Page size for runs/list (max 26; default 26)")
  parser.add_argument("--no-show", action="store_true", help="Do not show the resulting DataFrame")
  parser.add_argument("--log-table", default=None, help="Fully qualified table to append results to, e.g. catalog.schema.table")
  # Unity Catalog metadata (optional)
  parser.add_argument("--table-comment", default=None, help="Unity Catalog table comment for data discovery")
  parser.add_argument("--table-tags", default=None, help="JSON object of UC table tags: {\"tag\": \"value\", ...}")
  parser.add_argument("--column-comments", default=None, help="JSON object of column->comment mappings")
  parser.add_argument("--column-tags", default=None, help="JSON object of column->{tag->value} mappings")
  parser.add_argument("--column-masks", default=None, help="JSON object of column->masking_policy_name mappings")
  return parser


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
  return build_parser().parse_args(argv)


