from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl
import duckdb

from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule

@register_rule("custom_sql_check")
class CustomSQLCheck(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        query = self.params.get("query")
        if not query:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": "Missing 'query' parameter",
            }
        try:
            # duckdb.query_df expects pandas; convert from Polars.
            pdf = df.to_pandas()
            result = duckdb.query_df(pdf, "data", query).to_df()
            failed_count = len(result)
            return {
                "rule_id": self.rule_id,
                "passed": failed_count == 0,
                "failed_count": failed_count,
                "message": f"Custom SQL check failed for {failed_count} rows",
            }
        except Exception as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Rule execution failed: {e}",
            }

    def compile_predicate(self):
        return None  # fallback-only in v2
