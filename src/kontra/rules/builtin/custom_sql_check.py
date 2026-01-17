from __future__ import annotations
from typing import Dict, Any, Optional
import polars as pl
import duckdb

from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule
from kontra.state.types import FailureMode

@register_rule("custom_sql_check")
class CustomSQLCheck(BaseRule):
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        # Accept both 'sql' (documented) and 'query' (legacy) parameter names
        query = self.params.get("sql") or self.params.get("query")
        if not query:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": "Missing 'sql' parameter",
            }

        # Substitute {table} placeholder with the registered table name
        query = query.replace("{table}", "data")

        try:
            # Use DuckDB's native Polars support (zero-copy)
            con = duckdb.connect()
            con.register("data", df)
            result = con.execute(query).pl()
            failed_count = len(result)

            res: Dict[str, Any] = {
                "rule_id": self.rule_id,
                "passed": failed_count == 0,
                "failed_count": failed_count,
                "message": f"Custom SQL check failed for {failed_count} rows",
            }

            if failed_count > 0:
                res["failure_mode"] = str(FailureMode.CUSTOM_CHECK_FAILED)
                res["details"] = {
                    "query": query,
                    "failed_row_count": failed_count,
                }

            return res
        except Exception as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Rule execution failed: {e}",
            }

    def compile_predicate(self):
        return None  # fallback-only in v2
