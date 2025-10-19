from __future__ import annotations
from typing import Protocol, List, Dict, Any


class SqlRuleExecutor(Protocol):
    """
    Pluggable SQL pushdown executor.

    Responsibilities:
      - Decide if it can operate against a given source URI.
      - Compile backend-agnostic SQL rule specs into a backend-native plan.
      - Execute the compiled plan and return Contra-style result dicts.
      - Provide lightweight dataset introspection without materializing rows.

    Contract (current):
      - 'sql_specs' is list[dict] with minimal fields:
          {"kind": "not_null"|"min_rows"|"max_rows", "rule_id": str, ...}
      - compile(sql_specs) -> backend-native plan object (e.g., SQL string)
      - execute(source_uri, compiled_plan) -> {"results": [ {rule_id, passed, ...}, ... ]}
      - introspect(source_uri) -> {"row_count": int, "available_cols": list[str]}
    """

    name: str

    def compile(self, sql_specs: List[Dict[str, Any]]) -> Any:
        ...

    def execute(self, source_uri: str, compiled_plan: Any) -> Dict[str, Any]:
        ...

    def introspect(self, source_uri: str) -> Dict[str, Any]:
        ...
