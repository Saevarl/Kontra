
#!/usr/bin/env python3
"""
parquet_meta.py — inspect Parquet metadata and (optionally) simulate pruning.

Usage examples:
  python parquet_meta.py /path/to/file.parquet
  python parquet_meta.py "/data/sales/dt=2025-09-*/part-*.parquet"
  python parquet_meta.py /path/*.parquet --json
  python parquet_meta.py /path/*.parquet --keep "ts>=2025-09-01" --keep "amount>=0"
  python parquet_meta.py /path/*.parquet --keep "ts>=2025-09-01" --columns ts,amount --manifest out.json
"""

import argparse
import json
import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

Predicate = Tuple[str, str, Any]  # (column, op, value)

def _parse_value(s: str) -> Any:
    if re.fullmatch(r"[+-]?\d+", s):
        try: return int(s)
        except Exception: pass
    if re.fullmatch(r"[+-]?\d+\.\d+", s):
        try: return float(s)
        except Exception: pass
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return date.fromisoformat(s)
    except Exception: pass
    try:
        return datetime.fromisoformat(s)
    except Exception: pass
    return s

def parse_predicate(expr: str) -> Predicate:
    expr = expr.strip().replace(" ", "")
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_\.]*)\^(=)(.+)$", expr)
    if m:
        col, _, val = m.groups()
        return (col, "^=", val)
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_\.]*)(==|!=|>=|<=|>|<)(.+)$", expr)
    if not m:
        raise ValueError(f"Could not parse predicate '{expr}'")
    col, op, sval = m.groups()
    return (col, op, _parse_value(sval))

def _to_py(val: Any) -> Any:
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val

def collect_metadata(path: str) -> Dict[str, Any]:
    pf = pq.ParquetFile(path)
    md = pf.metadata
    schema = md.schema
    try:
        schema_names = list(schema.names)
    except Exception:
        try:
            schema_names = [f.name for f in schema.to_arrow_schema()]
        except Exception:
            schema_names = []
    out = {
        "path": path,
        "created_by": md.created_by,
        "num_rows": md.num_rows,
        "num_row_groups": md.num_row_groups,
        "schema": schema_names,
        "row_groups": []
    }
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        rg_info = {"index": i, "num_rows": rg.num_rows, "columns": {}}
        for j in range(rg.num_columns):
            col = rg.column(j)
            try:
                name = str(col.path_in_schema)
            except Exception:
                name = schema_names[j] if j < len(schema_names) else f"col_{j}"
            stats = col.statistics
            if stats is None:
                rg_info["columns"][name] = {"stats": None}
            else:
                rg_info["columns"][name] = {
                    "stats": {
                        "min": _to_py(stats.min) if getattr(stats, "has_min_max", True) else None,
                        "max": _to_py(stats.max) if getattr(stats, "has_min_max", True) else None,
                        "null_count": stats.null_count if getattr(stats, "has_null_count", True) else None,
                        "distinct_count": getattr(stats, "distinct_count", None)
                    }
                }
        out["row_groups"].append(rg_info)
    return out

def _cmp_min_max(op: str, value: Any, stats: Dict[str, Any]) -> Optional[bool]:
    if stats is None or (stats.get("min") is None and stats.get("max") is None):
        return None
    mn = stats.get("min")
    mx = stats.get("max")
    try:
        if isinstance(mn, str) and not isinstance(value, str):
            value = str(value)
    except Exception:
        pass
    if op == "==":
        if mn is not None and mx is not None and (value < mn or value > mx):
            return False
        return True
    if op == "!=":
        return True
    if op == ">=":
        if mx is not None and mx < value: return False
        return True
    if op == "<=":
        if mn is not None and mn > value: return False
        return True
    if op == ">":
        if mx is not None and mx <= value: return False
        return True
    if op == "<":
        if mn is not None and mn >= value: return False
        return True
    return None

def _prefix_maybe(prefix: str, stats: Dict[str, Any]) -> Optional[bool]:
    if stats is None or stats.get("min") is None or stats.get("max") is None:
        return None
    mn = stats["min"]; mx = stats["max"]
    if not isinstance(mn, str) or not isinstance(mx, str):
        return None
    upper = prefix + "\uffff"
    if upper < mn or prefix > mx:
        return False
    return True

def prune_row_groups(meta: Dict[str, Any], predicates: List[Predicate]) -> List[int]:
    keep = []
    for rg in meta["row_groups"]:
        maybe = True
        for (col, op, val) in predicates:
            col_info = rg["columns"].get(col)
            stats = None if col_info is None else col_info.get("stats")
            verdict = _prefix_maybe(str(val), stats) if op == "^=" else _cmp_min_max(op, val, stats)
            if verdict is False:
                maybe = False; break
        if maybe:
            keep.append(rg["index"])
    return keep

def build_manifest(metas, keep_predicates, columns):
    files = []
    for m in metas:
        rgs = prune_row_groups(m, keep_predicates) if keep_predicates else list(range(m["num_row_groups"]))
        if rgs:
            files.append({"path": m["path"], "row_groups": rgs})
    return {"columns": columns or [], "files": files}

def main():
    import argparse, json
    from pathlib import Path
    ap = argparse.ArgumentParser(description="Inspect Parquet metadata and simulate pruning.")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--keep", action="append", default=[])
    ap.add_argument("--columns", type=str, default="")
    ap.add_argument("--manifest", type=str)
    args = ap.parse_args()

    paths = []
    for p in args.paths:
        if any(sym in p for sym in ["*", "?", "["]):
            paths.extend([str(x) for x in Path().glob(p)])
        else:
            paths.append(p)

    predicates = [parse_predicate(k) for k in args.keep]
    columns = [c for c in (args.columns.split(",") if args.columns else []) if c]

    metas = []
    for path in paths:
        try:
            metas.append(collect_metadata(path))
        except Exception as e:
            print(f"[WARN] Failed to read {path}: {e}")

    if args.json:
        print(json.dumps(metas, indent=2))
    else:
        for m in metas:
            print(f"\n=== {m['path']} ===")
            print(f"created_by: {m['created_by']}  rows: {m['num_rows']}  row_groups: {m['num_row_groups']}")
            cols = ", ".join(m["schema"])
            print(f"columns: [{cols}]")
            if predicates:
                keep = prune_row_groups(m, predicates)
                print(f"keep_row_groups (by {args.keep}): {keep if keep else 'NONE'}")
            else:
                print("keep_row_groups: (no predicates) -> ALL")
            show_cols = columns if columns else m["schema"][: min(5, len(m['schema']))]
            print("row_groups:")
            for rg in m["row_groups"]:
                stats_short = []
                for c in show_cols:
                    s = rg["columns"].get(c, {}).get("stats")
                    if s is None:
                        stats_short.append(f"{c}: ?")
                    else:
                        smin = s.get("min"); smax = s.get("max"); nnull = s.get("null_count")
                        stats_short.append(f"{c}: [{smin},{smax}] nulls={nnull}")
                print(f"  - rg#{rg['index']} rows={rg['num_rows']}  " + "  ".join(stats_short))

    if args.manifest:
        manifest = build_manifest(metas, predicates, columns or None)
        with open(args.manifest, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n[OK] Wrote manifest to {args.manifest}")

if __name__ == "__main__":
    main()
