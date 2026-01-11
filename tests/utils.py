# tests/utils.py
def collect_counts(out):
    summary = out["summary"]
    stats = out.get("stats", {})
    proj = (stats or {}).get("projection", {})
    # Handle nested structure: required_count is under "full", loaded_count under "residual"
    full = proj.get("full", {})
    residual = proj.get("residual", {})
    return {
        "rules_total": summary["total_rules"],
        "rules_passed": summary["rules_passed"],
        "rules_failed": summary["rules_failed"],
        "required_count": full.get("required_count"),
        "loaded_count": residual.get("loaded_count"),
        "available_count": proj.get("available_count"),
    }

def by_rule_id(out):
    return {r.get("rule_id", ""): r for r in out["results"]}


