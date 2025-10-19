# src/contra/engine/result.py
from typing import List, Dict, Any
from dataclasses import dataclass, asdict

@dataclass
class ValidationResult:
    dataset: str
    results: List[Dict[str, Any]]
    summary: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def passed(self) -> bool:
        return self.summary.get("passed", False)
