# src/contra/config/models.py
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Literal, Optional

class RuleSpec(BaseModel):
    """
    Declarative specification for a rule from contract.yml
    """
    name: str = Field(..., description="The rule name (e.g., not_null, unique).")
    id: Optional[str] = Field(default=None, description="Explicit rule ID (optional, auto-generated if not provided).")
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters passed to the rule.")
    severity: Literal["blocking", "warning", "info"] = Field(
        default="blocking",
        description="Rule severity: blocking (fails pipeline), warning (warns but continues), info (logs only)."
    )

class Contract(BaseModel):
    name: Optional[str] = Field(default=None, description="Contract name (optional, used for identification).")
    dataset: str
    rules: List[RuleSpec]


