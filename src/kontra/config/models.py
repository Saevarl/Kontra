# src/contra/config/models.py
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

class RuleSpec(BaseModel):
    """
    Declarative specification for a rule from contract.yml
    """
    name: str = Field(..., description="The rule name (e.g., not_null, unique).")
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters passed to the rule.")

class Contract(BaseModel):
    dataset: str
    rules: List[RuleSpec]


