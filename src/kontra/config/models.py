# src/kontra/config/models.py
from pydantic import BaseModel, Field, model_validator
from typing import Dict, Any, List, Literal, Optional

class RuleSpec(BaseModel):
    """
    Declarative specification for a rule from contract.yml

    The `context` field is for consumer-defined metadata that Kontra stores
    but does not use for validation. Consumers/agents can read context for
    routing, explanations, fix hints, etc.
    """
    name: str = Field(..., description="The rule name (e.g., not_null, unique).")
    id: Optional[str] = Field(default=None, description="Explicit rule ID (optional, auto-generated if not provided).")
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters passed to the rule.")
    severity: Literal["blocking", "warning", "info"] = Field(
        default="blocking",
        description="Rule severity: blocking (fails pipeline), warning (warns but continues), info (logs only)."
    )
    tally: Optional[bool] = Field(
        default=None,
        description="Count all violations (True) or early-stop at first (False/None). None = use global default."
    )
    context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Consumer-defined context (owner, tags, fix_hint, etc.). Stored but not used by Kontra."
    )

class Contract(BaseModel):
    """
    Data contract specification.

    The `datasource` field can be:
    - A named datasource from config: "prod_db.users"
    - A file path: "./data/users.parquet"
    - A URI: "s3://bucket/users.parquet", "postgres:///public.users"
    - Omitted when data is passed directly to validate()
    """
    name: Optional[str] = Field(default=None, description="Contract name (optional, used for identification).")
    datasource: str = Field(default="inline", description="Data source: named datasource, path, or URI. Defaults to 'inline' when data is passed directly.")
    rules: List[RuleSpec] = Field(default_factory=list)

    # Backwards compatibility: accept 'dataset' as alias for 'datasource'
    @model_validator(mode="before")
    @classmethod
    def handle_dataset_alias(cls, data: Any) -> Any:
        """Accept 'dataset' as deprecated alias for 'datasource'."""
        if isinstance(data, dict):
            if "dataset" in data and "datasource" not in data:
                data["datasource"] = data.pop("dataset")
        return data


