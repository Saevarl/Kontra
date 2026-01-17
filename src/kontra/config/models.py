# src/kontra/config/models.py
from pydantic import BaseModel, Field, model_validator
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


