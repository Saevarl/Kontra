from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
import os

from kontra.config.models import Contract, RuleSpec
from kontra.errors import ContractNotFoundError


class ContractLoader:
    """Static helpers to load a Contract from different sources."""

    @staticmethod
    def from_uri(uri: Union[str, Path]) -> Contract:
        uri_str = str(uri)
        if uri_str.lower().startswith("s3://"):
            return ContractLoader.from_s3(uri_str)
        return ContractLoader.from_path(uri_str)

    @staticmethod
    def from_path(path: Union[str, Path]) -> Contract:
        import yaml

        p = Path(path)
        if not p.exists():
            raise ContractNotFoundError(str(p))
        with p.open("r") as f:
            raw = yaml.safe_load(f)
        # Resolve extends before parsing
        raw = ContractLoader._resolve_extends(raw, str(p.resolve()))
        return ContractLoader._parse_and_validate(raw, source=str(p))

    @staticmethod
    def _resolve_extends(
        raw: Any,
        source_path: str,
        _visited: Optional[Set[str]] = None,
    ) -> Any:
        """
        Resolve contract inheritance via the 'extends' field.

        Recursively loads base contracts and prepends their rules.
        Paths are resolved relative to the child contract's directory.

        Only `rules` are inherited — `name` and `datasource` are NOT inherited.
        Cycle detection prevents infinite loops.

        Args:
            raw: Raw YAML dict
            source_path: Absolute path of the contract file being loaded
            _visited: Set of already-visited absolute paths (cycle detection)

        Returns:
            The raw dict with base rules prepended and 'extends' key removed
        """
        if not isinstance(raw, dict):
            return raw

        extends = raw.get("extends")
        if not extends:
            return raw

        if _visited is None:
            _visited = set()

        # Add current file to visited set
        _visited.add(source_path)

        # Normalize to list
        extends_list = [extends] if isinstance(extends, str) else extends
        if not isinstance(extends_list, list):
            raise ValueError(
                f"Contract 'extends' must be a string or list of strings, "
                f"got {type(extends_list).__name__}"
            )

        source_dir = Path(source_path).parent
        all_base_rules: List[Any] = []
        seen_in_this_list: set = set()  # Track duplicates within this extends list (BUG-032)

        for ext_path in extends_list:
            if not isinstance(ext_path, str):
                raise ValueError(
                    f"Each entry in 'extends' must be a string path, "
                    f"got {type(ext_path).__name__}"
                )
            resolved = (source_dir / ext_path).resolve()
            key = str(resolved)

            # Check for duplicate path in the same extends list (BUG-032)
            if key in seen_in_this_list:
                raise ValueError(
                    f"Duplicate base contract path in 'extends': {ext_path} "
                    f"(resolved to {key})"
                )
            seen_in_this_list.add(key)

            if key in _visited:
                raise ValueError(f"Circular contract inheritance detected: {key}")

            if not resolved.exists():
                raise ContractNotFoundError(str(resolved))

            import yaml

            with resolved.open("r") as f:
                base_raw = yaml.safe_load(f)

            if not isinstance(base_raw, dict):
                raise ValueError(
                    f"Base contract at {resolved} is not a valid YAML mapping"
                )

            # Recurse — base can extend another base
            base_raw = ContractLoader._resolve_extends(
                base_raw, str(resolved), _visited
            )
            base_rules = base_raw.get("rules", []) or []
            if not base_rules:
                import warnings
                warnings.warn(
                    f"Base contract at {resolved} has no 'rules' key. "
                    f"No rules will be inherited from this base.",
                    UserWarning,
                    stacklevel=3,
                )
            all_base_rules.extend(base_rules)

        # Prepend base rules, child rules come after
        raw["rules"] = all_base_rules + (raw.get("rules") or [])
        # Remove extends key — it's been resolved
        raw.pop("extends", None)
        return raw

    # ---------- NEW/UPDATED S3 LOADER ----------
    @staticmethod
    def _s3_storage_options() -> Dict[str, Any]:
        """
        Build fsspec/s3fs storage_options from env. Works with AWS S3 and MinIO.
        """
        opts: Dict[str, Any] = {"anon": False}

        key = os.getenv("AWS_ACCESS_KEY_ID")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if key and secret:
            opts["key"] = key
            opts["secret"] = secret

        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            # MinIO/custom endpoints
            opts["client_kwargs"] = {"endpoint_url": endpoint}
            # Path-style is typical for MinIO
            opts["config_kwargs"] = {"s3": {"addressing_style": "path"}}
            # Use SSL only if endpoint is https
            opts["use_ssl"] = endpoint.startswith("https")

        region = os.getenv("AWS_REGION")
        if region:
            opts.setdefault("client_kwargs", {})
            opts["client_kwargs"].setdefault("region_name", region)

        return opts

    @staticmethod
    def from_s3(uri: str) -> Contract:
        import yaml

        """
        Load contract YAML from S3/MinIO using s3fs via fsspec with storage_options.
        Requires: pip install s3fs
        """
        try:
            import fsspec  # s3fs discovered by fsspec
        except ImportError as e:
            raise RuntimeError(
                "Reading contracts from S3 requires 's3fs'. Install with: pip install s3fs"
            ) from e

        storage_options = ContractLoader._s3_storage_options()

        try:
            fs = fsspec.filesystem("s3", **storage_options)
            with fs.open(uri, mode="r") as f:
                raw = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Contract file not found on S3: {uri}")
        except PermissionError as e:
            raise RuntimeError(f"Failed to read contract from S3 '{uri}': Permission denied") from e
        except Exception as e:
            raise RuntimeError(f"Failed to read contract from S3 '{uri}': {e}") from e

        return ContractLoader._parse_and_validate(raw, source=uri)

    # ----------------- unchanged -----------------
    @staticmethod
    def _parse_and_validate(raw: Any, source: str) -> Contract:
        if not isinstance(raw, dict):
            raise ValueError(
                f"Invalid or empty contract YAML at {source}. "
                "Expected a mapping with keys like 'datasource' and 'rules'."
            )
        # datasource is optional - defaults to "inline" when data is passed directly
        rules_raw = raw.get("rules", []) or []
        if not isinstance(rules_raw, list):
            raise ValueError("Contract 'rules' must be a list.")

        rules: List[RuleSpec] = []
        for i, r in enumerate(rules_raw):
            if not isinstance(r, dict):
                raise ValueError(f"Rule at index {i} is not a mapping.")
            if "name" not in r:
                raise ValueError(f"Rule at index {i} missing required key: 'name'.")
            params = r.get("params", {}) or {}
            if not isinstance(params, dict):
                raise ValueError(f"Rule at index {i} has non-dict 'params'.")
            context = r.get("context", {}) or {}
            if not isinstance(context, dict):
                raise ValueError(f"Rule at index {i} has non-dict 'context'.")
            rules.append(RuleSpec(
                name=r["name"],
                id=r.get("id"),
                params=params,
                severity=r.get("severity", "blocking"),
                tally=r.get("tally"),  # None = use global default, True/False = explicit
                context=context,
            ))

        # Use 'datasource' if present, otherwise fall back to 'dataset' for backwards compat
        # If neither is present, default to "inline" (handled by Contract model)
        datasource_value = raw.get("datasource") or raw.get("dataset") or "inline"
        return Contract(
            name=raw.get("name"),
            datasource=str(datasource_value),
            rules=rules,
        )
