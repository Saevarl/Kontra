from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Union
import os
import yaml

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
        p = Path(path)
        if not p.exists():
            raise ContractNotFoundError(str(p))
        with p.open("r") as f:
            raw = yaml.safe_load(f)
        return ContractLoader._parse_and_validate(raw, source=str(p))

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
