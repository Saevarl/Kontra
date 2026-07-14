"""Secure, protocol-independent service layer for the official Kontra MCP."""

from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


_MAX_HISTORY = 100
_MAX_SAMPLE_ROWS = 100_000
_MAX_COLUMNS = 100
_MAX_KEYS = 8
_DEFAULT_MAX_PROBE_ROWS = 100_000


@dataclass(frozen=True)
class MCPSettings:
    """Immutable server configuration loaded once at startup."""

    postgres_uri: str
    contracts_dir: Path
    config_path: Path | None = None
    max_probe_rows: int = _DEFAULT_MAX_PROBE_ROWS

    def __post_init__(self) -> None:
        """Normalize paths even when settings are constructed programmatically."""
        object.__setattr__(self, "contracts_dir", self.contracts_dir.expanduser().resolve())
        if self.config_path is not None:
            object.__setattr__(self, "config_path", self.config_path.expanduser().resolve())
        if (
            not isinstance(self.max_probe_rows, int)
            or isinstance(self.max_probe_rows, bool)
            or self.max_probe_rows < 1
        ):
            raise ValueError("max_probe_rows must be a positive integer")

    @classmethod
    def from_env(cls) -> "MCPSettings":
        uri = os.getenv("KONTRA_MCP_POSTGRES_URI") or os.getenv("DATABASE_URL")
        if not uri:
            # ``postgres://`` delegates credentials to the standard PG* vars.
            if os.getenv("PGDATABASE"):
                uri = "postgres://"
            else:
                raise ValueError(
                    "PostgreSQL is required. Set KONTRA_MCP_POSTGRES_URI, "
                    "DATABASE_URL, or the standard PG* variables."
                )
        if not uri.startswith(("postgres://", "postgresql://")):
            raise ValueError("The MCP backend must be a PostgreSQL URI")

        config = os.getenv("KONTRA_CONFIG")
        contracts = Path(os.getenv("KONTRA_MCP_CONTRACTS_DIR", "contracts"))
        try:
            max_probe_rows = int(
                os.getenv("KONTRA_MCP_MAX_PROBE_ROWS", str(_DEFAULT_MAX_PROBE_ROWS))
            )
        except ValueError as exc:
            raise ValueError("KONTRA_MCP_MAX_PROBE_ROWS must be an integer") from exc
        return cls(
            postgres_uri=uri,
            contracts_dir=contracts.expanduser().resolve(),
            config_path=Path(config).expanduser().resolve() if config else None,
            max_probe_rows=max_probe_rows,
        )


class KontraMCPService:
    """Bounded adapter over Kontra's measurement and history APIs."""

    def __init__(self, settings: MCPSettings):
        import kontra
        from kontra.scout.store import get_profile_store
        from kontra.state.backends import get_store

        self.settings = settings
        if settings.config_path is not None:
            kontra.set_config(str(settings.config_path))
        self._state_store = get_store(settings.postgres_uri)
        self._profile_store = get_profile_store(settings.postgres_uri)
        # Existing Postgres stores each own one long-lived psycopg connection.
        # Serialize service calls so concurrent Streamable HTTP requests never
        # interleave operations or transactions on those connections.
        self._store_lock = threading.RLock()

    def close(self) -> None:
        """Close owned PostgreSQL connections."""
        self._profile_store.close()
        self._state_store.close()

    def health(self) -> dict[str, Any]:
        import kontra

        result = kontra.health()
        # The agent needs readiness, not the server's absolute filesystem layout.
        result.pop("config_path", None)
        result["mcp"] = {
            "status": "ready",
            "state_backend": "postgresql",
            "profile_backend": "postgresql",
            "config_explicit": self.settings.config_path is not None,
        }
        return result

    def list_rules(self) -> list[dict[str, Any]]:
        import kontra

        return kontra.list_rules()

    def list_datasources(self) -> dict[str, list[str]]:
        import kontra

        return kontra.list_datasources()

    def validate(
        self,
        datasource: str,
        contract: str,
        *,
        env: str | None = None,
        tally: bool | None = None,
        sample: int = 0,
    ) -> dict[str, Any]:
        import kontra

        with self._store_lock:
            self._require_datasource(datasource)
            contract_path = self._contract_path(contract)
            if not isinstance(sample, int) or isinstance(sample, bool) or not 0 <= sample <= 100:
                raise ValueError("sample must be an integer between 0 and 100")
            result = kontra.validate(
                datasource,
                str(contract_path),
                env=env,
                tally=tally,
                sample=sample,
                save=True,
                state_store=self._state_store,
            )
        return result.to_dict()

    def profile(
        self,
        datasource: str,
        *,
        preset: str = "scan",
        columns: list[str] | None = None,
        sample: int | None = None,
        save: bool = True,
    ) -> dict[str, Any]:
        import kontra
        from kontra.scout.store import create_profile_state

        with self._store_lock:
            self._require_datasource(datasource)
            if preset not in {"scout", "scan", "interrogate"}:
                raise ValueError("preset must be scout, scan, or interrogate")
            if columns is not None and len(columns) > _MAX_COLUMNS:
                raise ValueError(f"columns is limited to {_MAX_COLUMNS} entries")
            if sample is not None and (
                not isinstance(sample, int)
                or isinstance(sample, bool)
                or not 1 <= sample <= _MAX_SAMPLE_ROWS
            ):
                raise ValueError(f"sample must be between 1 and {_MAX_SAMPLE_ROWS}")

            result = kontra.profile(
                datasource, preset=preset, columns=columns, sample=sample, save=False
            )
            if save:
                state = create_profile_state(result)
                # Keep the resolved-source fingerprint for history lookup while
                # storing only the trusted alias as agent-visible provenance.
                state.source_uri = datasource
                state.profile.source_uri = datasource
                self._profile_store.save(state)
        return self._profile_payload(result.to_dict(), datasource)

    def validation_history(
        self,
        contract: str,
        *,
        limit: int = 20,
        since: str | None = None,
        failed_only: bool = False,
    ) -> list[dict[str, Any]]:
        from kontra.config.loader import ContractLoader
        from kontra.state.fingerprint import fingerprint_contract

        with self._store_lock:
            limit = self._limit(limit)
            contract_obj = ContractLoader.from_path(str(self._contract_path(contract)))
            fingerprint = fingerprint_contract(contract_obj)
            summaries = self._state_store.get_run_summaries(
                fingerprint,
                limit=limit,
                since=self._parse_since(since),
                failed_only=failed_only,
            )
        return [summary.to_dict() for summary in summaries]

    def validation_diff(self, contract: str) -> dict[str, Any]:
        from kontra.api.results import Diff
        from kontra.config.loader import ContractLoader
        from kontra.state.fingerprint import fingerprint_contract
        from kontra.state.types import StateDiff

        with self._store_lock:
            contract_obj = ContractLoader.from_path(str(self._contract_path(contract)))
            states = self._state_store.get_history(fingerprint_contract(contract_obj), limit=2)
        if len(states) < 2:
            return Diff.empty("Need at least 2 validation runs to compute a diff").to_dict()
        return Diff.from_state_diff(StateDiff.compute(states[1], states[0])).to_dict()

    def get_validation_run(
        self, contract: str, *, run_id: str | None = None
    ) -> dict[str, Any] | None:
        """Get a persisted run by ID, or the latest run when no ID is given."""
        with self._store_lock:
            if run_id is not None and re.fullmatch(r"[1-9][0-9]{0,18}", run_id) is None:
                raise ValueError("run_id must be a positive numeric PostgreSQL run ID")
            states = self._state_store.get_history(
                self._contract_fingerprint(contract), limit=_MAX_HISTORY
            )
            if not states:
                return None
            state = states[0] if run_id is None else next(
                (
                    item
                    for item in states
                    if str(item.id) == run_id
                ),
                None,
            )
            if state is None:
                raise ValueError(f"Unknown run ID for contract: {run_id}")
            payload = state.to_dict()
            # Resolved dataset URIs may contain credentials and internal hostnames.
            # The trusted contract name identifies the run without exposing them.
            payload.pop("dataset_uri", None)
            payload.pop("contract_fingerprint", None)
            payload.pop("dataset_fingerprint", None)
            return payload

    def measure_failure_samples(
        self,
        datasource: str,
        contract: str,
        rule_id: str,
        *,
        n: int = 5,
        env: str | None = None,
    ) -> dict[str, Any]:
        """Measure bounded failure samples against current data, without saving a run."""
        import kontra

        if not isinstance(rule_id, str) or not rule_id.strip():
            raise ValueError("rule_id must be a non-empty rule identifier")
        if not isinstance(n, int) or isinstance(n, bool) or not 1 <= n <= 100:
            raise ValueError("n must be an integer between 1 and 100")
        with self._store_lock:
            self._require_datasource(datasource)
            self._enforce_probe_row_limit(datasource)
            result = kontra.validate(
                datasource,
                str(self._contract_path(contract)),
                env=env,
                sample=n,
                sample_budget=n,
                sample_columns="relevant",
                save=False,
            )
            rule = next((item for item in result.rules if item.rule_id == rule_id), None)
            if rule is None:
                raise ValueError(f"Rule not found: {rule_id}")
            samples = result.sample_failures(rule_id, n=n, upgrade_tier=True)
            return {
                "measurement": "current",
                "historical_run_id": None,
                "rule_id": rule_id,
                "passed": rule.passed,
                "failed_count": rule.failed_count,
                "tally": rule.tally,
                "sample_columns": "relevant",
                "samples": samples.to_dict(),
            }

    def profile_history(self, datasource: str, *, limit: int = 20) -> list[dict[str, Any]]:
        from kontra.config.settings import resolve_datasource
        from kontra.scout.store import fingerprint_source

        with self._store_lock:
            self._require_datasource(datasource)
            states = self._profile_store.get_history(
                fingerprint_source(resolve_datasource(datasource)), limit=self._limit(limit)
            )
        return [
            self._profile_payload(state.profile.to_dict(), datasource)
            for state in states
        ]

    def profile_diff(self, datasource: str) -> dict[str, Any] | None:
        """Compare the two latest persisted profiles for a named datasource."""
        from kontra.config.settings import resolve_datasource
        from kontra.scout.store import fingerprint_source
        from kontra.scout.types import ProfileDiff

        with self._store_lock:
            self._require_datasource(datasource)
            states = self._profile_store.get_history(
                fingerprint_source(resolve_datasource(datasource)), limit=2
            )
            if len(states) < 2:
                return None
            payload = ProfileDiff.compute(states[1], states[0]).to_dict()
            payload["before"]["source_uri"] = datasource
            payload["after"]["source_uri"] = datasource
            return payload

    def compare_datasets(
        self,
        before: str,
        after: str,
        *,
        key: str | list[str] | None = None,
        before_key: str | list[str] | None = None,
        after_key: str | list[str] | None = None,
    ) -> dict[str, Any]:
        """Measure structural differences between two configured datasources."""
        import kontra

        self._validate_probe_inputs(before, after, key, before_key, after_key)
        with self._store_lock:
            self._enforce_probe_row_limit(before, after)
            result = kontra.compare(
                before,
                after,
                key=key,
                before_key=before_key,
                after_key=after_key,
                sample_limit=0,
                save=False,
            )
            return result.to_dict()

    def profile_relationship(
        self,
        left: str,
        right: str,
        *,
        on: str | list[str] | None = None,
        left_on: str | list[str] | None = None,
        right_on: str | list[str] | None = None,
    ) -> dict[str, Any]:
        """Measure the relational shape of two configured datasources."""
        import kontra

        self._validate_probe_inputs(left, right, on, left_on, right_on)
        with self._store_lock:
            self._enforce_probe_row_limit(left, right)
            result = kontra.profile_relationship(
                left,
                right,
                on=on,
                left_on=left_on,
                right_on=right_on,
                sample_limit=0,
                save=False,
            )
            return result.to_dict()

    def _require_datasource(self, datasource: str) -> None:
        if not isinstance(datasource, str) or not datasource.strip():
            raise ValueError("datasource must be a configured datasource name")
        if "://" in datasource or datasource.startswith(("/", ".", "~")):
            raise ValueError("Only configured datasource names are accepted")
        configured = self.list_datasources()
        root, separator, table = datasource.partition(".")
        if root not in configured or (separator and table not in configured[root]):
            raise ValueError(f"Unknown configured datasource: {datasource}")

    def _contract_path(self, contract: str) -> Path:
        if not isinstance(contract, str) or not contract.strip():
            raise ValueError("contract must be a trusted contract name")
        candidate = Path(contract)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError("Contract must be inside the configured contracts directory")
        if candidate.suffix not in {".yml", ".yaml"}:
            candidate = candidate.with_suffix(".yml")
        resolved = (self.settings.contracts_dir / candidate).resolve()
        if resolved.parent != self.settings.contracts_dir and self.settings.contracts_dir not in resolved.parents:
            raise ValueError("Contract must be inside the configured contracts directory")
        if not resolved.is_file():
            raise ValueError(f"Unknown contract: {contract}")
        return resolved

    def _contract_fingerprint(self, contract: str) -> str:
        from kontra.config.loader import ContractLoader
        from kontra.state.fingerprint import fingerprint_contract

        return fingerprint_contract(ContractLoader.from_path(str(self._contract_path(contract))))

    def _validate_probe_inputs(
        self,
        left: str,
        right: str,
        symmetric: str | list[str] | None,
        left_keys: str | list[str] | None,
        right_keys: str | list[str] | None,
    ) -> None:
        self._require_datasource(left)
        self._require_datasource(right)
        values = [value for value in (symmetric, left_keys, right_keys) if value is not None]
        for value in values:
            keys = [value] if isinstance(value, str) else value
            if not isinstance(keys, list) or not keys or len(keys) > _MAX_KEYS:
                raise ValueError(f"Probe keys must contain between 1 and {_MAX_KEYS} columns")
            if any(not isinstance(item, str) or not item.strip() for item in keys):
                raise ValueError("Probe key columns must be non-empty strings")

    def _enforce_probe_row_limit(self, *datasources: str) -> None:
        import kontra

        for datasource in datasources:
            profile = kontra.profile(datasource, preset="scout", save=False)
            if profile.row_count > self.settings.max_probe_rows:
                estimate = "estimated " if profile.row_count_estimated else ""
                raise ValueError(
                    f"Probe input '{datasource}' has {estimate}{profile.row_count:,} rows; "
                    f"the MCP materialization limit is {self.settings.max_probe_rows:,}."
                )

    @staticmethod
    def _profile_payload(payload: dict[str, Any], datasource: str) -> dict[str, Any]:
        """Expose the configured alias, never a resolved URI or internal hostname."""
        payload["source_uri"] = datasource
        return payload

    @staticmethod
    def _limit(limit: int) -> int:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= _MAX_HISTORY:
            raise ValueError(f"limit must be between 1 and {_MAX_HISTORY}")
        return limit

    @staticmethod
    def _parse_since(value: str | None) -> datetime | None:
        if value is None:
            return None
        text = value.strip().lower()
        now = datetime.now(timezone.utc)
        try:
            if text.endswith("h"):
                return now - timedelta(hours=int(text[:-1]))
            if text.endswith("d"):
                return now - timedelta(days=int(text[:-1]))
            parsed = datetime.fromisoformat(text.replace("z", "+00:00"))
            return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)
        except (ValueError, OverflowError) as exc:
            raise ValueError("since must be an ISO timestamp or duration such as 24h or 7d") from exc
