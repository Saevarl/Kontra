"""Secure, protocol-independent service layer for the official Kontra MCP."""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


_MAX_HISTORY = 100
_MAX_SAMPLE_ROWS = 100_000
_MAX_COLUMNS = 100


@dataclass(frozen=True)
class MCPSettings:
    """Immutable server configuration loaded once at startup."""

    postgres_uri: str
    contracts_dir: Path
    config_path: Path | None = None

    def __post_init__(self) -> None:
        """Normalize paths even when settings are constructed programmatically."""
        object.__setattr__(self, "contracts_dir", self.contracts_dir.expanduser().resolve())
        if self.config_path is not None:
            object.__setattr__(self, "config_path", self.config_path.expanduser().resolve())

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
        return cls(
            postgres_uri=uri,
            contracts_dir=contracts.expanduser().resolve(),
            config_path=Path(config).expanduser().resolve() if config else None,
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
                self._profile_store.save(create_profile_state(result))
        return result.to_dict()

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

    def profile_history(self, datasource: str, *, limit: int = 20) -> list[dict[str, Any]]:
        from kontra.config.settings import resolve_datasource
        from kontra.scout.store import fingerprint_source

        with self._store_lock:
            self._require_datasource(datasource)
            states = self._profile_store.get_history(
                fingerprint_source(resolve_datasource(datasource)), limit=self._limit(limit)
            )
        return [state.profile.to_dict() for state in states]

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
