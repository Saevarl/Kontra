# src/kontra/scout/profiler.py
"""
ScoutProfiler - Contract-free data profiling with pluggable backends.

Supports:
- Parquet and CSV files (local + S3) via DuckDB backend
- PostgreSQL tables via PostgreSQL backend

Efficiency optimizations:
- Parquet metadata extraction (schema, row count) without data scan
- PostgreSQL pg_stats for lite preset
- Single-pass aggregation queries
- Smart sampling for expensive operations
- Preset modes for different profiling depths
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.version import VERSION

from kontra.logging import get_logger

_logger = get_logger(__name__)

from .types import (
    ColumnProfile,
    DatasetProfile,
    NumericStats,
    StringStats,
    TemporalStats,
    TopValue,
    enforce_profile_invariants,
)
from .dtype_mapping import normalize_dtype


# Preset configurations
# New names (v0.7+): scout, scan, interrogate
# Old names (deprecated): lite, standard, deep
PRESETS = {
    # --- New preset names ---
    "scout": {
        # Quick recon: schema + row count + basic null/distinct only
        # Uses metadata-only path when available (pg_stats, Parquet footer)
        "include_numeric_stats": False,
        "include_string_stats": False,
        "include_temporal_stats": False,
        "include_top_values": False,
        "include_percentiles": False,
        "top_n": 0,
        "list_values_threshold": 5,
        "metadata_only": True,  # Use metadata-only path when backend supports it
    },
    "scan": {
        # Systematic pass: full stats, moderate top values
        # Uses strategic profiling when backend supports it (PostgreSQL)
        "include_numeric_stats": True,
        "include_string_stats": True,
        "include_temporal_stats": True,
        "include_top_values": True,
        "include_percentiles": False,
        "top_n": 5,
        "list_values_threshold": 10,
        "metadata_only": False,
        "strategic_standard": True,  # Use smart probing when available
    },
    "interrogate": {
        # Deep investigation: everything including percentiles
        "include_numeric_stats": True,
        "include_string_stats": True,
        "include_temporal_stats": True,
        "include_top_values": True,
        "include_percentiles": True,
        "top_n": 10,
        "list_values_threshold": 20,
        "metadata_only": False,
    },
    # --- Deprecated aliases (for backward compatibility) ---
    "lite": {
        # DEPRECATED: Use "scout" instead
        "include_numeric_stats": False,
        "include_string_stats": False,
        "include_temporal_stats": False,
        "include_top_values": False,
        "include_percentiles": False,
        "top_n": 0,
        "list_values_threshold": 5,
        "metadata_only": True,
    },
    "standard": {
        # DEPRECATED: Use "scan" instead
        "include_numeric_stats": True,
        "include_string_stats": True,
        "include_temporal_stats": True,
        "include_top_values": True,
        "include_percentiles": False,
        "top_n": 5,
        "list_values_threshold": 10,
        "metadata_only": False,
        "strategic_standard": True,
    },
    "deep": {
        # DEPRECATED: Use "interrogate" instead
        "include_numeric_stats": True,
        "include_string_stats": True,
        "include_temporal_stats": True,
        "include_top_values": True,
        "include_percentiles": True,
        "top_n": 10,
        "list_values_threshold": 20,
        "metadata_only": False,
    },
}

# Mapping from old preset names to new names (for deprecation warnings)
_DEPRECATED_PRESETS = {
    "lite": "scout",
    "standard": "scan",
    "deep": "interrogate",
    "llm": "scan",  # llm preset is removed, recommend scan + to_llm()
}


def _select_backend(handle: DatasetHandle, sample_size: Optional[int] = None):
    """
    Select the appropriate backend for the data source.

    Returns an instance of ProfilerBackend.
    """
    scheme = (handle.scheme or "").lower()

    if scheme in ("postgres", "postgresql"):
        from .backends.postgres_backend import PostgreSQLBackend
        return PostgreSQLBackend(handle, sample_size=sample_size)

    if scheme in ("clickhouse", "clickhouses"):
        from .backends.clickhouse_backend import ClickHouseBackend
        return ClickHouseBackend(handle, sample_size=sample_size)

    if scheme in ("mssql", "sqlserver"):
        from .backends.sqlserver_backend import SqlServerBackend
        return SqlServerBackend(handle, sample_size=sample_size)

    # Default to DuckDB for files (parquet, csv, etc.)
    from .backends.duckdb_backend import DuckDBBackend
    return DuckDBBackend(handle, sample_size=sample_size)


def _is_numeric(dtype: str) -> bool:
    return dtype in ("int", "float")


def _is_string(dtype: str) -> bool:
    return dtype == "string"


def _is_temporal(dtype: str) -> bool:
    return dtype in ("date", "datetime", "time")


def _is_identifier_name(name: str) -> bool:
    """Heuristic: does this column name look like an identifier / key?

    Used to decide when an *estimated* distinct count is worth replacing with
    an exact COUNT(DISTINCT) (see ScoutProfiler._refine_identifier_distinct_counts).
    Estimated distinct counts on identifier columns are the ones that most
    mislead consumers about uniqueness, so we target them specifically.
    """
    raw = name.strip()
    n = raw.lower()
    if not n:
        return False
    if n in ("id", "uuid", "guid", "pk", "rowid"):
        return True
    if n.endswith(("_id", "_uuid", "_guid", "_pk", "_key")):
        # e.g. user_id, order_uuid, tenant_key
        return True
    if "uuid" in n or "guid" in n:
        return True
    # camelCase / caps identifiers: "customerId", "orderID". Case is the signal
    # here, which avoids matching English words ending in "id" (valid, paid...).
    if len(raw) > 2 and raw.endswith(("Id", "ID")):
        return True
    return False


class ScoutProfiler:
    """
    Contract-free data profiler with pluggable backends.

    Supports:
    - Parquet and CSV files (local + S3) via DuckDB backend
    - PostgreSQL tables via PostgreSQL backend

    Efficiency features:
    - Parquet metadata extraction (row count, schema) without data scan
    - PostgreSQL pg_stats for lite preset
    - Single-pass aggregation queries
    - Preset modes (lite/standard/deep) for different use cases
    - Smart sampling for large datasets

    Usage:
        # Quick overview
        profiler = ScoutProfiler("data.parquet", preset="lite")

        # Full analysis
        profiler = ScoutProfiler("data.parquet", preset="deep", include_patterns=True)

        # PostgreSQL table
        profiler = ScoutProfiler("postgres://user:pass@host/db/public.users")

        profile = profiler.profile()
        print(profile.to_dict())
    """

    # Tables at or below this row count are cheap enough to run an exact
    # COUNT(DISTINCT) on an identifier column when the stats-based estimate is
    # untrustworthy (see _refine_identifier_distinct_counts).
    _EXACT_DISTINCT_ROW_THRESHOLD = 1_000_000

    def __init__(
        self,
        source_uri: str,
        *,
        preset: Literal["lite", "standard", "deep"] = "standard",
        list_values_threshold: Optional[int] = None,
        top_n: Optional[int] = None,
        sample_size: Optional[int] = None,
        include_patterns: bool = False,
        percentiles: Optional[List[int]] = None,
        columns: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the profiler.

        Args:
            source_uri: Path or URI to the dataset (local, s3://, postgres://)
            preset: Profiling depth preset ("lite", "standard", "deep")
            list_values_threshold: List all values if distinct count <= this (overrides preset)
            top_n: Number of top frequent values to include (overrides preset)
            sample_size: If set, sample this many rows for profiling
            include_patterns: Whether to detect patterns (email, uuid, etc.)
            percentiles: List of percentiles to compute (overrides preset)
            columns: Specific columns to profile (default: all)
            storage_options: Cloud storage credentials (S3, Azure, GCS).
                For S3/MinIO: aws_access_key_id, aws_secret_access_key, aws_region, endpoint_url
                For Azure: account_name, account_key, sas_token, etc.
                These override environment variables when provided.
        """
        self.source_uri = source_uri
        self.handle = DatasetHandle.from_uri(source_uri, storage_options=storage_options)
        self.sample_size = sample_size
        self.include_patterns = include_patterns
        self.columns_filter = columns

        # Apply preset, then override with explicit args
        if preset not in PRESETS:
            valid_presets = ["scout", "scan", "interrogate"]
            raise ValueError(
                f"Invalid preset '{preset}'. Valid presets: {', '.join(valid_presets)}"
            )
        self.preset_name = preset  # Store for output
        preset_config = PRESETS[preset]
        self.list_values_threshold = (
            list_values_threshold
            if list_values_threshold is not None
            else preset_config["list_values_threshold"]
        )
        self.top_n = top_n if top_n is not None else preset_config["top_n"]
        self.include_numeric_stats = preset_config["include_numeric_stats"]
        self.include_string_stats = preset_config["include_string_stats"]
        self.include_temporal_stats = preset_config["include_temporal_stats"]
        self.include_top_values = preset_config["include_top_values"]
        self.include_percentiles = preset_config["include_percentiles"]

        # Percentiles (only used if include_percentiles is True)
        self.percentiles = percentiles or [25, 50, 75, 99]

        # Metadata-only mode (for lite preset)
        self.metadata_only = preset_config.get("metadata_only", False)

        # Strategic standard mode (for standard preset on PostgreSQL)
        self.strategic_standard = preset_config.get("strategic_standard", False)

        # Backend is created on profile() call
        self.backend = None

    def profile(self) -> DatasetProfile:
        """Execute profiling and return structured results."""
        t0 = time.perf_counter()

        # Create backend
        self.backend = _select_backend(self.handle, sample_size=self.sample_size)

        try:
            # Connect to data source
            self.backend.connect()

            # 1. Get schema (column names and types)
            schema = self.backend.get_schema()

            # Filter columns if specified
            if self.columns_filter:
                schema = [(n, t) for n, t in schema if n in self.columns_filter]

            # 2. Get row count (backend handles optimization)
            row_count = self.backend.get_row_count()

            # Track effective row count + provenance. Column-profiling paths may
            # replace an estimate with a same-moment exact COUNT(*) when they
            # scan the table anyway (see _profile_columns and the metadata paths).
            self._effective_row_count = row_count
            self._row_count_estimated = bool(
                getattr(self.backend, "row_count_estimated", False)
            )

            # 3. Get estimated size (if available)
            estimated_size = self.backend.get_estimated_size_bytes()

            # 4. Check for stale statistics (PostgreSQL only)
            warnings: List[str] = []
            if hasattr(self.backend, "check_stats_staleness"):
                staleness_warning = self.backend.check_stats_staleness()
                if staleness_warning:
                    warnings.append(staleness_warning)
                    _logger.warning(staleness_warning)

            # 5. Profile each column (single-pass aggregation)
            column_profiles = self._profile_columns(schema, row_count)

            # 5a. Identifier columns with ESTIMATED distinct counts mislead
            # consumers about uniqueness (fake duplicates). On small tables,
            # replace the estimate with an exact COUNT(DISTINCT) so the
            # uniqueness ratio is trustworthy. May adopt a same-moment exact
            # COUNT(*) as the row count too.
            self._refine_identifier_distinct_counts(
                column_profiles, self._effective_row_count
            )

            # A profiling path may have upgraded the estimate to an exact,
            # same-moment count. Use the effective values from here on.
            row_count = self._effective_row_count

            # 5b. Constant columns (distinct_count == 1) must surface their
            # single value; several value-fetching paths can skip them.
            self._ensure_constant_values_surfaced(column_profiles, row_count)

            # 6. Optionally detect patterns (sampling-based, efficient)
            if self.include_patterns:
                self._detect_patterns(column_profiles)

            # 7. Infer semantic types
            self._infer_semantic_types(column_profiles)

            duration_ms = int((time.perf_counter() - t0) * 1000)

            profile = DatasetProfile(
                source_uri=self.source_uri,
                source_format=self.backend.source_format,
                profiled_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                engine_version=VERSION,
                preset=self.preset_name,
                row_count=row_count,
                row_count_estimated=self._row_count_estimated,
                column_count=len(column_profiles),
                estimated_size_bytes=estimated_size,
                sampled=self.sample_size is not None,
                sample_size=self.sample_size,
                columns=column_profiles,
                warnings=warnings,
                profile_duration_ms=duration_ms,
            )

            # 8. Consistency guard: clamp estimated metrics that violate bounds
            # (null_count/distinct_count <= row_count) so the profile never
            # reports impossible facts. Exact-vs-exact conflicts are surfaced
            # as warnings instead of being silently hidden.
            inv_warnings = enforce_profile_invariants(profile)
            for w in inv_warnings:
                _logger.warning(w)
            profile.warnings.extend(inv_warnings)

            return profile
        finally:
            if self.backend:
                self.backend.close()

    def _profile_columns(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> List[ColumnProfile]:
        """Build single compound query for all column statistics."""
        if not schema:
            return []

        # Check if we can use metadata-only path (faster, no table scan)
        use_metadata_only = (
            self.metadata_only
            and hasattr(self.backend, "supports_metadata_only")
            and self.backend.supports_metadata_only()
        )

        if use_metadata_only:
            return self._profile_columns_from_metadata(schema, row_count)

        # Check if we can use strategic standard path (PostgreSQL optimization)
        use_strategic_standard = (
            self.strategic_standard
            and hasattr(self.backend, "supports_strategic_standard")
            and self.backend.supports_strategic_standard()
        )

        if use_strategic_standard:
            return self._profile_columns_strategic(schema, row_count)

        # Build aggregation expressions for each column
        exprs: List[str] = []
        col_info: List[Tuple[str, str, str]] = []  # (name, raw_type, normalized_type)

        # Same-moment exact row count: this path scans the table for exact
        # aggregates (null/distinct counts), so compute COUNT(*) in the SAME
        # batch and use it as the authoritative row count. This prevents pairing
        # exact aggregates with a stale partition-stats/reltuples estimate.
        # When sampling, COUNT(*) would return the sample size, not the table
        # size, so skip the override and mark aggregates as sample estimates.
        count_alias = "__total_rows__"
        if self.sample_size is None:
            exprs.append(f"COUNT(*) AS {self.backend.esc_ident(count_alias)}")

        for col_name, raw_type in schema:
            dtype = normalize_dtype(raw_type)
            col_info.append((col_name, raw_type, dtype))
            col_exprs = self._build_column_agg_exprs(col_name, dtype)
            exprs.extend(col_exprs)

        # Execute single aggregate query via backend
        results = self.backend.execute_stats_query(exprs)

        if self.sample_size is None:
            scanned = results.get(count_alias)
            if scanned is not None:
                row_count = int(scanned)
                self._effective_row_count = row_count
                self._row_count_estimated = False

        # Build ColumnProfile objects
        profiles: List[ColumnProfile] = []
        for col_name, raw_type, dtype in col_info:
            profile = self._build_column_profile(
                col_name, raw_type, dtype, results, row_count
            )
            # Aggregates over a sample are estimates of the full table.
            if self.sample_size is not None:
                profile.null_count_estimated = True
                profile.distinct_count_estimated = True
            profiles.append(profile)

        # Fetch top values and low-cardinality values
        for profile in profiles:
            self._fetch_top_values(profile, row_count)
            if profile.distinct_count <= self.list_values_threshold:
                self._fetch_all_values(profile)

        return profiles

    def _profile_columns_from_metadata(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> List[ColumnProfile]:
        """
        Profile columns using metadata only (no table scan).

        Used for 'lite' preset when backend supports it (PostgreSQL pg_stats, Parquet footer).
        Returns estimates, not exact counts.
        """
        # Get metadata from backend
        metadata = self.backend.profile_metadata_only(schema, row_count)

        # If the backend captured a same-moment exact COUNT(*) while scanning
        # (e.g. SQL Server's exact null/distinct path), adopt it as the row count
        # so estimates aren't paired with a stale partition estimate.
        row_count = self._adopt_exact_row_count(metadata, row_count)

        profiles: List[ColumnProfile] = []
        for col_name, raw_type in schema:
            dtype = normalize_dtype(raw_type)
            col_meta = metadata.get(col_name, {})

            null_count = col_meta.get("null_count", 0)
            non_null_count = row_count - null_count

            null_est = col_meta.get(
                "null_count_estimated", col_meta.get("is_estimate", False)
            )
            distinct_est = col_meta.get(
                "distinct_count_estimated", col_meta.get("is_estimate", False)
            )

            # Handle distinct_count estimates:
            # - Parquet upper-bound estimates (is_upper_bound=True) are unreliable
            # - pg_stats estimates are reliable even if they equal non_null_count
            distinct_count = col_meta.get("distinct_count", 0)
            if col_meta.get("is_upper_bound", False):
                # Discard unreliable upper-bound estimates (e.g., Parquet without stats)
                distinct_count = 0
                distinct_est = False

            null_rate = null_count / row_count if row_count > 0 else 0.0
            uniqueness_ratio = (
                distinct_count / non_null_count if non_null_count > 0 else 0.0
            )

            profile = ColumnProfile(
                name=col_name,
                dtype=dtype,
                dtype_raw=raw_type,
                row_count=row_count,
                null_count=null_count,
                null_rate=null_rate,
                distinct_count=distinct_count,
                uniqueness_ratio=uniqueness_ratio,
                null_count_estimated=null_est,
                distinct_count_estimated=distinct_est if distinct_count > 0 else False,
                # Only classify as low cardinality if we have meaningful distinct count
                is_low_cardinality=distinct_count > 0 and distinct_count <= self.list_values_threshold,
            )

            # Use most_common_vals from pg_stats for low-cardinality columns
            mcv = col_meta.get("most_common_vals")
            if mcv and profile.is_low_cardinality:
                profile.values = mcv

            profiles.append(profile)

        return profiles

    def _profile_columns_strategic(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> List[ColumnProfile]:
        """
        Profile columns using strategic queries (PostgreSQL optimization).

        This method optimizes standard preset for PostgreSQL by:
        1. Using metadata (pg_stats) for null/distinct counts
        2. Classifying columns by cardinality to choose optimal strategy
        3. Using TABLESAMPLE SYSTEM (not BERNOULLI) for numeric stats
        4. Batching low-cardinality GROUP BY queries
        5. Trusting pg_stats MCVs for high-cardinality columns

        Much faster than full table scan approach.
        """
        # Step 1: Get freshness info
        freshness = self.backend.get_table_freshness()
        is_fresh = freshness.get("is_fresh", False)

        stale_ratio = freshness.get("stale_ratio", 1.0)
        _logger.info(
            f"PostgreSQL stats freshness: stale_ratio={stale_ratio:.2f}, is_fresh={is_fresh}"
        )

        # Step 2: Get metadata (null/distinct) and classify columns
        metadata = self.backend.profile_metadata_only(schema, row_count)
        classification = self.backend.classify_columns(schema, row_count)

        # Adopt a same-moment exact COUNT(*) if the backend captured one while
        # scanning for exact null/distinct counts (avoids pairing exact
        # aggregates with a stale partition/reltuples estimate).
        row_count = self._adopt_exact_row_count(metadata, row_count)

        # Step 3: Build profile objects with metadata
        profiles: List[ColumnProfile] = []
        numeric_cols = []
        low_cardinality_cols = []

        for col_name, raw_type in schema:
            dtype = normalize_dtype(raw_type)
            col_meta = metadata.get(col_name, {})
            col_class = classification.get(col_name, {})

            null_count = col_meta.get("null_count", 0)
            distinct_count = col_meta.get("distinct_count", 0)

            null_est = col_meta.get(
                "null_count_estimated", col_meta.get("is_estimate", False)
            )
            distinct_est = col_meta.get(
                "distinct_count_estimated", col_meta.get("is_estimate", False)
            )

            non_null_count = row_count - null_count
            null_rate = null_count / row_count if row_count > 0 else 0.0
            uniqueness_ratio = (
                distinct_count / non_null_count if non_null_count > 0 else 0.0
            )

            profile = ColumnProfile(
                name=col_name,
                dtype=dtype,
                dtype_raw=raw_type,
                row_count=row_count,
                null_count=null_count,
                null_rate=null_rate,
                distinct_count=distinct_count,
                uniqueness_ratio=uniqueness_ratio,
                null_count_estimated=null_est,
                distinct_count_estimated=distinct_est,
                is_low_cardinality=distinct_count <= self.list_values_threshold,
            )

            # Track columns needing additional queries
            if _is_numeric(dtype) and self.include_numeric_stats:
                numeric_cols.append((col_name, profile))

            if col_class.get("strategy") == "group_by":
                low_cardinality_cols.append(col_name)
            elif col_class.get("strategy") == "metadata_only":
                # Use MCVs from pg_stats for top_values
                mcv = col_meta.get("most_common_vals")
                if mcv and self.include_top_values:
                    profile.top_values = [
                        TopValue(value=v, count=0, pct=0.0)
                        for v in mcv[:self.top_n]
                    ]
                    if profile.is_low_cardinality:
                        profile.values = mcv

            profiles.append(profile)

        # Step 4: Numeric stats via TABLESAMPLE SYSTEM (fast block sampling)
        if numeric_cols:
            numeric_exprs = []
            # SQL Server uses STDEV, PostgreSQL/DuckDB use STDDEV
            is_duckdb = self.backend.source_format in ("parquet", "csv", "duckdb")
            stddev_fn = "STDEV" if self.backend.source_format == "sqlserver" else "STDDEV"
            for col_name, _ in numeric_cols:
                c = self.backend.esc_ident(col_name)
                # DuckDB: Filter out infinity values to prevent overflow errors
                if is_duckdb:
                    finite_col = f"CASE WHEN ISFINITE({c}) THEN {c} END"
                    numeric_exprs.extend([
                        f"MIN({finite_col}) AS {self.backend.esc_ident(f'__min__{col_name}')}",
                        f"MAX({finite_col}) AS {self.backend.esc_ident(f'__max__{col_name}')}",
                        f"AVG({finite_col}) AS {self.backend.esc_ident(f'__mean__{col_name}')}",
                        f"{stddev_fn}({finite_col}) AS {self.backend.esc_ident(f'__std__{col_name}')}",
                    ])
                else:
                    numeric_exprs.extend([
                        f"MIN({c}) AS {self.backend.esc_ident(f'__min__{col_name}')}",
                        f"MAX({c}) AS {self.backend.esc_ident(f'__max__{col_name}')}",
                        f"AVG({c}) AS {self.backend.esc_ident(f'__mean__{col_name}')}",
                        f"{stddev_fn}({c}) AS {self.backend.esc_ident(f'__std__{col_name}')}",
                    ])

            # Use SYSTEM sampling (block-level) - much faster than BERNOULLI
            # If stats are fresh, use smaller sample; if stale, use larger sample
            sample_pct = 1.0 if is_fresh else 5.0
            numeric_results = self.backend.execute_sampled_stats_query(
                numeric_exprs, sample_pct=sample_pct
            )

            # Populate numeric stats
            for col_name, profile in numeric_cols:
                profile.numeric = NumericStats(
                    min=self._to_float(numeric_results.get(f"__min__{col_name}")),
                    max=self._to_float(numeric_results.get(f"__max__{col_name}")),
                    mean=self._to_float(numeric_results.get(f"__mean__{col_name}")),
                    std=self._to_float(numeric_results.get(f"__std__{col_name}")),
                    median=None,  # Skip median in strategic mode (expensive)
                    percentiles={},
                )

        # Step 5: Low-cardinality columns via batched GROUP BY
        if low_cardinality_cols and self.include_top_values:
            low_card_values = self.backend.fetch_low_cardinality_values_batched(
                low_cardinality_cols
            )

            # Populate values and top_values
            for profile in profiles:
                if profile.name in low_card_values:
                    values_with_counts = low_card_values[profile.name]
                    profile.values = [v for v, _ in values_with_counts]
                    profile.top_values = [
                        TopValue(
                            value=v,
                            count=c,
                            pct=(c / row_count * 100) if row_count > 0 else 0.0,
                        )
                        for v, c in values_with_counts[:self.top_n]
                    ]

        # Step 6: Medium cardinality - sample top values
        medium_card_cols = [
            p.name for p in profiles
            if classification.get(p.name, {}).get("strategy") == "sample"
            and p.top_values is None
        ]

        if medium_card_cols and self.include_top_values:
            for col_name in medium_card_cols:
                profile = next(p for p in profiles if p.name == col_name)
                try:
                    rows = self.backend.fetch_top_values(col_name, self.top_n)
                    profile.top_values = [
                        TopValue(
                            value=val,
                            count=int(cnt),
                            pct=(int(cnt) / row_count * 100) if row_count > 0 else 0.0,
                        )
                        for val, cnt in rows
                    ]
                except (ValueError, TypeError, OSError) as e:
                    _logger.debug(f"Could not fetch top values for {col_name}: {e}")

        return profiles

    def _adopt_exact_row_count(
        self, metadata: Dict[str, Dict[str, Any]], row_count: int
    ) -> int:
        """
        If a backend captured a same-moment exact COUNT(*) in its metadata scan,
        adopt it as the effective row count and clear the estimate flag.

        Returns the row count to use (exact if captured, else the original).
        """
        for col_meta in metadata.values():
            exact = col_meta.get("exact_row_count")
            if exact is not None:
                self._effective_row_count = int(exact)
                self._row_count_estimated = False
                return int(exact)
        return row_count

    def _refine_identifier_distinct_counts(
        self, profiles: List[ColumnProfile], row_count: int
    ) -> None:
        """Replace ESTIMATED distinct counts on identifier-like columns with an
        exact COUNT(DISTINCT), for small tables.

        Stats-based distinct estimates (pg_stats n_distinct, SQL Server
        histograms) are frequently wrong on identifier columns: even a truly
        unique column can report a distinct count below the row count, yielding
        a uniqueness_ratio < 1.0 and implying phantom duplicates. For any table
        at/under the threshold, an exact COUNT(DISTINCT) is cheap and makes the
        ratio trustworthy. Exact paths (parquet scan, full-scan aggregates) are
        untouched because they never set distinct_count_estimated.

        Backend-agnostic: uses only execute_stats_query/esc_ident, which every
        backend implements.
        """
        # Metadata-only presets (scout) are contractually scan-free: a COUNT
        # (DISTINCT) is a full scan, so leave the estimate in place (flagged).
        # The estimated marker is faithfully surfaced by to_llm()/reporters.
        if self.metadata_only:
            return
        # Never override sample-based estimates: a sample can't yield an exact
        # full-table distinct count, and execute_stats_query would sample.
        if self.sample_size is not None:
            return
        if row_count <= 0 or row_count > self._EXACT_DISTINCT_ROW_THRESHOLD:
            return

        targets = [
            p
            for p in profiles
            if p.distinct_count_estimated and _is_identifier_name(p.name)
        ]
        if not targets:
            return

        esc = self.backend.esc_ident
        exprs: List[str] = []
        for p in targets:
            c = esc(p.name)
            exprs.append(f"COUNT(DISTINCT {c}) AS {esc(f'__exd__{p.name}')}")
            exprs.append(f"COUNT(*) - COUNT({c}) AS {esc(f'__exn__{p.name}')}")
        exprs.append(f"COUNT(*) AS {esc('__exrows__')}")

        try:
            res = self.backend.execute_stats_query(exprs)
        except (ValueError, TypeError, OSError) as e:
            _logger.debug(f"Exact distinct fallback failed: {e}")
            return

        exact_rows = res.get("__exrows__")
        if exact_rows is None:
            return
        exact_rows = int(exact_rows)
        if exact_rows <= 0:
            return

        # A same-moment exact COUNT(*) is strictly better than a stale estimate.
        if self._row_count_estimated:
            self._effective_row_count = exact_rows
            self._row_count_estimated = False

        for p in targets:
            exd = res.get(f"__exd__{p.name}")
            if exd is None:
                continue
            exd = int(exd)
            exn_raw = res.get(f"__exn__{p.name}")
            exn = int(exn_raw) if exn_raw is not None else p.null_count
            non_null = exact_rows - exn

            p.row_count = exact_rows
            p.distinct_count = exd
            p.distinct_count_estimated = False
            p.null_count = exn
            p.null_count_estimated = False
            p.null_rate = exn / exact_rows if exact_rows > 0 else 0.0
            p.uniqueness_ratio = exd / non_null if non_null > 0 else 0.0
            p.is_low_cardinality = exd <= self.list_values_threshold

    def _ensure_constant_values_surfaced(
        self, profiles: List[ColumnProfile], row_count: int
    ) -> None:
        """Backfill the single value for constant columns (distinct_count == 1).

        A distinct=1 column is trivially low-cardinality, but it can slip
        through the value-fetching paths and end up with empty top_values/values:
        pg_stats may omit most_common_vals for an all-same column, the
        metadata-only (scout) path fills `values` but never `top_values`, and
        cardinality classification can route it to a strategy that skips the
        GROUP BY. This backfills the single value on every backend.

        Cost discipline: when the value is already known (top_values or values
        populated) it is surfaced for FREE — a constant column's count is just
        its non-null row count. A table scan (fetch_top_values) is only issued
        for value-scanning presets (scan/interrogate), so the scan-free scout
        preset stays scan-free.
        """
        for profile in profiles:
            if profile.distinct_count != 1:
                continue

            # distinct == 1 is unambiguously low cardinality.
            profile.is_low_cardinality = True

            # (1) Already have top_values: just mirror into `values` if missing.
            if profile.top_values:
                if not profile.values:
                    profile.values = [tv.value for tv in profile.top_values[:1]]
                continue

            # A constant column's single value appears in every non-null row.
            non_null = row_count - min(profile.null_count, row_count)

            # (2) Value already known (e.g. pg_stats MCV populated `values`):
            # synthesize top_values for free, no query.
            if profile.values:
                v = profile.values[0]
                profile.top_values = [
                    TopValue(
                        value=v,
                        count=non_null,
                        pct=(non_null / row_count * 100) if row_count > 0 else 0.0,
                    )
                ]
                continue

            # (3) Value genuinely unknown (e.g. SQL Server has no MCV metadata).
            # A single targeted GROUP BY surfaces it. The column is constant, so
            # this is one output group; it runs in every preset because a
            # constant's value is small, high-signal metadata worth the bounded
            # cost. Reached only for the rare constant column whose value the
            # metadata path did not already provide.
            try:
                rows = self.backend.fetch_top_values(profile.name, 1)
            except (ValueError, TypeError, OSError) as e:
                _logger.debug(
                    f"Could not fetch constant value for {profile.name}: {e}"
                )
                continue
            if not rows:
                continue
            val, cnt = rows[0]
            cnt = int(cnt)
            profile.top_values = [
                TopValue(
                    value=val,
                    count=cnt,
                    pct=(cnt / row_count * 100) if row_count > 0 else 0.0,
                )
            ]
            profile.values = [val]

    def _build_column_agg_exprs(self, col: str, dtype: str) -> List[str]:
        """Generate SQL expressions for a single column's statistics."""
        esc = self.backend.esc_ident
        c = esc(col)
        source_fmt = getattr(self.backend, "source_format", "")
        is_sqlserver = source_fmt == "sqlserver"
        is_duckdb = source_fmt in ("parquet", "csv", "duckdb")

        # Core stats: always included (null count, distinct count)
        exprs = [
            f"COUNT(*) - COUNT({c}) AS {esc(f'__null__{col}')}",
            f"COUNT(DISTINCT {c}) AS {esc(f'__distinct__{col}')}",
        ]

        # Numeric stats: controlled by preset
        if _is_numeric(dtype) and self.include_numeric_stats:
            # SQL Server: Cast to FLOAT to prevent overflow on large tables
            avg_expr = f"AVG(CAST({c} AS FLOAT))" if is_sqlserver else f"AVG({c})"
            # DuckDB: Filter out infinity values to prevent overflow errors
            if is_duckdb:
                finite_col = f"CASE WHEN ISFINITE({c}) THEN {c} END"
                exprs.extend([
                    f"MIN({finite_col}) AS {esc(f'__min__{col}')}",
                    f"MAX({finite_col}) AS {esc(f'__max__{col}')}",
                    f"AVG({finite_col}) AS {esc(f'__mean__{col}')}",
                ])
            else:
                exprs.extend([
                    f"MIN({c}) AS {esc(f'__min__{col}')}",
                    f"MAX({c}) AS {esc(f'__max__{col}')}",
                    f"{avg_expr} AS {esc(f'__mean__{col}')}",
                ])
            # SQL Server requires different PERCENTILE_CONT syntax (window function)
            # Skip median/percentiles for SQL Server - use STDEV instead of STDDEV
            if is_sqlserver:
                exprs.append(f"STDEV({c}) AS {esc(f'__std__{col}')}")
            elif is_duckdb:
                finite_col = f"CASE WHEN ISFINITE({c}) THEN {c} END"
                exprs.extend([
                    f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {finite_col}) AS {esc(f'__median__{col}')}",
                    f"STDDEV({finite_col}) AS {esc(f'__std__{col}')}",
                ])
                # Additional percentiles for DuckDB: expensive, only in interrogate preset
                if self.include_percentiles:
                    for p in self.percentiles:
                        if p != 50:  # 50th is already the median
                            exprs.append(
                                f"PERCENTILE_CONT({p / 100}) WITHIN GROUP (ORDER BY {finite_col}) "
                                f"AS {esc(f'__p{p}__{col}')}"
                            )
            else:
                exprs.extend([
                    f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {c}) AS {esc(f'__median__{col}')}",
                    f"STDDEV({c}) AS {esc(f'__std__{col}')}",
                ])
                # Additional percentiles: expensive, only in interrogate preset
                if self.include_percentiles:
                    for p in self.percentiles:
                        if p != 50:  # 50th is already the median
                            exprs.append(
                                f"PERCENTILE_CONT({p / 100}) WITHIN GROUP (ORDER BY {c}) "
                                f"AS {esc(f'__p{p}__{col}')}"
                            )

        # String stats: controlled by preset
        if _is_string(dtype) and self.include_string_stats:
            # SQL Server uses LEN(), others use LENGTH()
            len_fn = "LEN" if is_sqlserver else "LENGTH"
            # SQL Server needs BIGINT cast to prevent overflow on large tables
            sum_cast = "CAST(1 AS BIGINT)" if is_sqlserver else "1"
            exprs.extend([
                f"MIN({len_fn}({c})) AS {esc(f'__minlen__{col}')}",
                f"MAX({len_fn}({c})) AS {esc(f'__maxlen__{col}')}",
                f"AVG(CAST({len_fn}({c}) AS FLOAT)) AS {esc(f'__avglen__{col}')}",
                f"SUM(CASE WHEN {c} = '' THEN {sum_cast} ELSE 0 END) AS {esc(f'__empty__{col}')}",
            ])

        # Temporal stats: controlled by preset
        if _is_temporal(dtype) and self.include_temporal_stats:
            exprs.extend([
                f"MIN({c}) AS {esc(f'__datemin__{col}')}",
                f"MAX({c}) AS {esc(f'__datemax__{col}')}",
            ])

        return exprs

    def _build_column_profile(
        self,
        col_name: str,
        raw_type: str,
        dtype: str,
        results: Dict[str, Any],
        row_count: int,
    ) -> ColumnProfile:
        """Build a ColumnProfile from aggregation results."""
        null_count = int(results.get(f"__null__{col_name}", 0) or 0)
        distinct_count = int(results.get(f"__distinct__{col_name}", 0) or 0)

        non_null_count = row_count - null_count
        null_rate = null_count / row_count if row_count > 0 else 0.0
        uniqueness_ratio = (
            distinct_count / non_null_count if non_null_count > 0 else 0.0
        )

        profile = ColumnProfile(
            name=col_name,
            dtype=dtype,
            dtype_raw=raw_type,
            row_count=row_count,
            null_count=null_count,
            null_rate=null_rate,
            distinct_count=distinct_count,
            uniqueness_ratio=uniqueness_ratio,
            is_low_cardinality=distinct_count <= self.list_values_threshold,
        )

        # Add type-specific stats (only if included by preset)
        if _is_numeric(dtype) and self.include_numeric_stats:
            percentiles = {}
            if self.include_percentiles:
                for p in self.percentiles:
                    val = results.get(f"__p{p}__{col_name}")
                    if val is not None:
                        percentiles[f"p{p}"] = float(val)

            profile.numeric = NumericStats(
                min=self._to_float(results.get(f"__min__{col_name}")),
                max=self._to_float(results.get(f"__max__{col_name}")),
                mean=self._to_float(results.get(f"__mean__{col_name}")),
                median=self._to_float(results.get(f"__median__{col_name}")),
                std=self._to_float(results.get(f"__std__{col_name}")),
                percentiles=percentiles,
            )

        if _is_string(dtype) and self.include_string_stats:
            profile.string = StringStats(
                min_length=self._to_int(results.get(f"__minlen__{col_name}")),
                max_length=self._to_int(results.get(f"__maxlen__{col_name}")),
                avg_length=self._to_float(results.get(f"__avglen__{col_name}")),
                empty_count=self._to_int(results.get(f"__empty__{col_name}")) or 0,
            )

        if _is_temporal(dtype) and self.include_temporal_stats:
            date_min = results.get(f"__datemin__{col_name}")
            date_max = results.get(f"__datemax__{col_name}")
            profile.temporal = TemporalStats(
                date_min=str(date_min) if date_min else None,
                date_max=str(date_max) if date_max else None,
            )

        return profile

    def _fetch_top_values(self, profile: ColumnProfile, row_count: int) -> None:
        """Fetch top N most frequent values for a column."""
        # Skip if top values not requested or top_n is 0
        if not self.include_top_values or self.top_n <= 0:
            return
        if row_count == 0:
            return

        try:
            rows = self.backend.fetch_top_values(profile.name, self.top_n)
            profile.top_values = [
                TopValue(
                    value=val,
                    count=int(cnt),
                    pct=(int(cnt) / row_count * 100) if row_count > 0 else 0.0,
                )
                for val, cnt in rows
            ]
        except (ValueError, TypeError, OSError) as e:
            # Some types may not be groupable
            _logger.debug(f"Could not fetch top values for {profile.name}: {e}")

    def _fetch_all_values(self, profile: ColumnProfile) -> None:
        """Fetch all distinct values for low-cardinality columns."""
        try:
            profile.values = self.backend.fetch_distinct_values(profile.name)
        except (ValueError, TypeError, OSError) as e:
            # Some types may not be sortable
            _logger.debug(f"Could not fetch distinct values for {profile.name}: {e}")

    def _detect_patterns(self, profiles: List[ColumnProfile]) -> None:
        """Detect common patterns in string columns."""
        from .patterns import detect_patterns

        for profile in profiles:
            if profile.dtype != "string" or profile.distinct_count == 0:
                continue

            try:
                sample = self.backend.fetch_sample_values(profile.name, 100)
                sample = [str(v) for v in sample if v is not None]
                if sample:
                    profile.detected_patterns = detect_patterns(sample)
            except (ValueError, TypeError, OSError) as e:
                _logger.debug(f"Could not detect patterns for {profile.name}: {e}")

    def _infer_semantic_types(self, profiles: List[ColumnProfile]) -> None:
        """Infer semantic type for each column based on profile data."""
        for profile in profiles:
            # Primary key / identifier candidate
            if profile.uniqueness_ratio >= 0.99 and profile.null_rate == 0:
                profile.semantic_type = "identifier"
            # Category (low cardinality, non-numeric)
            elif profile.is_low_cardinality and profile.dtype == "string":
                profile.semantic_type = "category"
            # Measure (numeric, non-low-cardinality)
            elif profile.dtype in ("int", "float") and not profile.is_low_cardinality:
                profile.semantic_type = "measure"
            # Timestamp
            elif profile.dtype in ("date", "datetime"):
                profile.semantic_type = "timestamp"
            # Boolean as category
            elif profile.dtype == "bool":
                profile.semantic_type = "category"

    @staticmethod
    def _to_float(val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(val: Any) -> Optional[int]:
        if val is None:
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            return None
