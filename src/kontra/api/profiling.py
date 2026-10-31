"""
Profiling functions: kontra.profile(), draft(), compare_profiles(),
profile_diff() and the saved-profile lookups, plus deprecated aliases.
"""

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from kontra.api.inputs import _normalize_to_dataframe
from kontra.api.results import Suggestions
from kontra.scout.types import DatasetProfile, ProfileDiff

if TYPE_CHECKING:
    import polars as pl


def profile(
    data: Union[str, "pl.DataFrame", List[Dict[str, Any]], Dict[str, Any]],
    preset: str = "scan",
    *,
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    save: bool = False,
    storage_options: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> DatasetProfile:
    """
    Profile a dataset.

    Args:
        data: DataFrame (Polars), list[dict], dict, or path/URI to data file
        preset: Profiling depth:
            - "scout": Quick recon (metadata only)
            - "scan": Systematic pass (full stats) [default]
            - "interrogate": Deep investigation (everything + percentiles)
        columns: Only profile these columns
        sample: Sample N rows (default: all)
        save: Persist the profile to history (default: False). When True, the
            profile is written to the same local store the CLI uses
            (``.kontra/profiles/`` under the current directory), making it
            available to ``kontra.get_profile()``, ``kontra.list_profiles()``,
            and ``kontra.profile_diff()``. Inline DataFrame profiles have no
            stable source identity and are not saved.
        storage_options: Cloud storage credentials (S3, Azure, GCS).
            For S3/MinIO: aws_access_key_id, aws_secret_access_key, aws_region, endpoint_url
            For Azure: account_name, account_key, sas_token, etc.
            These override environment variables when provided.
        **kwargs: Additional arguments passed to ScoutProfiler

    Returns:
        DatasetProfile with column statistics

    Example:
        profile = kontra.profile("data.parquet")
        print(f"Rows: {profile.row_count}")
        for col in profile.columns:
            print(f"{col.name}: {col.dtype}")

        # Quick metadata-only profile
        profile = kontra.profile("big_data.parquet", preset="scout")

        # Deep profile with percentiles
        profile = kontra.profile("data.parquet", preset="interrogate")
    """
    import warnings
    import polars as pl
    from kontra.scout.profiler import ScoutProfiler, _DEPRECATED_PRESETS
    from kontra.errors import InvalidDataError

    # Input validation — match validate() behavior (BUG-059)
    if data is None:
        raise InvalidDataError(
            "NoneType",
            detail="profile() requires data. Pass a file path, DataFrame, or named datasource.",
        )
    if isinstance(data, bool) or (isinstance(data, (int, float)) and not isinstance(data, bool)):
        raise InvalidDataError(
            type(data).__name__,
            detail=f"profile() received {type(data).__name__}, expected file path, DataFrame, or list of dicts.",
        )

    # Warn on deprecated preset names
    if preset in _DEPRECATED_PRESETS:
        new_name = _DEPRECATED_PRESETS[preset]
        warnings.warn(
            f"Preset '{preset}' is deprecated, use '{new_name}' instead",
            DeprecationWarning,
            stacklevel=2,
        )

    def _save_profile_to_history(result: DatasetProfile) -> DatasetProfile:
        """Persist a profile to the local history store if save was requested.

        Mirrors the CLI save path (``create_profile_state`` +
        ``get_default_profile_store().save``) so profiles saved via the API and
        the CLI are interoperable. Inline DataFrame profiles are skipped: they
        have no stable source identity to fingerprint or look up later.
        """
        if not save:
            return result
        if result.source_format == "dataframe" or result.source_uri.startswith("<"):
            return result
        from kontra.scout.store import create_profile_state, get_default_profile_store

        state = create_profile_state(result)
        get_default_profile_store().save(state)
        return result

    # Convert list/dict/pandas to Polars DataFrame
    data = _normalize_to_dataframe(data)

    if isinstance(data, pl.DataFrame):
        # Handle empty DataFrame (no columns) - DuckDB can't read parquet with no columns
        if data.width == 0:
            from datetime import datetime, timezone
            from kontra.version import VERSION
            return DatasetProfile(
                source_uri="<inline DataFrame>",
                source_format="dataframe",
                profiled_at=datetime.now(timezone.utc).isoformat(),
                engine_version=VERSION,
                row_count=data.height,
                column_count=0,
                columns=[],
            )

        # For DataFrame input, write to temp file
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name
            data.write_parquet(temp_path)

        try:
            profiler = ScoutProfiler(
                temp_path,
                preset=preset,
                columns=columns,
                sample_size=sample,
                **kwargs,
            )
            profile = profiler.profile()
            # Replace temp file path with friendly name for DataFrame input
            profile.source_uri = f"<DataFrame: {data.height:,} rows, {data.width} cols>"
            return profile
        finally:
            os.unlink(temp_path)
    else:
        # Resolve named datasources (e.g., "prod_db.users" -> actual URI)
        resolved_data = data
        if isinstance(data, str):
            from kontra.config.settings import resolve_datasource

            try:
                resolved_data = resolve_datasource(data)
            except ValueError:
                # Not a named datasource - use as-is (file path or URI).
                # DatasourceTableError (datasource found, table missing) inherits
                # from KontraError, not ValueError, so it propagates correctly.
                pass

        try:
            profiler = ScoutProfiler(
                resolved_data,
                preset=preset,
                columns=columns,
                sample_size=sample,
                storage_options=storage_options,
                **kwargs,
            )
            result = profiler.profile()
        except FileNotFoundError as e:
            raise InvalidDataError(
                "file",
                detail=f"File not found: {resolved_data}",
            ) from e
        except Exception as e:
            # Only a bare LOCAL PATH gets the friendly "file not found". Any
            # URI source (s3://, az://, http://, postgres://, clickhouse://, …)
            # must surface its REAL error instead — an Azure blob-listing
            # failure, a credential/permission error, a glob-expansion failure,
            # or a DB "relation does not exist" often contains "not found" /
            # "does not exist" in its text, and flattening that to "file not
            # found" hides the actual cause (the reported bug: an az:// glob
            # error misdiagnosed as a missing file).
            src = resolved_data if isinstance(resolved_data, str) else ""
            is_uri = "://" in src
            err_str = str(e).lower()
            is_not_found = (
                "no such file" in err_str
                or "does not exist" in err_str
                or "no files found" in err_str
            )
            if is_not_found and not is_uri:
                raise InvalidDataError(
                    "file",
                    detail=f"File not found: {resolved_data}",
                ) from e
            # Propagate the real error (all URI/engine failures included).
            raise

        # Persist AFTER profiling succeeds and outside the source-resolution
        # try/except, so a history-write failure (read-only cwd, disk full)
        # surfaces its own error instead of being misclassified as a source
        # "file not found".
        return _save_profile_to_history(result)


def draft(
    data: Any,
    min_confidence: float = 0.5,
    *,
    preset: str = "interrogate",
    storage_options: Optional[Dict[str, Any]] = None,
) -> Suggestions:
    """
    Draft validation rules from data or a profile.

    Analyzes the data and suggests rules based on observed patterns.
    These are starting points - refine them based on domain knowledge.

    Args:
        data: DatasetProfile, DataFrame, file path, or any data source.
              If not a DatasetProfile, will profile the data first using
              the specified preset.
        min_confidence: Minimum confidence score (0.0-1.0)
        preset: Profile preset when profiling data ("scout", "scan", "interrogate").
                Defaults to "interrogate" for thorough rule suggestions.
        storage_options: Cloud storage credentials (for S3/Azure paths)

    Returns:
        Suggestions with .to_yaml(), .save(), .filter()

    Example:
        # From file path (profiles internally)
        suggestions = kontra.draft("data/users.parquet")

        # From DataFrame
        suggestions = kontra.draft(df)

        # From profile (explicit)
        profile = kontra.profile(df, preset="interrogate")
        suggestions = kontra.draft(profile)

        # Filter high confidence
        high_conf = suggestions.filter(min_confidence=0.9)

        # Save to file
        high_conf.save("contracts/users.yml")

        # Or use directly
        result = kontra.validate(df, rules=suggestions.to_rules_list())
    """
    # If already a DatasetProfile, use directly
    if isinstance(data, DatasetProfile):
        return Suggestions.from_profile(data, min_confidence=min_confidence)

    # Otherwise, profile the data first
    data_profile = profile(data, preset=preset, storage_options=storage_options)
    return Suggestions.from_profile(data_profile, min_confidence=min_confidence)


def compare_profiles(
    a: Union[str, "pl.DataFrame", List[Dict[str, Any]], Dict[str, Any]],
    b: Union[str, "pl.DataFrame", List[Dict[str, Any]], Dict[str, Any]],
    *,
    preset: str = "scan",
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    a_label: Optional[str] = None,
    b_label: Optional[str] = None,
) -> ProfileDiff:
    """
    Profile two sources and diff them, aligned by column, in one call.

    The two-source "bisect" verb: point it at two pipeline stages (a file, a
    table, a DataFrame, a database URI, a named datasource — any mix) and get a
    structured, column-aligned delta — row-count change, columns added/removed,
    and per-column dtype / null-rate / cardinality / value shifts — without
    holding two full profiles in context. Read it with ``.to_llm()``.

    Args:
        a: The "before" source (any source ``kontra.profile()`` accepts).
        b: The "after" source.
        preset: Profiling preset (scout | scan | interrogate).
        columns: Restrict profiling to these columns on both sides.
        sample: Sample size for profiling (None = full).
        storage_options: Cloud storage credentials.
        a_label / b_label: Display names for the two sides (default: the source
            string, truncated).

    Returns:
        ProfileDiff — ``.has_changes``, ``.has_schema_changes``,
        ``.columns_added/.columns_removed/.columns_changed``, ``.to_llm()``,
        ``.to_dict()``.

    Example:
        diff = kontra.compare_profiles("stage1.parquet", "stage2.parquet")
        print(diff.to_llm())
        if diff.has_schema_changes:
            ...
    """
    prof_a = profile(a, preset=preset, columns=columns, sample=sample,
                     save=False, storage_options=storage_options)
    prof_b = profile(b, preset=preset, columns=columns, sample=sample,
                     save=False, storage_options=storage_options)

    def _label(src: Any, given: Optional[str]) -> str:
        if given:
            return given
        text = src if isinstance(src, str) else type(src).__name__
        return text if len(text) <= 60 else text[:57] + "..."

    from kontra.connectors.db_utils import mask_credentials

    return ProfileDiff.from_profiles(
        prof_a, prof_b,
        before_label=mask_credentials(_label(a, a_label)),
        after_label=mask_credentials(_label(b, b_label)),
    )


def profile_diff(
    source: str,
    *,
    since: Optional[str] = None,
) -> Optional[ProfileDiff]:
    """
    Compare profile runs over time.

    Args:
        source: Data source path or name
        since: Compare to profile from this time ago

    Returns:
        ProfileDiff with .has_changes, .schema_changes, .to_llm()
        Returns None if no history available

    Example:
        diff = kontra.profile_diff("data.parquet", since="7d")
        if diff and diff.has_schema_changes:
            print("Schema changed!")
            for col in diff.columns_added:
                print(f"  NEW: {col}")
    """
    from kontra.scout.store import get_default_profile_store
    from kontra.scout.types import ProfileDiff

    store = get_default_profile_store()
    source_fp = _resolve_profile_source_fingerprint(source)
    history = store.get_history(source_fp, limit=100)

    if len(history) < 1:
        return None

    # Newest first: after is the latest profile.
    after_state = history[0]
    before_state: Optional[Any] = None

    if since is not None:
        from datetime import datetime, timedelta, timezone
        from kontra.rule_defs.builtin.freshness import parse_duration

        delta = parse_duration(since)
        target_str = (datetime.now(timezone.utc) - delta).isoformat()
        for state in history[1:]:
            if state.profiled_at <= target_str:
                before_state = state
                break
        if before_state is None:
            return None
    else:
        # Default: compare the two most recent profiles.
        if len(history) < 2:
            return None
        before_state = history[1]

    return ProfileDiff.compute(before_state, after_state)



def _resolve_profile_source_fingerprint(source: str) -> str:
    """Resolve a data source to the fingerprint under which its profiles are
    stored.

    ``create_profile_state`` fingerprints the profiled ``source_uri`` (the
    resolved URI for named datasources), so named datasources must be resolved
    here for lookups to match what ``profile(save=True)`` and the CLI wrote.
    Plain file paths and URIs are not named datasources and fingerprint as-is.
    """
    from kontra.scout.store import fingerprint_source

    resolved = source
    if isinstance(source, str):
        from kontra.config.settings import resolve_datasource

        try:
            resolved = resolve_datasource(source)
        except ValueError:
            # Not a named datasource — a file path or URI. Fingerprint as-is.
            resolved = source
    return fingerprint_source(resolved)


def list_profiles(source: str) -> List[Dict[str, Any]]:
    """
    List past profile runs for a data source.

    Args:
        source: Data source path or name

    Returns:
        List of profile summaries (newest first), each with ``profiled_at``,
        ``source_uri``, ``row_count``, ``column_count``, and ``engine_version``.
        Empty if no profiles have been saved for the source.
    """
    from kontra.scout.store import get_default_profile_store

    store = get_default_profile_store()
    source_fp = _resolve_profile_source_fingerprint(source)
    history = store.get_history(source_fp, limit=100)

    return [
        {
            "profiled_at": state.profiled_at,
            "source_uri": state.source_uri,
            "row_count": state.profile.row_count,
            "column_count": state.profile.column_count,
            "engine_version": state.engine_version,
        }
        for state in history
    ]


def get_profile(
    source: str,
    run_id: Optional[str] = None,
) -> Optional[DatasetProfile]:
    """
    Get a specific saved profile run.

    Args:
        source: Data source path or name
        run_id: Specific run identifier (the profile's ``profiled_at`` ISO
            timestamp, or a unique prefix of it). Defaults to the latest
            profile.

    Returns:
        DatasetProfile, or None if no matching profile was found.
    """
    from kontra.scout.store import get_default_profile_store

    store = get_default_profile_store()
    source_fp = _resolve_profile_source_fingerprint(source)

    if run_id is None:
        latest = store.get_latest(source_fp)
        return latest.profile if latest else None

    for state in store.get_history(source_fp, limit=100):
        if state.profiled_at == run_id or state.profiled_at.startswith(run_id):
            return state.profile
    return None


# =============================================================================
# Deprecated Aliases (for backward compatibility)
# =============================================================================


def scout(
    data: Union[str, "pl.DataFrame"],
    preset: str = "standard",
    *,
    columns: Optional[List[str]] = None,
    sample: Optional[int] = None,
    save: bool = True,
    **kwargs,
) -> DatasetProfile:
    """
    DEPRECATED: Use kontra.profile() instead.

    Profile a dataset.
    """
    import warnings
    warnings.warn(
        "kontra.scout() is deprecated, use kontra.profile() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return profile(data, preset=preset, columns=columns, sample=sample, save=save, **kwargs)


def suggest_rules(
    data: Union[str, DatasetProfile, "pl.DataFrame"],
    min_confidence: float = 0.5,
) -> Suggestions:
    """
    DEPRECATED: Use kontra.profile() then kontra.draft() instead.

    Generate validation rule suggestions from data or a profile.

    Args:
        data: File path, DataFrame, or DatasetProfile
        min_confidence: Minimum confidence score (0.0-1.0)

    Returns:
        Suggestions with .to_yaml(), .save(), .filter()
    """
    import warnings
    import polars as pl
    warnings.warn(
        "kontra.suggest_rules() is deprecated, use kontra.profile() then kontra.draft() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    # Handle different input types
    if isinstance(data, DatasetProfile):
        prof = data
    elif isinstance(data, (str, pl.DataFrame)):
        prof = profile(data, preset="scan")
    else:
        raise TypeError(
            f"suggest_rules() expects str, DataFrame, or DatasetProfile, got {type(data).__name__}"
        )
    return draft(prof, min_confidence=min_confidence)


def scout_diff(
    source: str,
    *,
    since: Optional[str] = None,
) -> Optional[ProfileDiff]:
    """
    DEPRECATED: Use kontra.profile_diff() instead.

    Compare profile runs over time.
    """
    import warnings
    warnings.warn(
        "kontra.scout_diff() is deprecated, use kontra.profile_diff() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return profile_diff(source, since=since)
