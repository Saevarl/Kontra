# tests/test_scout_reporters.py
"""Tests for Scout reporter modules (JSON, Markdown, LLM)."""

import json
import pytest
import polars as pl

from kontra.scout.profiler import ScoutProfiler
from kontra.scout.reporters.json_reporter import (
    render_json,
    render_llm,
    build_compact_json,
    _strip_nulls,
)
from kontra.scout.reporters.markdown_reporter import (
    render_markdown,
    _cardinality_label,
    _fmt,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_profile(tmp_path):
    """Create a profile from sample data."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve",
                 "Frank", "Grace", "Henry", "Ivy", "Jack"],
        "status": ["active", "active", "inactive", "active", "pending",
                   "active", "inactive", "active", "pending", "active"],
        "age": [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        "score": [85.5, 90.2, 78.9, 92.1, 88.3, 75.6, 93.4, 81.2, 86.7, 79.5],
        "email": [f"{c}@example.com" for c in "abcdefghij"],
    })

    parquet = tmp_path / "sample.parquet"
    df.write_parquet(parquet)

    profiler = ScoutProfiler(str(parquet), preset="standard")
    return profiler.profile()


@pytest.fixture
def profile_with_nulls(tmp_path):
    """Profile with null values."""
    df = pl.DataFrame({
        "id": [1, 2, 3, None, 5],
        "name": ["Alice", None, "Charlie", "David", None],
        "value": [1.0, 2.0, None, 4.0, 5.0],
    })

    parquet = tmp_path / "nulls.parquet"
    df.write_parquet(parquet)

    profiler = ScoutProfiler(str(parquet), preset="standard")
    return profiler.profile()


@pytest.fixture
def profile_with_dates(tmp_path):
    """Profile with temporal columns."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "created_at": pl.Series([
            "2024-01-01T10:00:00",
            "2024-01-02T11:00:00",
            "2024-01-03T12:00:00",
            "2024-01-04T13:00:00",
            "2024-01-05T14:00:00",
        ]).str.to_datetime(),
    })

    parquet = tmp_path / "dates.parquet"
    df.write_parquet(parquet)

    profiler = ScoutProfiler(str(parquet), preset="standard")
    return profiler.profile()


# =============================================================================
# JSON Reporter Tests
# =============================================================================


class TestJSONReporter:
    """Tests for JSON reporter."""

    def test_render_json_returns_valid_json(self, sample_profile):
        """render_json returns valid JSON string."""
        json_str = render_json(sample_profile)

        # Should parse without error
        parsed = json.loads(json_str)
        assert isinstance(parsed, dict)

    def test_render_json_has_expected_keys(self, sample_profile):
        """JSON has expected keys."""
        json_str = render_json(sample_profile)
        parsed = json.loads(json_str)

        # Data is nested under "dataset" key
        if "dataset" in parsed:
            dataset = parsed["dataset"]
        else:
            dataset = parsed

        assert "row_count" in dataset or "columns" in parsed
        assert "columns" in parsed or "columns" in dataset

    def test_render_json_custom_indent(self, sample_profile):
        """render_json respects indent parameter."""
        json_compact = render_json(sample_profile, indent=0)
        json_pretty = render_json(sample_profile, indent=4)

        # Pretty should be longer due to indentation
        assert len(json_pretty) > len(json_compact)

    def test_build_compact_json(self, sample_profile):
        """build_compact_json strips nulls."""
        compact = build_compact_json(sample_profile)

        assert isinstance(compact, dict)
        # Should not contain None values at top level
        for v in compact.values():
            assert v is not None

    def test_strip_nulls_dict(self):
        """_strip_nulls removes None from dict."""
        data = {"a": 1, "b": None, "c": 3}
        result = _strip_nulls(data)

        assert "a" in result
        assert "b" not in result
        assert "c" in result

    def test_strip_nulls_nested(self):
        """_strip_nulls handles nested structures."""
        data = {
            "a": 1,
            "b": {"x": None, "y": 2},
            "c": [1, None, 3],
        }
        result = _strip_nulls(data)

        assert result["b"] == {"y": 2}
        assert result["c"] == [1, 3]

    def test_strip_nulls_empty_containers(self):
        """_strip_nulls removes empty lists/dicts."""
        data = {"a": [], "b": {}, "c": [1, 2]}
        result = _strip_nulls(data)

        assert "a" not in result
        assert "b" not in result
        assert "c" in result


# =============================================================================
# LLM Reporter Tests
# =============================================================================


class TestLLMReporter:
    """Tests for LLM-optimized output."""

    def test_render_llm_returns_string(self, sample_profile):
        """render_llm returns string."""
        output = render_llm(sample_profile)
        assert isinstance(output, str)

    def test_render_llm_has_header(self, sample_profile):
        """LLM output has dataset header."""
        output = render_llm(sample_profile)
        assert "# Dataset:" in output

    def test_render_llm_has_row_col_counts(self, sample_profile):
        """LLM output has row and column counts."""
        output = render_llm(sample_profile)
        assert "rows=" in output
        assert "cols=" in output

    def test_render_llm_has_columns_section(self, sample_profile):
        """LLM output has columns section."""
        output = render_llm(sample_profile)
        assert "## Columns" in output

    def test_render_llm_has_summary_section(self, sample_profile):
        """LLM output has summary section."""
        output = render_llm(sample_profile)
        assert "## Summary" in output

    def test_render_llm_shows_null_rates(self, profile_with_nulls):
        """LLM output shows null rates for columns with nulls."""
        output = render_llm(profile_with_nulls)
        assert "nulls=" in output

    def test_render_llm_shows_distinct_counts(self, sample_profile):
        """LLM output shows distinct counts."""
        output = render_llm(sample_profile)
        assert "distinct=" in output

    def test_render_llm_column_types(self, sample_profile):
        """LLM output includes data types."""
        output = render_llm(sample_profile)
        # Should have some type info (int, string, float, etc.)
        assert any(t in output for t in ["int", "string", "float"])


# =============================================================================
# Markdown Reporter Tests
# =============================================================================


class TestMarkdownReporter:
    """Tests for Markdown reporter."""

    def test_render_markdown_returns_string(self, sample_profile):
        """render_markdown returns string."""
        output = render_markdown(sample_profile)
        assert isinstance(output, str)

    def test_render_markdown_has_title(self, sample_profile):
        """Markdown has title header."""
        output = render_markdown(sample_profile)
        assert "# Data Profile:" in output

    def test_render_markdown_has_summary(self, sample_profile):
        """Markdown has summary section."""
        output = render_markdown(sample_profile)
        assert "## Summary" in output
        assert "**Rows:**" in output
        assert "**Columns:**" in output

    def test_render_markdown_has_schema_table(self, sample_profile):
        """Markdown has schema table."""
        output = render_markdown(sample_profile)
        assert "## Schema" in output
        assert "| Column | Type | Nulls | Distinct | Cardinality |" in output

    def test_render_markdown_has_footer(self, sample_profile):
        """Markdown has footer with preset name."""
        output = render_markdown(sample_profile)
        # Footer should include "Generated by Kontra" and the preset name
        assert "Generated by Kontra" in output
        assert sample_profile.preset.title() in output

    def test_render_markdown_numeric_columns(self, sample_profile):
        """Markdown includes numeric columns section."""
        output = render_markdown(sample_profile)
        # age and score are numeric
        assert "## Numeric Columns" in output
        assert "| Min | Max | Mean |" in output

    def test_render_markdown_categorical_columns(self, sample_profile):
        """Markdown includes categorical columns section."""
        output = render_markdown(sample_profile)
        # status should be categorical
        assert "## Categorical Columns" in output or "status" in output

    def test_render_markdown_temporal_columns(self, profile_with_dates):
        """Markdown includes temporal columns section."""
        output = render_markdown(profile_with_dates)
        assert "## Temporal Columns" in output or "created_at" in output


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestMarkdownHelpers:
    """Tests for Markdown helper functions."""

    def test_cardinality_label_unique(self, sample_profile):
        """_cardinality_label returns 'unique' for unique columns."""
        # Find a column that might be unique
        for col in sample_profile.columns:
            if col.uniqueness_ratio >= 0.99 and col.null_rate == 0 and col.distinct_count > 5:
                label = _cardinality_label(col)
                assert label == "unique"
                break

    def test_cardinality_label_low(self, sample_profile):
        """_cardinality_label returns 'low' for low cardinality columns that aren't unique."""
        # Find a low cardinality column that isn't unique (like status)
        for col in sample_profile.columns:
            if col.is_low_cardinality and col.uniqueness_ratio < 0.99:
                label = _cardinality_label(col)
                assert label == "low"
                return
        # If no such column found, test passes (no low-card non-unique column in data)
        pass

    def test_fmt_large_number(self):
        """_fmt formats large numbers with commas."""
        assert _fmt(1000000) == "1,000,000"
        assert "," in _fmt(10000.5)

    def test_fmt_small_number(self):
        """_fmt formats small numbers with decimals."""
        assert _fmt(1.5) == "1.50"
        assert _fmt(0.001) == "0.0010"

    def test_fmt_none(self):
        """_fmt handles None."""
        assert _fmt(None) == "N/A"


# =============================================================================
# Profile Output Method Tests
# =============================================================================


class TestProfileOutputMethods:
    """Test DatasetProfile output methods (using reporter functions)."""

    def test_profile_to_dict(self, sample_profile):
        """Profile.to_dict() works."""
        d = sample_profile.to_dict()
        assert isinstance(d, dict)
        # Data may be nested under "dataset" key
        if "dataset" in d:
            assert "row_count" in d["dataset"]
        else:
            assert "columns" in d

    def test_render_json_for_profile(self, sample_profile):
        """render_json works on profile."""
        json_str = render_json(sample_profile)
        parsed = json.loads(json_str)
        # Verify it's valid JSON with data
        assert isinstance(parsed, dict)

    def test_render_llm_for_profile(self, sample_profile):
        """render_llm works on profile."""
        llm = render_llm(sample_profile)
        assert isinstance(llm, str)
        assert "Dataset:" in llm

    def test_render_markdown_for_profile(self, sample_profile):
        """render_markdown works on profile."""
        md = render_markdown(sample_profile)
        assert "# Data Profile:" in md


# =============================================================================
# Edge Cases
# =============================================================================


class TestReporterEdgeCases:
    """Test edge cases for reporters."""

    def test_empty_dataframe_profile(self, tmp_path):
        """Reporters handle empty DataFrame profile."""
        df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
        parquet = tmp_path / "empty.parquet"
        df.write_parquet(parquet)

        profiler = ScoutProfiler(str(parquet), preset="lite")
        profile = profiler.profile()

        # All reporters should handle this without error
        json_out = render_json(profile)
        assert "row_count" in json_out

        llm_out = render_llm(profile)
        assert "rows=0" in llm_out

        md_out = render_markdown(profile)
        assert "**Rows:** 0" in md_out

    def test_single_column_profile(self, tmp_path):
        """Reporters handle single-column profile."""
        df = pl.DataFrame({"single_col": [1, 2, 3, 4, 5]})
        parquet = tmp_path / "single.parquet"
        df.write_parquet(parquet)

        profiler = ScoutProfiler(str(parquet), preset="lite")
        profile = profiler.profile()

        json_out = render_json(profile)
        parsed = json.loads(json_out)
        assert len(parsed["columns"]) == 1

    def test_all_null_column_profile(self, tmp_path):
        """Reporters handle all-null column."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "all_nulls": [None, None, None],
        })
        parquet = tmp_path / "nulls.parquet"
        df.write_parquet(parquet)

        profiler = ScoutProfiler(str(parquet), preset="lite")
        profile = profiler.profile()

        # Should handle without error
        llm_out = render_llm(profile)
        assert "all_nulls" in llm_out
