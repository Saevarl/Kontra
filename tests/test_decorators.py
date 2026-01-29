# tests/test_decorators.py
"""Tests for the @kontra.validate_decorator decorator."""

import pytest
import polars as pl
import warnings

import kontra
from kontra import rules, ValidationError


class TestValidateDecoratorBasic:
    """Basic decorator functionality tests."""

    def test_pass_returns_data(self):
        """Decorator returns data when validation passes."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="raise",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, 2, 3]})

        result = load_data()
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (3, 1)

    def test_preserves_function_name(self):
        """Decorator preserves function metadata."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="raise",
            save=False,
        )
        def my_function() -> pl.DataFrame:
            """My docstring."""
            return pl.DataFrame({"id": [1]})

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."

    def test_with_args_and_kwargs(self):
        """Decorator works with functions that have arguments."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="raise",
            save=False,
        )
        def load_data(count: int, prefix: str = "row") -> pl.DataFrame:
            return pl.DataFrame({
                "id": list(range(count)),
                "name": [f"{prefix}_{i}" for i in range(count)],
            })

        result = load_data(5, prefix="item")
        assert result.shape == (5, 2)
        assert result["name"][0] == "item_0"


class TestValidateDecoratorRaise:
    """Tests for on_fail='raise' mode."""

    def test_raises_on_blocking_failure(self):
        """Decorator raises ValidationError on blocking failure."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="raise",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        with pytest.raises(ValidationError) as exc_info:
            load_data()

        assert exc_info.value.result is not None
        assert not exc_info.value.result.passed
        assert "1 blocking rule(s) failed" in str(exc_info.value)

    def test_error_contains_result(self):
        """ValidationError contains the full ValidationResult."""
        @kontra.validate_decorator(
            rules=[
                rules.not_null("id"),
                rules.not_null("email"),
            ],
            on_fail="raise",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({
                "id": [1, None, 3],
                "email": [None, "b@b.com", None],
            })

        with pytest.raises(ValidationError) as exc_info:
            load_data()

        result = exc_info.value.result
        assert result.total_rules == 2
        # Both rules have failures
        assert result.failed_count >= 1

    def test_does_not_raise_on_warning_only(self):
        """Decorator doesn't raise if only warning-severity rules fail."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id", severity="warning")],
            on_fail="raise",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        # Should not raise - warning severity doesn't block
        result = load_data()
        assert isinstance(result, pl.DataFrame)


class TestValidateDecoratorWarn:
    """Tests for on_fail='warn' mode."""

    def test_returns_data_with_warning(self):
        """Decorator returns data and emits warning on failure."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="warn",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = load_data()

            # Data should be returned
            assert isinstance(result, pl.DataFrame)
            assert result.shape == (3, 1)

            # Warning should be emitted
            assert len(w) == 1
            assert "Validation failed in load_data" in str(w[0].message)
            assert "1 blocking rule(s) failed" in str(w[0].message)

    def test_no_warning_when_pass(self):
        """Decorator doesn't warn when validation passes."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="warn",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, 2, 3]})

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = load_data()

            assert isinstance(result, pl.DataFrame)
            assert len(w) == 0


class TestValidateDecoratorReturnResult:
    """Tests for on_fail='return_result' mode."""

    def test_returns_tuple_on_pass(self):
        """Decorator returns (data, result) tuple when passing."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="return_result",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, 2, 3]})

        data, result = load_data()

        assert isinstance(data, pl.DataFrame)
        assert data.shape == (3, 1)
        assert result.passed
        assert result.failed_count == 0

    def test_returns_tuple_on_fail(self):
        """Decorator returns (data, result) tuple when failing."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="return_result",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        data, result = load_data()

        assert isinstance(data, pl.DataFrame)
        assert data.shape == (3, 1)
        assert not result.passed
        assert result.failed_count == 1


class TestValidateDecoratorContract:
    """Tests for contract file parameter."""

    def test_with_contract_file(self, tmp_path):
        """Decorator works with contract file."""
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_contract
datasource: placeholder
rules:
  - name: not_null
    params:
      column: id
""")

        @kontra.validate_decorator(
            contract=str(contract_path),
            on_fail="raise",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, 2, 3]})

        result = load_data()
        assert isinstance(result, pl.DataFrame)

    def test_contract_failure(self, tmp_path):
        """Decorator raises on contract validation failure."""
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_contract
datasource: placeholder
rules:
  - name: not_null
    params:
      column: id
""")

        @kontra.validate_decorator(
            contract=str(contract_path),
            on_fail="raise",
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        with pytest.raises(ValidationError):
            load_data()


class TestValidateDecoratorOptions:
    """Tests for decorator options."""

    def test_sample_option(self):
        """Decorator passes sample option to validate."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="return_result",
            save=False,
            sample=3,
        )
        def load_data() -> pl.DataFrame:
            # Create 10 failures
            return pl.DataFrame({"id": [None] * 10})

        data, result = load_data()

        # Should only have 3 samples per rule max
        failing_rule = result.rules[0]
        assert len(failing_rule.samples) <= 3

    def test_sample_columns_option(self):
        """Decorator passes sample_columns option to validate."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail="return_result",
            save=False,
            sample=5,
            sample_columns=["id"],
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({
                "id": [None, None],
                "name": ["Alice", "Bob"],
                "email": ["a@a.com", "b@b.com"],
            })

        data, result = load_data()

        # Samples should only have id and row_index
        samples = result.rules[0].samples
        assert len(samples) > 0
        for sample in samples:
            # Should have id, row_index (and maybe _duplicate_count)
            sample_keys = set(sample.keys())
            assert "id" in sample_keys
            assert "row_index" in sample_keys
            assert "name" not in sample_keys
            assert "email" not in sample_keys


class TestValidateDecoratorValidation:
    """Tests for decorator parameter validation."""

    def test_requires_contract_or_rules(self):
        """Decorator raises error if neither contract nor rules provided."""
        with pytest.raises(ValueError, match="Either 'contract' or 'rules' must be provided"):
            @kontra.validate_decorator()
            def load_data():
                return pl.DataFrame({"id": [1]})


class TestValidateDecoratorCallback:
    """Tests for on_fail with custom callback."""

    def test_callback_receives_result_and_data(self):
        """Callback receives ValidationResult and data."""
        received = {}

        def capture_callback(result, data):
            received["result"] = result
            received["data"] = data
            return data

        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail=capture_callback,
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        output = load_data()

        # Callback was called with correct args
        assert "result" in received
        assert "data" in received
        assert not received["result"].passed
        assert received["result"].failed_count == 1
        assert isinstance(received["data"], pl.DataFrame)
        # Returns what callback returns
        assert output is received["data"]

    def test_callback_can_raise(self):
        """Callback can raise custom exception."""
        class CustomError(Exception):
            pass

        def raise_custom(result, data):
            if not result.passed:
                raise CustomError(f"Custom error: {result.failed_count} violations")
            return data

        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail=raise_custom,
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        with pytest.raises(CustomError, match="Custom error: 1 violations"):
            load_data()

    def test_callback_can_transform_data(self):
        """Callback can transform and return different data."""
        def filter_valid(result, data):
            # Filter out rows with nulls
            return data.drop_nulls()

        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail=filter_valid,
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, None, 3]})

        result = load_data()

        # Data was filtered
        assert result.shape == (2, 1)
        assert result["id"].to_list() == [1, 3]

    def test_callback_on_pass(self):
        """Callback is still called when validation passes."""
        call_count = [0]

        def count_calls(result, data):
            call_count[0] += 1
            return data

        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail=count_calls,
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, 2, 3]})  # No nulls

        load_data()
        assert call_count[0] == 1

    def test_lambda_callback(self):
        """Lambda function works as callback."""
        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail=lambda result, data: (data, result),  # Always return tuple
            save=False,
        )
        def load_data() -> pl.DataFrame:
            return pl.DataFrame({"id": [1, 2, 3]})

        data, result = load_data()
        assert isinstance(data, pl.DataFrame)
        assert result.passed


class TestValidationErrorType:
    """Tests for ValidationError exception type."""

    def test_is_exception(self):
        """ValidationError is an exception."""
        assert issubclass(ValidationError, Exception)

    def test_from_kontra_module(self):
        """ValidationError is accessible from kontra module."""
        assert hasattr(kontra, "ValidationError")
        assert kontra.ValidationError is ValidationError

    def test_str_representation(self):
        """ValidationError has meaningful str representation."""
        # Create a mock result for testing
        from kontra.api.results import ValidationResult, RuleResult

        result = ValidationResult(
            passed=False,
            dataset="test.parquet",
            total_rows=100,
            total_rules=2,
            passed_count=1,
            failed_count=1,
            warning_count=0,
            rules=[
                RuleResult(
                    rule_id="COL:id:not_null",
                    name="not_null",
                    passed=False,
                    failed_count=5,
                    message="5 NULL values",
                    severity="blocking",
                    source="polars",
                ),
                RuleResult(
                    rule_id="COL:email:not_null",
                    name="not_null",
                    passed=True,
                    failed_count=0,
                    message="Passed",
                    severity="blocking",
                    source="polars",
                ),
            ],
        )

        error = ValidationError(result)
        error_str = str(error)

        assert "1 blocking rule(s) failed" in error_str
        # failed_count in ValidationResult is the number of failed rules (1), not per-rule counts
        assert "1 total violations" in error_str
