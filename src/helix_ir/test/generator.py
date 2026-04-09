"""Automatic test generation from schema metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pyarrow as pa

from helix_ir.schema.path import Path
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


@dataclass
class Test:
    """A generated data quality test."""

    name: str
    path: Path
    kind: str  # 'not_null', 'type_check', 'cardinality', 'range', 'regex', 'pii'
    description: str
    severity: str = "error"  # 'error', 'warning', 'info'
    sql_template: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TestResult:
    """Result of running a test."""

    test: Test
    passed: bool
    message: str = ""
    actual_value: Any = None
    expected_value: Any = None


def generate_tests(
    schema: Schema,
    sensitivity: float = 1.5,
) -> list[Test]:
    """Generate data quality tests based on schema metadata.

    Args:
        schema: The schema to generate tests for.
        sensitivity: Multiplier for expected cardinality deviations.
            Higher = more permissive (fewer false positives).

    Returns:
        A list of Test objects.
    """
    tests: list[Test] = []

    for path, ht in schema.walk():
        path_str = str(path)

        # NOT NULL test for non-nullable fields
        if ht.null_ratio == 0.0 and ht.sample_count > 0:
            tests.append(
                Test(
                    name=f"not_null__{path_str}",
                    path=path,
                    kind="not_null",
                    description=f"Column '{path_str}' must not contain NULL values",
                    severity="error",
                    sql_template=f"SELECT COUNT(*) FROM {{table}} WHERE \"{path_str}\" IS NULL",
                )
            )

        # NULL ratio test for nullable fields
        elif ht.null_ratio > 0.0 and ht.sample_count > 0:
            max_null_ratio = min(1.0, ht.null_ratio * sensitivity)
            tests.append(
                Test(
                    name=f"null_ratio__{path_str}",
                    path=path,
                    kind="null_ratio",
                    description=(
                        f"Column '{path_str}' null ratio should not exceed "
                        f"{max_null_ratio:.2%}"
                    ),
                    severity="warning",
                    sql_template=(
                        f"SELECT CAST(COUNT(*) FILTER (WHERE \"{path_str}\" IS NULL) AS FLOAT) "
                        f"/ COUNT(*) FROM {{table}}"
                    ),
                    metadata={"expected_max_null_ratio": max_null_ratio},
                )
            )

        # Type check (for string columns with semantic types)
        if ht.semantic and ht.semantic not in ("json_blob",) and not ht.semantic.startswith("union:"):
            tests.append(
                Test(
                    name=f"semantic__{path_str}__{ht.semantic}",
                    path=path,
                    kind="semantic",
                    description=f"Column '{path_str}' should match semantic type '{ht.semantic}'",
                    severity="warning",
                    metadata={"semantic": ht.semantic},
                )
            )

        # Cardinality test for enum fields
        if ht.semantic == "enum" and ht.cardinality_estimate is not None:
            max_card = int(ht.cardinality_estimate * sensitivity)
            tests.append(
                Test(
                    name=f"cardinality__{path_str}",
                    path=path,
                    kind="cardinality",
                    description=(
                        f"Column '{path_str}' cardinality should not exceed {max_card}"
                    ),
                    severity="warning",
                    sql_template=f"SELECT COUNT(DISTINCT \"{path_str}\") FROM {{table}}",
                    metadata={"expected_max_cardinality": max_card},
                )
            )

        # PII test
        if ht.pii_class:
            tests.append(
                Test(
                    name=f"pii__{path_str}__{ht.pii_class}",
                    path=path,
                    kind="pii",
                    description=(
                        f"Column '{path_str}' is classified as PII ({ht.pii_class}). "
                        "Ensure proper masking/access controls."
                    ),
                    severity="info",
                    metadata={"pii_class": ht.pii_class},
                )
            )

        # Range tests for numeric fields
        if (
            pa.types.is_integer(ht.arrow_type) or pa.types.is_floating(ht.arrow_type)
        ) and ht.min_value is not None and ht.max_value is not None:
            tests.append(
                Test(
                    name=f"range__{path_str}",
                    path=path,
                    kind="range",
                    description=(
                        f"Column '{path_str}' values should be between "
                        f"{ht.min_value} and {ht.max_value}"
                    ),
                    severity="warning",
                    sql_template=(
                        f"SELECT COUNT(*) FROM {{table}} WHERE "
                        f"\"{path_str}\" < {ht.min_value} OR \"{path_str}\" > {ht.max_value}"
                    ),
                    metadata={"min": ht.min_value, "max": ht.max_value},
                )
            )

    return tests


def run_test(test: Test, data: list[dict[str, Any]]) -> TestResult:
    """Run a test against in-memory data."""
    try:
        if test.kind == "not_null":
            path_str = str(test.path)
            nulls = sum(1 for row in data if row.get(path_str) is None)
            if nulls > 0:
                return TestResult(
                    test=test,
                    passed=False,
                    message=f"Found {nulls} null values",
                    actual_value=nulls,
                    expected_value=0,
                )
            return TestResult(test=test, passed=True, message="No null values found")

        elif test.kind == "cardinality":
            path_str = str(test.path)
            values = {row.get(path_str) for row in data if row.get(path_str) is not None}
            actual = len(values)
            expected_max = test.metadata.get("expected_max_cardinality", float("inf"))
            if actual > expected_max:
                return TestResult(
                    test=test,
                    passed=False,
                    message=f"Cardinality {actual} exceeds expected max {expected_max}",
                    actual_value=actual,
                    expected_value=expected_max,
                )
            return TestResult(test=test, passed=True)

        # Default: pass (complex tests require SQL execution)
        return TestResult(test=test, passed=True, message="Test requires SQL execution")

    except Exception as e:
        return TestResult(test=test, passed=False, message=str(e))
