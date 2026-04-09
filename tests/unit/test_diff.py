"""Unit tests for schema diff and change classification."""

from __future__ import annotations

import pytest
import pyarrow as pa

from helix_ir.diff import SchemaDiff, SchemaChange, diff
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def ht(arrow_type: pa.DataType, **kwargs) -> HelixType:
    return HelixType(arrow_type=arrow_type, **kwargs)


def make_schema(name: str, **fields) -> Schema:
    """Helper: make schema from name=arrow_type kwargs."""
    return Schema(
        name=name,
        fields=tuple((k, ht(v)) for k, v in fields.items()),
    )


class TestNoChanges:
    def test_identical_schemas(self, flat_schema) -> None:
        d = diff(flat_schema, flat_schema)
        assert not d.has_breaking_changes
        assert not d.has_risky_changes
        assert len(d) == 0


class TestFieldAdded:
    def test_nullable_field_added_is_safe(self) -> None:
        old = make_schema("test", id=pa.int64())
        new = Schema(
            name="test",
            fields=(
                ("id", ht(pa.int64())),
                ("extra", ht(pa.string(), null_ratio=0.5)),  # nullable
            ),
        )
        d = diff(old, new)
        added = [c for c in d.changes if c.kind == "added"]
        assert len(added) == 1
        assert added[0].severity == "safe"

    def test_non_nullable_field_added_is_risky(self) -> None:
        old = make_schema("test", id=pa.int64())
        new = make_schema("test", id=pa.int64(), extra=pa.string())
        d = diff(old, new)
        added = [c for c in d.changes if c.kind == "added"]
        assert len(added) == 1
        assert added[0].severity == "risky"


class TestFieldRemoved:
    def test_field_removed_is_breaking(self) -> None:
        old = Schema(
            name="test",
            fields=(("id", ht(pa.int64())), ("full_name", ht(pa.string()))),
        )
        new = make_schema("test", id=pa.int64())
        d = diff(old, new)
        removed = [c for c in d.changes if c.kind == "removed"]
        assert len(removed) == 1
        assert removed[0].severity == "breaking"
        assert d.has_breaking_changes


class TestTypeChanged:
    def test_type_widened_is_safe(self) -> None:
        old = make_schema("test", count=pa.int32())
        new = make_schema("test", count=pa.int64())
        d = diff(old, new)
        type_changes = [c for c in d.changes if c.kind == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0].severity == "safe"

    def test_type_narrowed_is_risky(self) -> None:
        old = make_schema("test", count=pa.int64())
        new = make_schema("test", count=pa.int32())
        d = diff(old, new)
        type_changes = [c for c in d.changes if c.kind == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0].severity in ("risky", "breaking")

    def test_incompatible_type_change_is_breaking(self) -> None:
        old = make_schema("test", value=pa.bool_())
        new = make_schema("test", value=pa.float64())
        d = diff(old, new)
        type_changes = [c for c in d.changes if c.kind == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0].severity == "breaking"
        assert d.has_breaking_changes


class TestNullabilityChanged:
    def test_become_nullable_is_safe(self) -> None:
        old = Schema(
            name="test",
            fields=(("name", ht(pa.string(), null_ratio=0.0)),),
        )
        new = Schema(
            name="test",
            fields=(("name", ht(pa.string(), null_ratio=0.1)),),
        )
        d = diff(old, new)
        null_changes = [c for c in d.changes if c.kind == "nullable_changed"]
        assert len(null_changes) == 1
        assert null_changes[0].severity == "safe"

    def test_become_non_nullable_is_risky(self) -> None:
        old = Schema(
            name="test",
            fields=(("name", ht(pa.string(), null_ratio=0.1)),),
        )
        new = Schema(
            name="test",
            fields=(("name", ht(pa.string(), null_ratio=0.0)),),
        )
        d = diff(old, new)
        null_changes = [c for c in d.changes if c.kind == "nullable_changed"]
        assert len(null_changes) == 1
        assert null_changes[0].severity == "risky"


class TestPIIChanged:
    def test_pii_added_is_risky(self) -> None:
        old = Schema(name="test", fields=(("email", ht(pa.string())),))
        new = Schema(name="test", fields=(("email", ht(pa.string(), pii_class="email")),))
        d = diff(old, new)
        pii_changes = [c for c in d.changes if c.kind == "pii_changed"]
        assert len(pii_changes) == 1
        assert pii_changes[0].severity == "risky"

    def test_pii_removed_is_safe(self) -> None:
        old = Schema(name="test", fields=(("email", ht(pa.string(), pii_class="email")),))
        new = Schema(name="test", fields=(("email", ht(pa.string())),))
        d = diff(old, new)
        pii_changes = [c for c in d.changes if c.kind == "pii_changed"]
        assert len(pii_changes) == 1
        assert pii_changes[0].severity == "safe"


class TestSchemaDiffAPI:
    def test_filter_by_severity(self) -> None:
        old = make_schema("test", a=pa.int64(), b=pa.string())
        new = make_schema("test", a=pa.int64(), c=pa.string())
        d = diff(old, new)
        breaking = d.filter("breaking")
        assert all(c.severity == "breaking" for c in breaking.changes)

    def test_summary(self) -> None:
        old = make_schema("test", a=pa.int64(), b=pa.string())
        new = make_schema("test", a=pa.int64(), c=pa.string())
        d = diff(old, new)
        summary = d.summary()
        assert isinstance(summary, dict)
        assert "safe" in summary
        assert "risky" in summary
        assert "breaking" in summary

    def test_bool_false_for_no_changes(self, flat_schema) -> None:
        d = diff(flat_schema, flat_schema)
        assert not bool(d)

    def test_bool_true_for_changes(self) -> None:
        old = make_schema("test", a=pa.int64())
        new = make_schema("test", b=pa.string())
        d = diff(old, new)
        assert bool(d)
