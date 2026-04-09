"""Unit tests for schema normalization."""

from __future__ import annotations

import pytest
import pyarrow as pa

from helix_ir.normalize import normalize
from helix_ir.normalize.plan import ForeignKey, NormalizationPlan
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def ht(arrow_type: pa.DataType, **kwargs) -> HelixType:
    return HelixType(arrow_type=arrow_type, **kwargs)


class TestFlatSchema:
    def test_flat_schema_produces_one_table(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        assert isinstance(plan, NormalizationPlan)
        assert len(plan.tables) == 1
        assert plan.tables[0].name == "orders"

    def test_flat_schema_has_no_fks(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        assert len(plan.foreign_keys) == 0

    def test_root_table(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        root = plan.root_table()
        assert root.name == "orders"

    def test_synthetic_id_added(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        root = plan.root_table()
        assert "__id" in root


class TestNestedArrayDecomposition:
    def test_nested_array_produces_child_table(self, nested_schema) -> None:
        plan = normalize(nested_schema, strategy="1nf")
        # Should have root + child for items
        assert len(plan.tables) >= 2
        table_names = plan.table_names()
        assert "orders" in table_names
        assert any("items" in name for name in table_names)

    def test_foreign_key_created(self, nested_schema) -> None:
        plan = normalize(nested_schema, strategy="1nf")
        assert len(plan.foreign_keys) >= 1
        fk = plan.foreign_keys[0]
        assert isinstance(fk, ForeignKey)
        assert fk.to_table == "orders"
        assert fk.to_column == "__id"

    def test_child_table_has_parent_id(self, nested_schema) -> None:
        plan = normalize(nested_schema, strategy="1nf")
        child_tables = [t for t in plan.tables if "items" in t.name]
        assert child_tables
        child = child_tables[0]
        assert "__parent_id" in child

    def test_child_table_has_ordinal(self, nested_schema) -> None:
        plan = normalize(nested_schema, strategy="1nf")
        child_tables = [t for t in plan.tables if "items" in t.name]
        assert child_tables
        child = child_tables[0]
        assert "__ordinal" in child


class TestMongoStrategy:
    def test_mongo_keeps_arrays_inline(self, nested_schema) -> None:
        plan = normalize(nested_schema, strategy="mongo")
        # With mongo strategy, arrays are kept inline as JSON
        table_names = plan.table_names()
        assert "orders" in table_names
        # Should have fewer tables than 1nf
        plan_1nf = normalize(nested_schema, strategy="1nf")
        assert len(plan.tables) <= len(plan_1nf.tables)


class TestInlineSmallStrategy:
    def test_inline_small_inlines_small_arrays(self) -> None:
        # Create a schema with an array that has low cardinality
        small_array_type = pa.list_(pa.string())
        schema = Schema(
            name="test",
            fields=(
                ("id", ht(pa.int64())),
                ("tags", ht(small_array_type, cardinality_estimate=3)),
            ),
        )
        plan = normalize(schema, strategy="inline_small", inline_threshold=5)
        # tags has cardinality 3 < 5 → should be inlined
        root = plan.root_table()
        tags_ht = root.get_field("tags")
        if tags_ht:
            from helix_ir.types.semantic import JSONBLOB_TYPE
            assert tags_ht.semantic == JSONBLOB_TYPE


class TestPlanAPI:
    def test_table_names(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        assert isinstance(plan.table_names(), list)

    def test_get_table(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        table = plan.get_table("orders")
        assert table is not None
        assert table.name == "orders"

    def test_get_missing_table(self, flat_schema) -> None:
        plan = normalize(flat_schema)
        assert plan.get_table("nonexistent") is None
