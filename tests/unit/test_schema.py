"""Unit tests for Schema class."""

from __future__ import annotations

import json

import pytest
import pyarrow as pa

from helix_ir.exceptions import PathNotFoundError
from helix_ir.schema.path import Path, PathSegment
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def ht(arrow_type: pa.DataType, **kwargs) -> HelixType:
    return HelixType(arrow_type=arrow_type, **kwargs)


# -------------------------------------------------------------------------
# Schema construction
# -------------------------------------------------------------------------


class TestSchemaConstruction:
    def test_basic_schema(self) -> None:
        schema = Schema(
            name="test",
            fields=(
                ("id", ht(pa.int64())),
                ("name", ht(pa.string())),
            ),
        )
        assert schema.name == "test"
        assert len(schema) == 2

    def test_empty_schema(self) -> None:
        schema = Schema(name="empty", fields=())
        assert len(schema) == 0

    def test_schema_is_frozen(self) -> None:
        schema = Schema(name="test", fields=(("id", ht(pa.int64())),))
        with pytest.raises((AttributeError, TypeError)):
            schema.name = "other"  # type: ignore[misc]

    def test_contains(self) -> None:
        schema = Schema(name="test", fields=(("id", ht(pa.int64())),))
        assert "id" in schema
        assert "name" not in schema


# -------------------------------------------------------------------------
# Field lookup
# -------------------------------------------------------------------------


class TestFieldLookup:
    def test_get_existing_field(self, flat_schema) -> None:
        ht_result = flat_schema.field("order_id")
        assert ht_result.arrow_type == pa.string()

    def test_get_missing_field_raises(self, flat_schema) -> None:
        with pytest.raises(PathNotFoundError):
            flat_schema.field("nonexistent")

    def test_get_field_returns_none(self, flat_schema) -> None:
        assert flat_schema.get_field("nonexistent") is None
        assert flat_schema.get_field("order_id") is not None

    def test_field_names(self, flat_schema) -> None:
        names = flat_schema.field_names()
        assert "order_id" in names
        assert "amount" in names


# -------------------------------------------------------------------------
# Path resolution
# -------------------------------------------------------------------------


class TestPathResolution:
    def test_top_level_path(self, flat_schema) -> None:
        ht_result = flat_schema.path("order_id")
        assert ht_result.arrow_type == pa.string()

    def test_path_not_found_raises(self, flat_schema) -> None:
        with pytest.raises(PathNotFoundError):
            flat_schema.path("does_not_exist")


# -------------------------------------------------------------------------
# walk()
# -------------------------------------------------------------------------


class TestWalk:
    def test_walk_flat(self, flat_schema) -> None:
        walked = list(flat_schema.walk())
        paths = [str(p) for p, _ in walked]
        assert "order_id" in paths
        assert "amount" in paths
        assert "status" in paths

    def test_walk_nested(self, nested_schema) -> None:
        walked = list(nested_schema.walk())
        paths = [str(p) for p, _ in walked]
        assert "order_id" in paths
        # Should include nested struct fields
        assert any("customer" in p for p in paths)

    def test_walk_arrays(self, nested_schema) -> None:
        arrays = list(nested_schema.walk_arrays())
        assert len(arrays) > 0
        paths = [str(p) for p, _ in arrays]
        assert any("items" in p for p in paths)


# -------------------------------------------------------------------------
# JSON serialization roundtrip
# -------------------------------------------------------------------------


class TestJsonSerialization:
    def test_to_json(self, flat_schema) -> None:
        data = flat_schema.to_json()
        assert data["name"] == "orders"
        assert isinstance(data["fields"], list)
        assert len(data["fields"]) == len(flat_schema.fields)

    def test_from_json_roundtrip(self, flat_schema) -> None:
        data = flat_schema.to_json()
        restored = Schema.from_json(data)
        assert restored.name == flat_schema.name
        assert len(restored) == len(flat_schema)
        for (orig_name, orig_ht), (rest_name, rest_ht) in zip(
            flat_schema.fields, restored.fields
        ):
            assert orig_name == rest_name
            assert orig_ht.arrow_type == rest_ht.arrow_type

    def test_json_serializable(self, flat_schema) -> None:
        """to_json() must produce a JSON-serializable dict."""
        data = flat_schema.to_json()
        json_str = json.dumps(data)
        assert isinstance(json_str, str)
        restored = json.loads(json_str)
        schema = Schema.from_json(restored)
        assert schema.name == flat_schema.name

    def test_nested_schema_roundtrip(self, nested_schema) -> None:
        data = nested_schema.to_json()
        restored = Schema.from_json(data)
        assert restored.name == nested_schema.name
        assert len(restored) == len(nested_schema)


# -------------------------------------------------------------------------
# Arrow interop roundtrip
# -------------------------------------------------------------------------


class TestArrowInterop:
    def test_to_arrow(self, flat_schema) -> None:
        arrow_schema = flat_schema.to_arrow()
        assert isinstance(arrow_schema, pa.Schema)
        assert len(arrow_schema) == len(flat_schema)

    def test_from_arrow(self) -> None:
        arrow_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ])
        schema = Schema.from_arrow("test", arrow_schema)
        assert schema.name == "test"
        assert len(schema) == 3
        assert schema.field("id").arrow_type == pa.int64()

    def test_to_from_arrow_roundtrip(self, flat_schema) -> None:
        arrow = flat_schema.to_arrow()
        restored = Schema.from_arrow(flat_schema.name, arrow)
        assert restored.name == flat_schema.name
        for (orig_name, orig_ht), (rest_name, rest_ht) in zip(
            flat_schema.fields, restored.fields
        ):
            assert orig_name == rest_name
            assert orig_ht.arrow_type == rest_ht.arrow_type


# -------------------------------------------------------------------------
# Immutable mutations
# -------------------------------------------------------------------------


class TestSchemaMutations:
    def test_add_field(self, flat_schema) -> None:
        new_schema = flat_schema.add_field("extra", ht(pa.bool_()))
        assert "extra" in new_schema
        assert "extra" not in flat_schema  # Original unchanged

    def test_drop_field(self, flat_schema) -> None:
        new_schema = flat_schema.drop_field("status")
        assert "status" not in new_schema
        assert "status" in flat_schema  # Original unchanged

    def test_rename(self, flat_schema) -> None:
        new_schema = flat_schema.rename("new_name")
        assert new_schema.name == "new_name"
        assert flat_schema.name == "orders"


# -------------------------------------------------------------------------
# Path class
# -------------------------------------------------------------------------


class TestPath:
    def test_parse_simple(self) -> None:
        p = Path.parse("customer.address.city")
        assert str(p) == "customer.address.city"

    def test_parse_array(self) -> None:
        p = Path.parse("items[].sku")
        parts = [str(s) for s in p.segments]
        assert "sku" in parts

    def test_parse_empty(self) -> None:
        p = Path.parse("")
        assert p.is_root()

    def test_append(self) -> None:
        p = Path.parse("customer")
        extended = p.append("address")
        assert str(extended) == "customer.address"

    def test_is_descendant(self) -> None:
        parent = Path.parse("customer")
        child = Path.parse("customer.address.city")
        assert child.is_descendant_of(parent)
        assert not parent.is_descendant_of(child)

    def test_depth(self) -> None:
        p = Path.parse("a.b.c")
        assert p.depth() == 3
