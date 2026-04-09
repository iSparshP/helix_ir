"""Unit tests for schema inference."""

from __future__ import annotations

import pytest
import pyarrow as pa

from helix_ir.exceptions import EmptySourceError
from helix_ir.infer import infer
from helix_ir.schema.schema import Schema
from tests.fixtures.sample_docs import FLAT_ORDERS, NESTED_ORDERS, make_enum_docs


class TestFlatInference:
    def test_flat_docs(self) -> None:
        schema = infer(FLAT_ORDERS, name="orders", detect_pii=False)
        assert isinstance(schema, Schema)
        assert schema.name == "orders"
        # Should have inferred the main fields
        field_names = schema.field_names()
        assert "order_id" in field_names
        assert "amount" in field_names

    def test_string_type_inferred(self) -> None:
        docs = [{"name": "alice"}, {"name": "bob"}]
        schema = infer(docs, detect_pii=False)
        ht = schema.field("name")
        assert pa.types.is_string(ht.arrow_type)

    def test_int_type_inferred(self) -> None:
        docs = [{"count": 1}, {"count": 2}, {"count": 3}]
        schema = infer(docs, detect_pii=False)
        ht = schema.field("count")
        assert pa.types.is_integer(ht.arrow_type)

    def test_float_type_inferred(self) -> None:
        docs = [{"price": 9.99}, {"price": 19.99}]
        schema = infer(docs, detect_pii=False)
        ht = schema.field("price")
        assert pa.types.is_floating(ht.arrow_type)

    def test_bool_type_inferred(self) -> None:
        docs = [{"active": True}, {"active": False}]
        schema = infer(docs, detect_pii=False)
        ht = schema.field("active")
        assert pa.types.is_boolean(ht.arrow_type)


class TestOptionalFields:
    def test_null_ratio_detected(self) -> None:
        schema = infer(FLAT_ORDERS, name="orders", detect_pii=False)
        email_ht = schema.field("customer_email")
        # One of 5 docs has null email → 20% null ratio
        assert email_ht.null_ratio > 0.0

    def test_fully_present_field_has_zero_null_ratio(self) -> None:
        schema = infer(FLAT_ORDERS, name="orders", detect_pii=False)
        id_ht = schema.field("order_id")
        assert id_ht.null_ratio == 0.0

    def test_optional_field_in_some_docs(self) -> None:
        docs = [
            {"id": 1, "extra": "foo"},
            {"id": 2},  # no extra
            {"id": 3, "extra": "bar"},
        ]
        schema = infer(docs, detect_pii=False)
        assert "id" in schema
        # extra field should be present since it appeared in some docs
        assert "extra" in schema


class TestEnumDetection:
    def test_enum_detected(self) -> None:
        docs = make_enum_docs(300)
        schema = infer(docs, name="test", detect_pii=False)
        status_ht = schema.field("status")
        # With 300 samples and cardinality < 50, should be detected as enum
        assert status_ht.semantic == "enum"

    def test_high_cardinality_not_enum(self) -> None:
        # Each doc has a unique ID → high cardinality, not enum
        docs = [{"id": str(i), "cat": f"cat_{i}"} for i in range(300)]
        schema = infer(docs, name="test", detect_pii=False)
        id_ht = schema.field("id")
        assert id_ht.semantic != "enum"


class TestEmptyInput:
    def test_empty_raises_by_default(self) -> None:
        with pytest.raises(EmptySourceError):
            infer([])

    def test_empty_no_raise(self) -> None:
        schema = infer([], fail_on_empty=False)
        assert isinstance(schema, Schema)
        assert len(schema) == 0


class TestDeterminism:
    def test_same_seed_same_result(self) -> None:
        docs = [{"id": i, "val": float(i)} for i in range(100)]
        s1 = infer(docs, seed=42, detect_pii=False)
        s2 = infer(docs, seed=42, detect_pii=False)
        assert s1.field_names() == s2.field_names()
        for (n1, ht1), (n2, ht2) in zip(s1.fields, s2.fields):
            assert n1 == n2
            assert ht1.arrow_type == ht2.arrow_type

    def test_different_seed_same_schema_for_small_input(self) -> None:
        """For small inputs where all docs fit in reservoir, seed doesn't matter."""
        docs = [{"id": i} for i in range(10)]
        s1 = infer(docs, seed=1, detect_pii=False)
        s2 = infer(docs, seed=2, detect_pii=False)
        assert s1.field_names() == s2.field_names()


class TestNestedInference:
    def test_nested_docs(self) -> None:
        schema = infer(NESTED_ORDERS, name="orders", detect_pii=False)
        assert isinstance(schema, Schema)
        field_names = schema.field_names()
        assert "order_id" in field_names
        assert "total" in field_names

    def test_sample_count(self) -> None:
        docs = [{"x": i} for i in range(50)]
        schema = infer(docs, detect_pii=False)
        ht = schema.field("x")
        assert ht.sample_count > 0
