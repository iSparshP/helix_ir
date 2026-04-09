"""Shared pytest fixtures."""

from __future__ import annotations

import pytest
import pyarrow as pa

from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType
from tests.fixtures.sample_docs import FLAT_ORDERS, NESTED_ORDERS


@pytest.fixture
def flat_schema() -> Schema:
    """A simple flat schema with common field types."""
    return Schema(
        name="orders",
        fields=(
            ("order_id", HelixType(arrow_type=pa.string(), null_ratio=0.0, sample_count=5)),
            ("customer_email", HelixType(arrow_type=pa.string(), null_ratio=0.2, sample_count=5)),
            ("amount", HelixType(arrow_type=pa.float64(), null_ratio=0.0, sample_count=5)),
            ("quantity", HelixType(arrow_type=pa.int64(), null_ratio=0.0, sample_count=5)),
            ("status", HelixType(arrow_type=pa.string(), null_ratio=0.0, sample_count=5, semantic="enum")),
            ("created_at", HelixType(arrow_type=pa.timestamp("us"), null_ratio=0.0, sample_count=5)),
        ),
    )


@pytest.fixture
def nested_schema() -> Schema:
    """A nested schema with a struct field and an array field."""
    address_type = pa.struct([
        pa.field("street", pa.string()),
        pa.field("city", pa.string()),
        pa.field("pincode", pa.string()),
    ])
    customer_type = pa.struct([
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("phone", pa.string()),
        pa.field("address", address_type),
    ])
    item_type = pa.struct([
        pa.field("sku", pa.string()),
        pa.field("name", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("qty", pa.int64()),
    ])
    return Schema(
        name="orders",
        fields=(
            ("order_id", HelixType(arrow_type=pa.string())),
            ("customer", HelixType(arrow_type=customer_type)),
            ("items", HelixType(arrow_type=pa.list_(item_type))),
            ("total", HelixType(arrow_type=pa.float64())),
            ("status", HelixType(arrow_type=pa.string())),
        ),
    )


@pytest.fixture
def flat_orders() -> list[dict]:
    return FLAT_ORDERS


@pytest.fixture
def nested_orders() -> list[dict]:
    return NESTED_ORDERS
